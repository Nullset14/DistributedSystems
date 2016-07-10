package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	private static HashMap<String, String> data = new HashMap<String, String>();

	private String getValue(String key) {
		String value;

		lock.readLock().lock();
			value = data.get(key);
		lock.readLock().unlock();

		return value;
	}

	private void setValue(String key, String value) {
		lock.writeLock().lock();
			data.put(key, value);
		lock.writeLock().unlock();
	}

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;

	String myPort = "";
	String hashPort = "";
	String myNode = "";
	String hashNode = "";
	String successorPort = "";
	String predecessorPort = "";

	static boolean successorSync = false;
	static boolean predecessorSync = false;

	HashMap<String, String> hashToNode = new HashMap<String, String> ();
	HashMap<String, String> nodeToHash = new HashMap<String, String> ();
	ArrayList<String> nodes = new ArrayList<String>();
	ArrayList<String> hashNodes = new ArrayList<String>();
	static ReadWriteLock lock = new ReentrantReadWriteLock();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		if (selection.equals("@")) {
			data.clear();
		} else if (selection.equals("*")) {
            /* Forward the query */
			new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "delete", myPort, selection);
		} else {
			new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "delete", myPort, selection);
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public  Uri insert(Uri uri, ContentValues values) {

		while(predecessorSync || successorSync);

		String key = values.get("key").toString();
		String value = values.get("value").toString();

		int i = hashIndex(key);
		for (int j = 0; j < 3; j++) {
			String toNode = hashToNode.get(hashNodes.get((i + j) % 5));
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(toNode) * 2);


				PrintStream out = new PrintStream(socket.getOutputStream());
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				Message msg = new Message(myPort, key + "-" + value, "insert");

				Log.e(TAG, "Inserting-" + key + "-in-" + toNode);
				out.println(msg.toString());
				out.flush();

				socket.close();
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException" + Log.getStackTraceString(e));
			} catch (NullPointerException e) {
				Log.e(TAG, "Null Pointer Exception" + Log.getStackTraceString(e));
			}
		}

		return uri;
	}

	@Override
	public boolean onCreate() {

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		this.myNode = portStr;
		this.myPort = myPort;
		arrangeNodes();

		switch (Integer.parseInt(portStr)) {
			case 5554: {
				predecessorPort = "11112";
				successorPort = "11116";
				break;
			}
			case 5558: {
				predecessorPort = "11108";
				successorPort = "11120";
				break;
			}
			case 5560: {
				predecessorPort = "11116";
				successorPort = "11124";
				break;
			}
			case 5562: {
				predecessorPort = "11120";
				successorPort = "11112";
				break;
			}
			case 5556: {
				predecessorPort = "11124";
				successorPort = "11108";
				break;
			}
			default: {
				Log.e(TAG, "Error in Switch!!");
				break;
			}
		}

		predecessorSync = true;
		successorSync = true;

		new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "predecessor", myPort);
		new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "successor", myPort);

		try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {

		MatrixCursor mCursor = new MatrixCursor(new String[] {"key", "value"});

		while(predecessorSync || successorSync);

		if (selection.equals("*")) {
			for (String node : nodes) {
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10, 0, 2, 2}),
							Integer.parseInt(node) * 2);
					PrintStream out = new PrintStream(socket.getOutputStream());
					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

					Message msg = new Message(myPort, selection, "query");
					out.println(msg.toString());
					out.flush();

					Message newMsg = new Message();
					String s = in.readLine();
					newMsg.splitMessage(s);
					String[] keyValues = newMsg.message.split("-");

					for (int i = 1; i < keyValues.length; i = i + 2) {
						mCursor.addRow(new String[]{ keyValues[i-1], keyValues[i]});
					}

					socket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException" + Log.getStackTraceString(e));
				} catch (NullPointerException e) {
					Log.e(TAG, "Null Pointer Exception" + Log.getStackTraceString(e));
				}
			}

		} else if (selection.equals("@")) {

			lock.readLock().lock();

				for (String key : data.keySet()) {
					mCursor.addRow(new String[]{key, getValue(key)});
			}

			lock.readLock().unlock();
		} else {
			int i = hashIndex(selection);
			for(int k = 0; k <= 2;  k++) {
				try {

					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(hashToNode.get(hashNodes.get((i + k) % 5))) * 2);
					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					PrintStream out = new PrintStream(socket.getOutputStream());
					Message msg = new Message(myPort, selection, "query");
					Message newMsg = new Message();

					out.println(msg.toString());
					out.flush();

					newMsg.splitMessage(in.readLine());
					String keyValues = newMsg.message;

					if (keyValues == null || keyValues.equals("null") || keyValues.isEmpty()) {
						socket.close();
						continue;
					}

					socket.close();

					mCursor.addRow(new String[]{selection, keyValues});

					break;
				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException" + Log.getStackTraceString(e));
					Log.e(TAG, selection);
				} catch (NullPointerException e) {
					Log.e(TAG, "Null Pointer Exception" + Log.getStackTraceString(e));
				}

			}
		}
		return mCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}


	/***
	 * ServerTask is an AsyncTask that should handle incoming messages. It is created by
	 * ServerTask.executeOnExecutor() call in SimpleDhtProvider.
	 * <p/>
	 * Please make sure you understand how AsyncTask works by reading
	 * http://developer.android.com/reference/android/os/AsyncTask.html
	 *
	 * @author stevko
	 */
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			while (true) {
				try {
					Socket localSocket = serverSocket.accept();
					DataInputStream messages = new DataInputStream(localSocket.getInputStream());
					String incomingMessage =  messages.readLine();

					Message protocolMessage = new Message();
					protocolMessage.splitMessage(incomingMessage);
					PrintStream out = new PrintStream(localSocket.getOutputStream());
					Message newMsg = new Message(myPort, "", protocolMessage.protocol);

					while(predecessorSync || successorSync);

					if (protocolMessage.protocol.equals("query")) {
						if (protocolMessage.message.equals("*")) {
							for (String key : data.keySet()) {
								if (newMsg.message.isEmpty())
									newMsg.message = String.format("%s-%s", key, getValue(key));
								else
									newMsg.message = newMsg.message.concat(String.format("-%s-%s", key, getValue(key)));
							}
						} else {
							newMsg.message = getValue(protocolMessage.message);
						}
						out.println(newMsg.toString());
						out.flush();
					} else if (protocolMessage.protocol.equals("insert")) {
						String[] msgs = protocolMessage.message.split("-");
						String key = msgs[0];
						String value = msgs[1];
						setValue(key, value);
						out.println("inserted");
						out.flush();
					} else if (protocolMessage.protocol.equals("delete")) {
						if (protocolMessage.message.equals("*")) {
							data.clear();
						} else {
							String key = protocolMessage.message;
							data.remove(key);
							out.println("deleted");
							out.flush();
						}
					}

					localSocket.close();
				} catch (IOException ioException) {
					Log.e(TAG, "ServerTask socket IOException" + Log.getStackTraceString(ioException));
				} catch (NullPointerException e) {
					Log.e(TAG, "Null Pointer Exception" + Log.getStackTraceString(e));
				}  catch (Exception e) {
					Log.e(TAG, "ServerTask socket UnknownException" + Log.getStackTraceString(e));
				}
			}
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

			if (msgs[0].equals("delete")) {
				int i = hashIndex(msgs[2]);
				Log.e(TAG, hashToNode.get(hashNodes.get(i)));
				Log.e(TAG, msgs[2]);

				int value = 3;
				if (msgs[2].equals("*")) {
					value = 5;
				}
				int k = 0;
				for(;k < value; k++) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10, 0, 2, 2}),
								Integer.parseInt(hashToNode.get(hashNodes.get((i+k) % 5))) * 2);
						PrintStream out = new PrintStream(socket.getOutputStream());
						Message msg = new Message(myPort, msgs[2], "delete");
						out.println(msg.toString());
						out.flush();
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException" + Log.getStackTraceString(e));
						Log.e(TAG, msgs[2]);
					} catch (NullPointerException e) {
						Log.e(TAG, "Null Pointer Exception" + Log.getStackTraceString(e));
					}
				}
				return null;
			} else if (msgs[0].equals("predecessor")) {

				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(predecessorPort));

					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					PrintStream out = new PrintStream(socket.getOutputStream());

					Message msg = new Message(msgs[1], "*", "query");
					out.println(msg.toString());
					out.flush();
					Message newMsg = new Message();

					newMsg.splitMessage(in.readLine());
					String[] keyValues = newMsg.message.split("-");

					for (int i = 1; i < keyValues.length; i = i + 2) {
						int j = hashIndex(keyValues[i - 1]);

						if (hashNodes.get(j).equals(hashNode)
								|| hashNodes.get((j + 1) % 5).equals(hashNode)
								|| hashNodes.get((j + 2) % 5).equals(hashNode)) {
							setValue(keyValues[i - 1], keyValues[i]);
						}
					}

					socket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException" + Log.getStackTraceString(e));
				} catch (NullPointerException e) {
					Log.e(TAG, "Null Pointer Exception" + Log.getStackTraceString(e));
				}

				predecessorSync = false;
			} else if (msgs[0].equals("successor")) {

				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(successorPort));

					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					PrintStream out = new PrintStream(socket.getOutputStream());

					Message msg = new Message(msgs[1], "*", "query");
					out.println(msg.toString());
					out.flush();
					String s = in.readLine();
					Message newMsg = new Message();
					newMsg.splitMessage(s);
					String[] keyValues = newMsg.message.split("-");

					for (int i = 1; i < keyValues.length; i = i + 2) {
						int j = hashIndex(keyValues[i - 1]);

						if (hashNodes.get(j).equals(hashNode)
								|| hashNodes.get((j + 1) % 5).equals(hashNode)
								|| hashNodes.get((j + 2) % 5).equals(hashNode)) {
							setValue(keyValues[i - 1], keyValues[i]);
						}
					}

					socket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException" + Log.getStackTraceString(e));
				} catch (NullPointerException e) {
					Log.e(TAG, "Null Pointer Exception" + Log.getStackTraceString(e));
				}

				successorSync = false;
			}

			return null;
		}
	}

	/* Custom class to send Messages */
	private class Message {

		public String originalSenderPort;
		public String message;
		public String protocol;

		Message(String originalSenderPort,
				String message, String protocol) {

			this.originalSenderPort = originalSenderPort;
			this.message = message;
			this.protocol = protocol;
		}

		Message () {
		}

		public String toString() {
			return (originalSenderPort + "_" +
					message + "_" + protocol);
		}

		public void splitMessage(String msg) {
			String[] messages = msg.split("_");
			this.originalSenderPort = messages[0];
			this.message = messages[1];
			this.protocol = messages[2];
		}
	}

	/* Arrange Nodes in Ring */
	void arrangeNodes() {
		try {
			ArrayList<String> portHashes = new ArrayList<String>();
			nodes.add("5554");
			nodes.add("5556");
			nodes.add("5558");
			nodes.add("5560");
			nodes.add("5562");

			for(String node: nodes) {
				portHashes.add(genHash(node));
				nodeToHash.put(node, genHash(node));
				hashToNode.put(genHash(node), node);
			}

			Collections.sort(portHashes);
			hashNodes = portHashes;
			hashPort = genHash(myPort);
			hashNode = genHash(myNode);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, Log.getStackTraceString(e));
		}
		return;
	}

	int hashIndex(String key) {
		String hashKey = "";
		try {
			hashKey = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, Log.getStackTraceString(e));
		}

		int i = 0;
		for (; i < 5; i++) {
			if (hashKey.compareTo(hashNodes.get((i + 4) % 5)) > 0 &&
					hashKey.compareTo(hashNodes.get(i)) <= 0) {
				break;
			} else if (i == 0 && hashKey.compareTo(hashNodes.get(4)) > 0 &&
					hashKey.compareTo(hashNodes.get(0)) >= 0) {
				break;
			} else if (i == 0 && hashKey.compareTo(hashNodes.get(4)) < 0 &&
					hashKey.compareTo(hashNodes.get(0)) <= 0) {
				break;
			}
		}

		return i;
	}
}