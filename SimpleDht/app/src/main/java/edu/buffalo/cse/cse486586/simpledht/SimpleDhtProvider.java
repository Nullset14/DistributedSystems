package edu.buffalo.cse.cse486586.simpledht;

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

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private static HashMap<String, String> data = new HashMap<String, String>();
    private String getValue(String key) {
        return data.get(key);
    }
    private void setValue(String key, String value) {
        data.put(key, value);
    }

    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    static final int SERVER_PORT = 10000;
    static final String REMOTE_PORT0 = "11108";

    String myPort = "";
    String hashPort = "";
    String successorPort = "";
    String predecessorPort = "";
    HashMap<String, String> queryData = new HashMap<String, String> ();
    HashMap<String, String> hashToNode = new HashMap<String, String> ();
    HashMap<String, String> nodeToHash = new HashMap<String, String> ();
    boolean queryResponse = false;
    ArrayList<String> nodes = new ArrayList<String>();
    ArrayList<String> hashNodes = new ArrayList<String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        if (selection.equals("@")) {
            for (String key : data.keySet()) {
                data.remove(key);
            }
        } else if (selection.equals("*")) {
            for (String key : data.keySet()) {
                data.remove(key);
            }

            /* Forward the query */
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete", hashPort);
        } else {
            data.remove(selection);
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key = values.get("key").toString();
        String value = values.get("value").toString();

        try {
            String hashKey =  genHash(key);

            /* No additional Nodes */
            if (predecessorPort.equals("") && successorPort.equals("")) {
                setValue(key, value);

             /* Scenario - Key 7, Nodes 5 and 9 */
            }   else if (hashKey.compareTo(genHash(predecessorPort)) > 0 && hashKey.compareTo(genHash(hashPort)) <= 0) {
                setValue(key, value);

            /* Scenario - Key 1, Nodes 12 and 2 */
            } else if (genHash(hashPort).equals(hashNodes.get(0)) && hashKey.compareTo(genHash(predecessorPort)) > 0 &&
                    hashKey.compareTo(genHash(hashPort)) >= 0) {
                setValue(key, value);

            } else if (hashKey.compareTo(genHash(predecessorPort)) < 0 && hashKey.compareTo(genHash(hashPort)) <= 0 &&
                    genHash(hashPort).equals(hashNodes.get(0))) {
                setValue(key, value);

            /* Forward the insert to next node */
            } else {

                Log.d(TAG, "forwarding insert to " + successorPort + " " + key);

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successorPort) * 2);
                    PrintStream out = new PrintStream(socket.getOutputStream());
                    Message msg = new Message(myPort, key + "-" + value, "insert");
                    out.println(msg.toString());
                    out.close();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
                    Log.e(TAG, "failedNode" + successorPort);
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException" + Log.getStackTraceString(e));
                    Log.e(TAG, "failedNode" + successorPort);
                } catch (NullPointerException e) {
                    Log.e(TAG, "Null Pointer Exception" + Log.getStackTraceString(e));
                    Log.e(TAG, "failedNode" + successorPort);
                }
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, Log.getStackTraceString(e));
        }

        Log.d("contents", data.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        this.hashPort = portStr;
        this.myPort = myPort;

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

        if (myPort != REMOTE_PORT0) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "join", myPort);
        } else {
            nodes.add("5554");
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        MatrixCursor mCursor = new MatrixCursor(new String[] {"key", "value"});

        queryData.clear();
        try {
            String hashKey = genHash(selection);

            if (selection.equals("*")) {
                for (String key : data.keySet()) {
                    mCursor.addRow(new String[]{key, getValue(key)});
                }

                Log.d(TAG, "forwarding * query to " + successorPort);

                if (!predecessorPort.isEmpty() && !successorPort.isEmpty()) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", hashPort, selection);

                /* Wait till response is received */
                    while (!queryResponse) ;

                    for (String key : queryData.keySet()) {
                        mCursor.addRow(new String[]{key, queryData.get(key)});
                    }
                }

                queryResponse = false;
            } else if (selection.equals("@")) {
                for (String key : data.keySet()) {
                    mCursor.addRow(new String[]{key, getValue(key)});
                }

            } else if (predecessorPort.isEmpty() || successorPort.isEmpty()) {
                Log.d(TAG, "empty " + successorPort);
                mCursor.addRow(new String[]{selection, getValue(selection)});

            } else if (hashKey.compareTo(genHash(predecessorPort)) > 0 && hashKey.compareTo(genHash(hashPort)) <= 0) {
                mCursor.addRow(new String[]{selection, getValue(selection)});

            } else if (genHash(hashPort).equals(hashNodes.get(0)) && hashKey.compareTo(genHash(predecessorPort)) > 0 &&
                hashKey.compareTo(genHash(hashPort)) >= 0) {
                mCursor.addRow(new String[]{selection, getValue(selection)});

            } else if (hashKey.compareTo(genHash(predecessorPort)) < 0 && hashKey.compareTo(genHash(hashPort)) <= 0 &&
                genHash(hashPort).equals(hashNodes.get(0))) {
                mCursor.addRow(new String[]{selection, getValue(selection)});

            } else {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", hashPort, selection);

                Log.d(TAG, "forwarding query to " + successorPort);

                /* Wait till response is received */
                while (!queryResponse);

                for (String key : queryData.keySet()) {
                    Log.d(TAG, "Key added " + key + " ");
                    mCursor.addRow(new String[]{key, queryData.get(key)});
                }
                queryResponse = false;
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, Log.getStackTraceString(e));
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

            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
            uriBuilder.scheme("content");

            while (true) {
                try {
                    ServerSocket serverSocket = sockets[0];
                    Socket localSocket = serverSocket.accept();
                    DataInputStream messages = new DataInputStream(localSocket.getInputStream());
                    String incomingMessage = messages.readLine();

                    Message protocolMessage = new Message();
                    protocolMessage.splitMessage(incomingMessage);
                    PrintStream out = new PrintStream(localSocket.getOutputStream());
                    Message newMsg = new Message(protocolMessage.originalSenderPort, "", protocolMessage.protocol);

                    if (protocolMessage.protocol.equals("query")) {
                        boolean forwardQuery = true;

                        if (protocolMessage.message.equals("*") && !protocolMessage.originalSenderPort.equals(myPort)) {
                            for (String key : data.keySet()) {

                                if (newMsg.message.isEmpty())
                                    newMsg.message = key + "-" + getValue(key);
                                else
                                    newMsg.message = newMsg.message + "-" + key + "-" + getValue(key);
                            }
                        } else if (!protocolMessage.originalSenderPort.equals(myPort)) {
                            String hashKey = genHash(protocolMessage.message);

                            if (hashKey.compareTo(genHash(predecessorPort)) > 0 && hashKey.compareTo(genHash(hashPort)) <= 0) {
                                newMsg.message = protocolMessage.message + "-" +  getValue(protocolMessage.message);
                                forwardQuery = false;

                            } else if ((genHash(hashPort).equals(hashNodes.get(0)) && hashKey.compareTo(genHash(predecessorPort)) > 0 &&
                                    hashKey.compareTo(genHash(hashPort)) >= 0)) {
                                newMsg.message = protocolMessage.message + "-" +  getValue(protocolMessage.message);
                                forwardQuery = false;

                            } else if (hashKey.compareTo(genHash(predecessorPort)) < 0 && hashKey.compareTo(genHash(hashPort)) <= 0 &&
                                    genHash(hashPort).equals(hashNodes.get(0))) {
                                newMsg.message = protocolMessage.message + "-" +  getValue(protocolMessage.message);
                                forwardQuery = false;
                            }
                        }

                        if (!protocolMessage.originalSenderPort.equals(hashPort) && forwardQuery) {
                            Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(successorPort) * 2);
                            BufferedReader in1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
                            PrintStream out1 = new PrintStream(socket1.getOutputStream());
                            Message newMsg1 = new Message(protocolMessage.originalSenderPort, protocolMessage.message, "query");
                            out1.println(newMsg1.toString());
                            newMsg1.splitMessage(in1.readLine());

                            if (newMsg.message.isEmpty()) {
                                newMsg.message = newMsg1.message;
                            } else {
                                newMsg.message += "-" + newMsg1.message;
                            }
                        }

                        out.println(newMsg.toString());

                    } else if (protocolMessage.protocol.equals("join")) {

                         String port = String.valueOf((Integer.parseInt(protocolMessage.originalSenderPort) / 2));
                         nodes.add(port);
                         arrangeNodes();
                         out.println("joined");
                         out.close();

                        for (String remotePort : nodes) {
                            Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(String.valueOf(Integer.parseInt(remotePort) * 2)));
                            PrintStream out1 = new PrintStream(socket1.getOutputStream());
                            newMsg = new Message(myPort, TextUtils.join("-", nodes), "fix");
                            out1.println(newMsg.toString());
                            out1.close();
                        }
                    } else if (protocolMessage.protocol.equals("fix")) {
                        String[] nodeList =  protocolMessage.message.split("-");
                        nodes = new ArrayList<String>();

                        for (String node : nodeList) {
                            nodes.add(node);
                        }

                        arrangeNodes();
                        int len = nodes.size();

                        for(int i = 0; i < len; i++) {
                            if (hashNodes.get(i).equals(nodeToHash.get(hashPort))) {
                                successorPort = hashToNode.get(hashNodes.get((i+1) % len));
                                predecessorPort = hashToNode.get(hashNodes.get((i-1 + len) % len));
                            }
                            out.println("fixed");
                            out.close();
                        }
                    } else if (protocolMessage.protocol.equals("insert")) {

                        String[] msgs = protocolMessage.message.split("-");
                        String key = msgs[0];
                        String value = msgs[1];
                        String hashKey = genHash(key);
                        out.close();

                        if (hashKey.compareTo(genHash(predecessorPort)) > 0 && hashKey.compareTo(genHash(hashPort)) <= 0) {
                            setValue(key, value);

                        } else if (genHash(hashPort).equals(hashNodes.get(0)) && (hashKey.compareTo(genHash(predecessorPort)) > 0)
                                && (hashKey.compareTo(genHash(hashPort)) >= 0)) {
                            setValue(key, value);

                        } else if (hashKey.compareTo(genHash(predecessorPort)) < 0 && hashKey.compareTo(genHash(hashPort)) <= 0 &&
                                genHash(hashPort).equals(hashNodes.get(0))) {
                            setValue(key, value);

                        } else {
                            try {
                                Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(successorPort) * 2);

                                BufferedReader in1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
                                Message msg = new Message(myPort, key + "-" + value, "insert");
                                PrintStream out1 = new PrintStream(socket1.getOutputStream());

                                out1.println(msg.toString());
                                out1.close();
                                in1.close();
                                socket1.close();

                            } catch (UnknownHostException e) {
                                Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
                                Log.e(TAG, "failedNode" + successorPort);
                            } catch (IOException e) {
                                Log.e(TAG, "ClientTask socket IOException" + Log.getStackTraceString(e));
                                Log.e(TAG, "failedNode" + successorPort);
                            } catch (NullPointerException e) {
                                Log.e(TAG, "Null Pointer Exception" + Log.getStackTraceString(e));
                                Log.e(TAG, "failedNode" + successorPort);
                            }
                        }
                    }
                } catch (IOException ioException) {
                    Log.e(TAG, "ServerTask socket IOException" + Log.getStackTraceString(ioException));
                } catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, "NoSuchAlgorithmException" + Log.getStackTraceString(e));
                } catch (Exception e) {
                    Log.e(TAG, "ServerTask socket UnknownException" + Log.getStackTraceString(e));
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            Log.d(TAG, msgs[0]);

            String port = successorPort;

            if (!successorPort.isEmpty())
                port = String.valueOf(Integer.parseInt(successorPort) * 2);

            if (msgs[0].equals("join")) {
                port = REMOTE_PORT0;
            }

            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));

                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintStream out = new PrintStream(socket.getOutputStream());

                if (msgs[0].equals("join")) {
                    Message msg = new Message(msgs[1], msgs[0], msgs[0]);
                    out.println(msg.toString());
                    in.readLine();
                } else if (msgs[0].equals("insert")) {
                    Message msg = new Message(msgs[1], msgs[0], msgs[0]);
                    out.println(msg.toString());
                    in.readLine();
                } else if (msgs[0].equals("query")) {
                    Message msg = new Message(msgs[1], msgs[2], msgs[0]);
                    out.println(msg.toString());

                    Message newMsg = new Message();
                    String s = in.readLine();
                    newMsg.splitMessage(s);
                    String[] keyValues = newMsg.message.split("-");
                    for (int i = 0; i < keyValues.length; i++) {
                        if (i % 2 == 0) {
                            queryData.put(keyValues[i], "");
                        } else {
                            queryData.put(keyValues[i-1], keyValues[i]);
                        }
                    }
                    queryResponse = true;
                }
                out.close();
                in.close();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
                Log.e(TAG, "failedNode" + successorPort);
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException" + Log.getStackTraceString(e));
                Log.e(TAG, "failedNode" + successorPort);
            } catch (NullPointerException e) {
                Log.e(TAG, "Null Pointer Exception" + Log.getStackTraceString(e));
                Log.e(TAG, "failedNode" + successorPort);
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
            for(String node: nodes) {
                portHashes.add(genHash(node));
                nodeToHash.put(node, genHash(node));
                hashToNode.put(genHash(node), node);
            }

            Collections.sort(portHashes);
            hashNodes = portHashes;

        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, Log.getStackTraceString(e));
        }
        return;
    }

}
