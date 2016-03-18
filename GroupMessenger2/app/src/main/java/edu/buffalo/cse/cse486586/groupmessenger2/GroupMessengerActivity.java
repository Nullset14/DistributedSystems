package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.KeyEvent;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStreamReader;
import java.io.StreamCorruptedException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.lang.Math;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.PriorityQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    static String failedNode = "00000";

    static volatile double maxGroupAgreedId = 0;
    static volatile double maxProposedId = 0;
    static volatile int IdMess = 0;

    static Hashtable<String, Double> proposedNumber = new Hashtable<String, Double>();
    static Set<String> receivedList = new HashSet<String>();
    static PriorityQueue<Message> pq = new PriorityQueue<Message>();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

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
            return;
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        final EditText editText = (EditText) findViewById(R.id.editText1);
        editText.setOnKeyListener(new View.OnKeyListener() {
            @Override
            public boolean onKey(View v, int keyCode, KeyEvent event) {
                if ((event.getAction() == KeyEvent.ACTION_DOWN) &&
                        (keyCode == KeyEvent.KEYCODE_ENTER)) {

                    /*
                     * If the key is pressed (i.e., KeyEvent.ACTION_DOWN) and it is an enter key
                     * (i.e., KeyEvent.KEYCODE_ENTER), then we display the string. Then we create
                     * an AsyncTask that sends the string to the remote AVD.
                     */

                    String msg = editText.getText().toString() + "\n";
                    editText.setText(""); // This is one way to reset the input box.

                    TextView localTextView = (TextView) findViewById(R.id.textView1);
                    localTextView.append("\t" + msg); // This is one way to display a string.

                    TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                    remoteTextView.append("\n");

                    /*
                     * Note that the following AsyncTask uses AsyncTask.SERIAL_EXECUTOR, not
                     * AsyncTask.THREAD_POOL_EXECUTOR as the above ServerTask does. To understand
                     * the difference, please take a look at
                     * http://developer.android.com/reference/android/os/AsyncTask.html
                     */
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
                    return true;
                }
                return false;
            }
        });

        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.

                TextView localTextView = (TextView) findViewById(R.id.textView1);
                localTextView.append("\t" + msg); // This is one way to display a string.

                TextView remoteTextView = (TextView) findViewById(R.id.textView2);
                remoteTextView.append("\n");

                /*
                 * Note that the following AsyncTask uses AsyncTask.SERIAL_EXECUTOR, not
                 * AsyncTask.THREAD_POOL_EXECUTOR as the above ServerTask does. To understand
                 * the difference, please take a look at
                 * http://developer.android.com/reference/android/os/AsyncTask.html
                 */
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    /***
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
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
            uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
            uriBuilder.scheme("content");

            while (true) {
                try {
                    ServerSocket serverSocket = sockets[0];
                    Socket localSocket = serverSocket.accept();
                    DataInputStream messages = new DataInputStream(localSocket.getInputStream());
                    String incomingMessage = messages.readLine();
                    incomingMessage.replace("\n", "");

                    publishProgress(new String[]{incomingMessage});
                    PrintStream out = new PrintStream(localSocket.getOutputStream());

                    Message protocolMessage = new Message();
                    protocolMessage.splitMessage(incomingMessage);
                    synchronized (this) {

                        if (protocolMessage.protocol.equals("propose")) {

                            /* Propose a sequence number */

                            incrementId(protocolMessage.message);
                            protocolMessage.maxProposedId = maxProposedId;

                            String tempPort = protocolMessage.receiverPort;
                            protocolMessage.receiverPort = protocolMessage.senderPort;
                            protocolMessage.senderPort = tempPort;
                            protocolMessage.protocol = "proposed";

                            /* Add the proposed number to the Queue*/
                            if(!protocolMessage.originalSender.equals(failedNode))
                                pq.add(protocolMessage);

                            out.println(protocolMessage.toString());
                            out.close();

                        } else if (protocolMessage.protocol.equals("fix")) {

                            /* Fix the agreed sequence number */

                            maxGroupAgreedId = Math.max(maxGroupAgreedId, protocolMessage.maxProposedId);
                            double maxP = Math.max(proposedNumber.get(protocolMessage.message),
                                    protocolMessage.maxProposedId);
                            proposedNumber.put(protocolMessage.message, maxP);
                            receivedList.add(protocolMessage.message);

                            /* ISIS Algorithm in action */

                            PriorityQueue<Message> pqr = new PriorityQueue<Message>();

                            while (pq.size() > 0) {
                                Message temp = pq.remove();

                                /*
                                if (failedNode.equals(temp.senderPort) && !receivedList.contains(protocolMessage.message)) {
                                    continue;
                                }
                                */

                                if (failedNode.equals(temp.originalSender)) {
                                    continue;
                                }

                                if (protocolMessage.message.equals(temp.message)) {
                                   continue;
                                } else {
                                    pqr.add(temp);
                                }
                            }

                            pqr.add(protocolMessage);
                            pq = pqr;

                            while (pq.size() > 0) {
                                if (receivedList.contains(pq.peek().message)) {
                                    ContentValues cv = new ContentValues();
                                    cv.put("key", "");
                                    cv.put("value", pq.remove().message);

                                    getContentResolver().insert(uriBuilder.build(), cv);
                                } else
                                    break;
                            }

                            out.println("fixed");
                            out.close();
                        }
                    }
                } catch (IOException ioException) {
                    Log.e(TAG, "ServerTask socket IOException" + Log.getStackTraceString(ioException));
                } catch (Exception e) {
                    Log.e(TAG, "ServerTask socket UnknownException" + Log.getStackTraceString(e));
                }
            }
        }

        protected void incrementId(String message) {
            maxProposedId = Math.max(maxProposedId, maxGroupAgreedId) + 1;
            proposedNumber.put(message, maxProposedId);
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();

            TextView remoteTextView = (TextView) findViewById(R.id.textView2);
            remoteTextView.append(strReceived + "\t\n");

            TextView localTextView = (TextView) findViewById(R.id.textView2);
            localTextView.append("\n");

            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */

            String filename = "SimpleMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;

            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }

            return;
        }
    }

    /*
     * @author stevko
     */

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
            uriBuilder.scheme("content");

            synchronized (this) {
                msgs[0] = msgs[0].replace("\n", "");
                IdMess++;
                double tempMaxP = 0;

                for (String remotePort :
                        new String[]{REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4}) {

                    String msgToSend = new Message(msgs[1], remotePort, msgs[1], maxProposedId, msgs[0], "propose").toString();

                    if (remotePort.equals(failedNode))
                        continue;

                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));

                        socket.setSoTimeout(3000);
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        PrintStream out = new PrintStream(socket.getOutputStream());
                        out.println(msgToSend);

                        String incomingMessage = in.readLine();
                        incomingMessage.replace("\n", "");

                        Message protocolMessage = new Message();
                        protocolMessage.splitMessage(incomingMessage);

                        /* Store and decide final agreed sequence number */
                        double maxP;

                        if (proposedNumber.get(protocolMessage.message) == null) {
                            maxP = protocolMessage.maxProposedId;
                        } else {
                            maxP = Math.max(proposedNumber.get(protocolMessage.message),
                                    protocolMessage.maxProposedId);
                        }

                        proposedNumber.put(protocolMessage.message, maxP);
                        tempMaxP = Math.max(tempMaxP, Math.max(maxGroupAgreedId, maxP));

                        out.close();
                        in.close();
                        socket.close();

                    } catch (SocketTimeoutException e) {
                        Log.e(TAG, "ClientDown Exception" + Log.getStackTraceString(e));
                        failedNode = remotePort;
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
                        failedNode = remotePort;
                    } catch (StreamCorruptedException e) {
                        Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
                        failedNode = remotePort;
                    } catch (EOFException e) {
                        Log.e(TAG, "ClientTask EOFException" + Log.getStackTraceString(e));
                        failedNode = remotePort;
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException" + Log.getStackTraceString(e));
                        failedNode = remotePort;
                    } catch (NullPointerException e) {
                        Log.e(TAG, "Null Pointer Exception" + Log.getStackTraceString(e));
                        failedNode = remotePort;
                    }

                }

                /* Broadcast final agreed sequence number */
                for (String remotePort :
                        new String[]{REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4}) {

                    if (remotePort.equals(failedNode))
                        continue;

                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        socket.setSoTimeout(3000);
                        PrintStream out = new PrintStream(socket.getOutputStream());
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        maxGroupAgreedId = tempMaxP;
                        String finalMessage = new Message(msgs[1], remotePort, msgs[1],
                                tempMaxP, msgs[0],
                                "fix").toString();
                        out.println(finalMessage);

                        String incomingMessage = in.readLine();
                        incomingMessage.replace("\n", "");

                        out.close();
                        in.close();
                        socket.close();
                    } catch (SocketTimeoutException e) {
                        Log.e(TAG, "ClientDown Exception" + Log.getStackTraceString(e));
                        failedNode = remotePort;
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
                    } catch (StreamCorruptedException e) {
                        Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
                        failedNode = remotePort;
                    } catch (EOFException e) {
                        Log.e(TAG, "ClientTask EOFException" + Log.getStackTraceString(e));
                        failedNode = remotePort;
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException" + Log.getStackTraceString(e));
                        failedNode = remotePort;
                    } catch (NullPointerException e) {
                        Log.e(TAG, "Null Pointer Exception" + Log.getStackTraceString(e));
                        failedNode = remotePort;
                    }
                }
            }
            return null;
        }
    }


    /* Compare between Messages */

    private class Message implements Comparable<Message> {

        public String senderPort;
        public String receiverPort;
        double maxProposedId;
        public String originalSender;
        public String message;
        public String protocol;

        Message(String senderPort, String receiverPort, String originalSender, double maxProposedId,
                String message, String protocol) {

            this.senderPort = senderPort;
            this.receiverPort = receiverPort;
            this.originalSender = originalSender;
            this.maxProposedId = maxProposedId;
            this.message = message.replace("\n", "");
            this.protocol = protocol;
        }

        Message() {
        }

        public String toString() {
            return (senderPort + "_" +
                    receiverPort + "_" +
                    originalSender + "_" +
                    Double.toString(maxProposedId) + "_" +
                    message.replace("\n", "") + "_" + protocol);
        }

        public void splitMessage(String msg) {
            String[] messages = msg.split("_");
            this.senderPort = messages[0];
            this.receiverPort = messages[1];
            this.originalSender = messages[2];
            this.maxProposedId = Double.parseDouble(messages[3]);
            this.message = messages[4].replace("\n", "");
            this.protocol = messages[5];
        }

        public int compareTo(Message msg) {
            int comparedValue = Double.compare(this.maxProposedId, msg.maxProposedId);
            if (comparedValue == 0) {
                return Double.compare(Double.parseDouble(this.originalSender),
                        Double.parseDouble(msg.originalSender));
            }
            return comparedValue;
        }
    }
}
