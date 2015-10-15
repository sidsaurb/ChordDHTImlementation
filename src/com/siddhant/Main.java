package com.siddhant;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Main {

    public static String IP = "172.20.205.82";
    public static int MASTER_PORT = 25602;
    static String FINGER_TABLE_DIRECTORY = "/home/siddhant/Documents/cs425/project/";
    //    static int maxValue;
    static int N = 16;
    static int logN = 4;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (Exception ignored) {
            }
            System.out.print("> Enter command: ");
            String command = scanner.next();
            switch (command) {
                case "j":
                    System.out.print("> Enter 5 digit port: ");
                    int port = scanner.nextInt();
                    int nodeHash = getHash(port);
                    ListenToSocket(nodeHash, port);
                    if (port == MASTER_PORT) {
                        ArrayList<String> contents = new ArrayList<>();
                        for (int i = 1; i <= logN; i++) {
                            int temp = (nodeHash + (int) Math.pow(2, i - 1)) % N;
                            contents.add(String.valueOf(temp) + "\t" + nodeHash + "\t" + String.valueOf(MASTER_PORT));
                        }
                        WriteToFile(String.valueOf(MASTER_PORT), contents);
                        ArrayList<String> responsibility = new ArrayList<>();
                        for (int i = 0; i < N; i++) {
                            responsibility.add(String.valueOf(i) + "\tabc_" + String.valueOf(i));
                        }
                        responsibility.add(0, "min\t" + String.valueOf((nodeHash + 1) % N));
                        WriteToFile(String.valueOf(MASTER_PORT) + "_files", responsibility);
                    } else {
                        try {
                            Socket socket = new Socket(IP, MASTER_PORT);
                            OutputStream os = socket.getOutputStream();
                            InputStream is = socket.getInputStream();
                            DataOutputStream dos = new DataOutputStream(os);
                            String toWrite = "successor\t" + String.valueOf(nodeHash) + "\n";
                            dos.writeBytes(toWrite);
                            dos.flush();
                            InputStreamReader isr = new InputStreamReader(is);
                            BufferedReader br = new BufferedReader(isr);
                            String successorNodePort = br.readLine();
                            String successorNodeHash = String.valueOf(getHash(Integer.parseInt(successorNodePort)));
                            os.close();
                            is.close();
                            dos.close();
                            isr.close();
                            br.close();
                            socket.close();

                            socket = new Socket(IP, Integer.valueOf(successorNodePort));
                            os = socket.getOutputStream();
                            is = socket.getInputStream();
                            dos = new DataOutputStream(os);
                            toWrite = "UpdateFileList\t" + String.valueOf(nodeHash) + "\n";
                            dos.writeBytes(toWrite);
                            dos.flush();
                            isr = new InputStreamReader(is);
                            br = new BufferedReader(isr);
                            String receivedData = br.readLine();
                            os.close();
                            is.close();
                            dos.close();
                            isr.close();
                            br.close();
                            socket.close();
                            String[] data = receivedData.split(";");
                            ArrayList<String> received = new ArrayList<>(Arrays.asList(data));
                            WriteToFile(String.valueOf(port) + "_files", received);


                            socket = new Socket(IP, Integer.valueOf(successorNodePort));
                            os = socket.getOutputStream();
                            dos = new DataOutputStream(os);
                            toWrite = "UpdateFingerTables\t" + String.valueOf(nodeHash) + "\t" + String.valueOf(port)
                                    + "\t" + successorNodeHash + "\t" + successorNodePort + "\n";
                            dos.writeBytes(toWrite);
                            dos.flush();
                            os.close();
                            dos.close();
                            socket.close();

                            //System.out.print(reply);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }

    private static void ListenToSocket(final int nodeId, final int port) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("> Listening on port " + String.valueOf(port));
                    ServerSocket listener = new ServerSocket(port);
                    listener.setReuseAddress(true);
                    try {
                        while (true) {
                            Socket socket = listener.accept();
                            OutputStream os = socket.getOutputStream();
                            InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                            BufferedReader br = new BufferedReader(isr);
                            String command = br.readLine();
                            if (command.contains("successor")) {
                                String targetNode = command.split("\t")[1];
                                String a = getSuccessorPort(String.valueOf(port), targetNode);
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(a + "\n");
                                dos.flush();
                            } else if (command.contains("UpdateFingerTables")) {
                                String[] temp = command.split("\t");
                                String nodeHash = temp[1];
                                String nodePort = temp[2];
                                String succHash = temp[3];
                                String succPort = temp[4];
                                if (Integer.parseInt(nodeHash) != getHash(port)) {
                                    UpdateFingerTable(String.valueOf(port), nodeHash, nodePort, succHash);
                                    String mySuccessor = getOwnSuccessor(String.valueOf(port));
                                    Socket socket1 = new Socket(IP, Integer.valueOf(mySuccessor));
                                    OutputStream os1 = socket1.getOutputStream();
                                    DataOutputStream dos1 = new DataOutputStream(os1);
                                    dos1.writeBytes(command);
                                    dos1.flush();
                                    os1.close();
                                    dos1.close();
                                    socket1.close();
                                } else {
                                    int min = 0;
                                    try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + String.valueOf(port) + "_files"))) {
                                        String line;
                                        if ((line = reader.readLine()) != null) {
                                            min = Integer.parseInt(line.split("\t")[1]);
                                        }
                                    }

                                    ArrayList<String> contents = new ArrayList<>();
                                    for (int i = 1; i <= logN; i++) {
                                        int temp1 = (nodeId + (int) Math.pow(2, i - 1)) % N;
                                        if (isBetween(min, nodeId, temp1)) {
                                            contents.add(String.valueOf(temp1) + "\t" + nodeId + "\t" + String.valueOf(port));
                                        } else {
                                            Socket socket1 = new Socket(IP, Integer.parseInt(succPort));
                                            OutputStream os1 = socket1.getOutputStream();
                                            InputStream is1 = socket1.getInputStream();
                                            DataOutputStream dos1 = new DataOutputStream(os1);
                                            String toWrite = "successor\t" + String.valueOf(temp1) + "\n";
                                            dos1.writeBytes(toWrite);
                                            dos1.flush();
                                            InputStreamReader isr1 = new InputStreamReader(is1);
                                            BufferedReader br1 = new BufferedReader(isr1);
                                            String successorNodePort = br1.readLine();
                                            String successorNodeHash = String.valueOf(getHash(Integer.parseInt(successorNodePort)));
                                            os1.close();
                                            is1.close();
                                            dos1.close();
                                            isr1.close();
                                            br1.close();
                                            socket1.close();
                                            contents.add(String.valueOf(temp1) + "\t" + successorNodeHash + "\t" + successorNodePort);
                                        }
                                    }
                                    WriteToFile(String.valueOf(port), contents);
                                }
                            } else if (command.contains("UpdateFileList")) {
                                String[] temp = command.split("\t");
                                String dataToSend = updateFileResponsibility(String.valueOf(port), temp[1]);
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(dataToSend + "\n");
                                dos.flush();
                            }
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        listener.close();
                    }
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                    //listener.close();
                }
            }
        }).start();
    }

    private static String updateFileResponsibility(String filename, String nodeHash1) {
        try {
            ArrayList<String> ownList = new ArrayList<>();
            ArrayList<String> toBeSentList = new ArrayList<>();
            Integer nodeHash = Integer.parseInt(nodeHash1);
            Integer myHash = getHash(Integer.parseInt(filename));
            ownList.add("min\t" + String.valueOf((nodeHash + 1) % N));
            try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + filename + "_files"))) {
                String line;
                if ((line = reader.readLine()) != null) {
                    toBeSentList.add(line);
                }
                while ((line = reader.readLine()) != null) {
                    String[] temp = line.split("\t");
                    Integer fileKey = Integer.parseInt(temp[0]);
                    if (isBetween((nodeHash + 1) % N, myHash, fileKey)) {
                        ownList.add(line);
                    } else {
                        toBeSentList.add(line);
                    }
                }
            }
            WriteToFile(filename + "_files", ownList);
            String toBeSent = "";
            for (String item : toBeSentList) {
                toBeSent += ";" + item;
            }
            return toBeSent;

        } catch (Exception ex) {

        }
        return "";

    }

    private static String getOwnSuccessor(String filename) {
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + filename))) {
                String line;
                if ((line = reader.readLine()) != null) {
                    String[] temp = line.split("\t");
                    return temp[2];
                }
            }
        } catch (Exception ex) {

        }
        return "";
    }


    private static String getSuccessorPort(String filename, String queryNodeId) {
        int min;
        int nodeId = getHash(Integer.parseInt(filename));
        try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + filename + "_files"))) {
            String line;
            if ((line = reader.readLine()) != null) {
                min = Integer.parseInt(line.split("\t")[1]);
                if (isBetween(min, nodeId, Integer.parseInt(queryNodeId))) {
                    return filename;
                } else {
                    String successorPort = getNextPortFromFingerTable(filename, queryNodeId);
                    String finalPort = getPortFromNextNode(queryNodeId, successorPort);
                    return finalPort;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return "";
    }

    private static String getPortFromNextNode(String queryNodeId, String successorPort) {
        try {
            Socket socket = new Socket(IP, Integer.parseInt(successorPort));
            OutputStream os = socket.getOutputStream();
            InputStream is = socket.getInputStream();
            DataOutputStream dos = new DataOutputStream(os);
            String toWrite = "successor\t" + queryNodeId + "\n";
            dos.writeBytes(toWrite);
            dos.flush();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String successorNode = br.readLine();
            os.close();
            is.close();
            dos.close();
            isr.close();
            br.close();
            socket.close();
            return successorNode;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    private static String getNextPortFromFingerTable(String filename, String queryNodeId) {
        int key = Integer.parseInt(queryNodeId);
        String previous = "";
        String succ_1 = "";
        try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + filename))) {
            String line;
            if ((line = reader.readLine()) != null) {
                String[] temp = line.split("\t");
                succ_1 = temp[2];
                previous = temp[2];
            }
            while ((line = reader.readLine()) != null) {
                String[] temp = line.split("\t");
                if (Integer.parseInt(temp[0]) == key) {
                    return temp[2];
                } else if (Integer.parseInt(temp[0]) > key) {
                    return previous;
                }
                previous = temp[2];
            }
        } catch (Exception ex) {

        }
        return succ_1;
    }


    // inclusive of min and max. in clockwise direction
    public static boolean isBetween(int min, int max, int query) {
        if (max == min) {
            return query == min;
        }
        if (max > min) {
            return query >= min && query <= max;
        } else {
            return query >= min || query <= max;
        }
    }

    static int getHash(int node) {
        return node % N;
    }

    private static void UpdateFingerTable(String filename, String nodeHash1, String nodePort1, String succHash1) {
        try {
            int nodeHash = Integer.parseInt(nodeHash1);
            int succHash = Integer.parseInt(succHash1);
            ArrayList<String> contents = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + filename))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] temp = line.split("\t");
                    if (Integer.parseInt(temp[1]) == succHash) {
                        if (!isBetween((nodeHash + 1) % N, succHash, Integer.parseInt(temp[0]))) {
                            contents.add(temp[0] + "\t" + nodeHash1 + "\t" + nodePort1);
                        } else {
                            contents.add(line);
                        }
                    } else {
                        contents.add(line);
                    }
                }
                WriteToFile(filename, contents);
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        } catch (Exception ex) {

        }
    }

    private static void WriteToFile(String filename, ArrayList<String> contents) {
        try {
            if (contents.get(0).equals("")) {
                contents.remove(0);
            }
            Path out = Paths.get(FINGER_TABLE_DIRECTORY + filename);
            Files.write(out, contents, Charset.defaultCharset());
        } catch (Exception e) {
        }
    }

    public static String toSHA1(String id) {
        MessageDigest md;
        byte[] bytes = id.getBytes();
        try {
            md = MessageDigest.getInstance("SHA-1");
            return toHexString(md.digest(bytes)).substring(0, 2);
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
    }

    public static byte[] toSHA1ByteArray(String id) {
        MessageDigest md;
        byte[] bytes = id.getBytes();
        try {
            md = MessageDigest.getInstance("SHA-1");
            return new byte[]{md.digest(bytes)[0]};
        } catch (NoSuchAlgorithmException e) {
            return new byte[]{};
        }
    }

    private static String toHexString(byte[] bytes) {
        Formatter formatter = new Formatter();
        for (byte b : bytes) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

}


