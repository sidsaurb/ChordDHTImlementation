package com.siddhant;

import java.io.*;
import java.net.BindException;
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

    public static String IP = "localhost";
    public static int MASTER_PORT = 12345;
    static String FINGER_TABLE_DIRECTORY = "/home/siddhant/Documents/cs425/project/fingertables/";
    static String FILE_DIRECTORY = "/home/siddhant/Documents/cs425/project/files/";

//    static int N = 65536;
//    static int logN = 16;

    static int N = 64;
    static int logN = 6;

    public static boolean alreadyListening = false;
    public static int myPort = 0;
    public static ServerSocket listener;


    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            try {
                Thread.sleep(500);
            } catch (Exception ignored) {
            }
            System.out.print("> Enter command: ");
            String command = scanner.next();
            switch (command) {
                // join
                case "j":
                    if (!alreadyListening) {
                        System.out.print("> Enter 5 digit port: ");
                        int port = scanner.nextInt();
                        int nodeHash = getHash(port);
                        alreadyListening = true;
                        ListenToSocket(nodeHash, port);
                        myPort = port;
                        if (port == MASTER_PORT) {
                            InitializeMasterPort(nodeHash);
                        } else {
                            InitializeOtherPorts(port, nodeHash);
                        }
                    } else {
                        System.out.println("This program is already listening");
                    }
                    break;
                // just listen without joining
                case "l":
                    if (!alreadyListening) {
                        System.out.print("> Enter 5 digit port: ");
                        int port1 = scanner.nextInt();
                        int nodeHash1 = getHash(port1);
                        alreadyListening = true;
                        ListenToSocket(nodeHash1, port1);
                        myPort = port1;
                    } else {
                        System.out.println("This program is already listening");
                    }
                    break;
                // find a file location
                case "f":
                    if (alreadyListening) {
                        System.out.print("> Enter file name: ");
                        String filename = scanner.next();
                        int fileHash = getSHA1Hash(filename);
                        String filePort = getFilePort(fileHash);
                        if (filePort.equals("null")) {
                            System.out.println("The file does not exists");
                        } else {
                            System.out.println("The file exists at port: " + filePort);
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                // upload a file
                case "u":
                    if (alreadyListening) {
                        System.out.print("> Enter file name: ");
                        String filename1 = scanner.next();
                        File f = new File(FILE_DIRECTORY + String.valueOf(myPort) + "/upload/" + filename1);
                        if (f.exists() && !f.isDirectory()) {
                            int fileHash1 = getSHA1Hash(filename1);
                            uploadFile(fileHash1);
                        } else {
                            System.out.println("File doesn't exist");
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                // download a file
                case "d":
                    if (alreadyListening) {
                        System.out.print("> Enter file name: ");
                        String filename2 = scanner.next();
                        int fileHash2 = getSHA1Hash(filename2);
                        String filePort2 = getFilePort(fileHash2);
                        if (filePort2.equals("null")) {
                            System.out.println("The file does not exists");
                        } else {
                            DownloadFile(filePort2, filename2);
                            System.out.println("Downloaded from port: " + filePort2);
//                            System.out.println("The file exists at port: " + filePort2);
                        }
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                // find route
                case "r":
                    if (alreadyListening) {
                        System.out.print("> Enter file name: ");
                        String filename2 = scanner.next();
                        int fileHash2 = getSHA1Hash(filename2);
                        String route = getFileRoute(fileHash2);
                        System.out.println(route);
                    } else {
                        System.out.println("The program is not listening to any port");
                    }
                    break;
                // quit the program
                case "q":
                    break;
                default:
                    System.out.println("Invalid command");
                    break;
            }
            if (command.equals("q")) {
                try {
                    System.out.println("Program exiting..");
                    listener.close();
                } catch (Exception e) {
//                    e.printStackTrace();
                }
                break;
            }
        }
    }

    // takes port which has the file and downloads the file from it
    private static void DownloadFile(String filePort, String filename) {
        try {
            Socket socket = new Socket(IP, Integer.parseInt(filePort));
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            String toWrite = "downloadfile\t" + String.valueOf(filename) + "\n";
            dos.writeBytes(toWrite);
            dos.flush();
            InputStream in = socket.getInputStream();
            String filepath = FILE_DIRECTORY + String.valueOf(myPort) + "/download/" + filename;
            OutputStream out = new FileOutputStream(filepath);
            copy(in, out);
            out.close();
            in.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // copies the streams
    static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[8192];
        int len;
        while ((len = in.read(buf)) != -1) {
            out.write(buf, 0, len);
        }
    }

    private static void uploadFile(int fileHash) {
        try {
//            String portOfFile = getSuccessorPort(String.valueOf(getHash(myPort)), String.valueOf(fileHash));
            String portOfFile = getSuccessorPort(String.valueOf((myPort)), String.valueOf(fileHash));

            // if my responsibility
            if (Integer.parseInt(portOfFile) == myPort) {
                String fileDestinationPort = getFileDestinationPort(portOfFile, fileHash);
                if (fileDestinationPort.equals("null")) {
                    // update the files table to point to the same port
                    changeFilePort(portOfFile, fileHash, portOfFile);
                    System.out.println("File uploaded");
                } else {
                    System.out.println("File with the same key already exists");
                }
            } else {
                // if not my responsibility then forward command to the successor port
                // but first check if file with same key already exists or not by
                // sending a filename command to the successor node

                Socket socket1 = new Socket(IP, Integer.parseInt(portOfFile));
                DataOutputStream dos1 = new DataOutputStream(socket1.getOutputStream());
                String toWrite1 = "filename\t" + String.valueOf(fileHash) + "\n";
                dos1.writeBytes(toWrite1);
                dos1.flush();
                InputStreamReader isr1 = new InputStreamReader(socket1.getInputStream());
                BufferedReader br1 = new BufferedReader(isr1);
                String fileDestinationPort = br1.readLine();
                socket1.close();
                // if file not exists send the file_upload command
                if (fileDestinationPort.equals("null")) {
                    Socket socket = new Socket(IP, Integer.parseInt(portOfFile));
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    String toWrite = "file_upload\t" + String.valueOf(fileHash) + "\t" + String.valueOf(myPort) + "\n";
                    dos.writeBytes(toWrite);
                    dos.flush();
                    InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                    BufferedReader br = new BufferedReader(isr);
                    String response = br.readLine();
                    socket.close();
                    if (response.equals("success")) {
                        System.out.println("File uploaded");
                    } else {
                        System.out.println("File with the same key already exists");
                    }
                } else {
                    // Show error message
                    System.out.println("File with the same key already exists");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // update file responsibility table of the node, corresponding to the key fileHash to point
    // to containingPort
    private static void changeFilePort(String portOfFile, int fileHash, String containingPort) {
        ArrayList<String> contents = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + portOfFile + "_files"))) {
            String line;
            line = reader.readLine();
            contents.add(line);
            while ((line = reader.readLine()) != null) {
                String[] temp = line.split("\t");
                Integer fileKey = Integer.parseInt(temp[0]);
                if (fileHash == fileKey) {
                    contents.add(temp[0] + "\t" + containingPort);
                } else {
                    contents.add(line);
                }
            }
            WriteToFile(portOfFile + "_files", contents);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getFilePort(int fileHash) {
        try {
//            String portOfFile = getSuccessorPort(String.valueOf(getHash(myPort)), String.valueOf(fileHash));
            String portOfFile = getSuccessorPort(String.valueOf((myPort)), String.valueOf(fileHash));
            if (Integer.parseInt(portOfFile) == myPort) {
                String fileDestinationPort = getFileDestinationPort(portOfFile, fileHash);
                if (fileDestinationPort.equals("null")) {
                    return "null";
                } else {
                    return fileDestinationPort;
                }
            } else {
                Socket socket = new Socket(IP, Integer.parseInt(portOfFile));
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                String toWrite = "filename\t" + String.valueOf(fileHash) + "\n";
                dos.writeBytes(toWrite);
                dos.flush();
                InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                BufferedReader br = new BufferedReader(isr);
                String fileDestinationPort = br.readLine();
                socket.close();
                if (fileDestinationPort.equals("null")) {
                    return "null";
                } else {
                    return fileDestinationPort;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "null";
    }

    private static String getFileRoute(int fileHash) {
        try {
//            String portOfFile = getSuccessorPort(String.valueOf(getHash(myPort)), String.valueOf(fileHash));
            String portOfFile = getSuccessorPort(String.valueOf((myPort)), String.valueOf(fileHash));
            if (Integer.parseInt(portOfFile) == myPort) {
                String fileDestinationPort = getFileDestinationPort(portOfFile, fileHash);
                if (fileDestinationPort.equals("null")) {
                    return String.valueOf(myPort) + " -> " + "null";
                } else {
                    return String.valueOf(myPort) + " -> " + fileDestinationPort;
                }
            } else {
                Socket socket = new Socket(IP, Integer.parseInt(portOfFile));
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                String toWrite = "route\t" + String.valueOf(fileHash) + "\n";
                dos.writeBytes(toWrite);
                dos.flush();
                InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                BufferedReader br = new BufferedReader(isr);
                String route = br.readLine();
                socket.close();
                return String.valueOf(myPort) + " -> " + route;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "null";
    }

    private static String getFileDestinationPort(String portOfFile, int fileHash) {
        try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + portOfFile + "_files"))) {
            String line;
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] temp = line.split("\t");
                Integer fileKey = Integer.parseInt(temp[0]);
                if (fileHash == fileKey) {
                    return temp[1];
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "null";
    }

    private static void InitializeOtherPorts(int port, int nodeHash) {
        try {
            // query master node for successor of this newly created node
            Socket socket = new Socket(IP, MASTER_PORT);
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            String toWrite = "successor\t" + String.valueOf(nodeHash) + "\n";
            dos.writeBytes(toWrite);
            dos.flush();
            InputStreamReader isr = new InputStreamReader(socket.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            String successorNodePort = br.readLine();
            String successorNodeHash = String.valueOf(getHash(Integer.parseInt(successorNodePort)));
            socket.close();

            if (successorNodeHash.equals(String.valueOf(getHash(port)))) {
                System.out.println("Node with the same key already exists.. try another value");
            } else {

                // send a command to its successor node to update its file responsibility
                // the successor node will update its responsibility and reply back with
                // this new node's responsibility
                socket = new Socket(IP, Integer.valueOf(successorNodePort));
                dos = new DataOutputStream(socket.getOutputStream());
                toWrite = "UpdateFileList\t" + String.valueOf(nodeHash) + "\n";
                dos.writeBytes(toWrite);
                dos.flush();
                isr = new InputStreamReader(socket.getInputStream());
                br = new BufferedReader(isr);
                String receivedData = br.readLine();
                socket.close();
                String[] data = receivedData.split(";");
                ArrayList<String> received = new ArrayList<>(Arrays.asList(data));
                WriteToFile(String.valueOf(port) + "_files", received);

                // send a command to its successor node to update its finger table
                // the successor node will in-turn forward this to its own successor till
                // this new node is reached again. After hitting back again this node
                // will create it's finger table
                socket = new Socket(IP, Integer.valueOf(successorNodePort));
                dos = new DataOutputStream(socket.getOutputStream());
                toWrite = "UpdateFingerTables\t" + String.valueOf(nodeHash) + "\t" + String.valueOf(port)
                        + "\t" + successorNodeHash + "\t" + successorNodePort + "\n";
                dos.writeBytes(toWrite);
                dos.flush();
                socket.close();

                CreateDirectories();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void InitializeMasterPort(int nodeHash) {
        ArrayList<String> contents = new ArrayList<>();
        for (int i = 1; i <= logN; i++) {
            int temp = (nodeHash + (int) Math.pow(2, i - 1)) % N;
            contents.add(String.valueOf(temp) + "\t" + nodeHash + "\t" + String.valueOf(MASTER_PORT));
        }
        WriteToFile(String.valueOf(MASTER_PORT), contents);
        ArrayList<String> responsibility = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            responsibility.add(String.valueOf(i) + "\tnull");
        }
        responsibility.add(0, "min\t" + String.valueOf((nodeHash + 1) % N));
        WriteToFile(String.valueOf(MASTER_PORT) + "_files", responsibility);
        CreateDirectories();
    }

    private static void CreateDirectories() {
        File theDir = new File(FILE_DIRECTORY + String.valueOf(myPort));
        if (!theDir.exists()) {
            try {
                theDir.mkdir();
            } catch (SecurityException se) {
            }
        }
        File theDir1 = new File(FILE_DIRECTORY + String.valueOf(myPort) + "/upload");
        if (!theDir1.exists()) {
            try {
                theDir1.mkdir();
            } catch (SecurityException se) {
            }
        }
        File theDir2 = new File(FILE_DIRECTORY + String.valueOf(myPort) + "/download");
        if (!theDir2.exists()) {
            try {
                theDir2.mkdir();
            } catch (SecurityException se) {
            }
        }
    }

    private static void ListenToSocket(final int hash, final int port) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("> Listening on port " + String.valueOf(port));
                    listener = new ServerSocket(port);
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
                                UpdateFingerTableAndForwardCommand(command, port, hash, nodeHash, nodePort, succHash, succPort);
                            } else if (command.contains("UpdateFileList")) {
                                String[] temp = command.split("\t");
                                String dataToSend = updateFileResponsibility(String.valueOf(port), temp[1]);
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(dataToSend + "\n");
                                dos.flush();
                            } else if (command.contains("filename")) {
                                // name is misleading. actually query for port number of a file.
//                                System.out.println("File name query on port " + String.valueOf(port));
                                String[] temp = command.split("\t");
                                String dataToSend = getFileDestinationPort(String.valueOf(port), Integer.parseInt(temp[1]));
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(dataToSend + "\n");
                                dos.flush();
                            } else if (command.contains("file_upload")) {
                                String[] temp = command.split("\t");
                                changeFilePort(String.valueOf(port), Integer.parseInt(temp[1]), temp[2]);
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes("success" + "\n");
                                dos.flush();
                            } else if (command.contains("downloadfile")) {
                                String[] temp = command.split("\t");
                                InputStream in = new FileInputStream(FILE_DIRECTORY + String.valueOf(port) + "/upload/" + temp[1]);
                                copy(in, os);
                                os.close();
                                in.close();
                            } else if (command.contains("route")) {
                                String[] temp = command.split("\t");
                                String a = getFileRoute(Integer.parseInt(temp[1]));
                                DataOutputStream dos = new DataOutputStream(os);
                                dos.writeBytes(a + "\n");
                                dos.flush();
                            }
                        }
                    } catch (Exception ex) {
//                        ex.printStackTrace();
                        alreadyListening = false;
                        listener.close();
                    }
                } catch (BindException ignored) {
                    alreadyListening = false;
                    System.out.println("Another process is already listening on port " + String.valueOf(port));
                } catch (Exception ignored) {
//                    ignored.printStackTrace();
                    alreadyListening = false;
                }
            }
        }).start();
    }

    private static void UpdateFingerTableAndForwardCommand(String command, int currentPort, int currentHash, String nodeHash, String nodePort, String succHash, String succPort) {
        try {
            // If not hit back on current node update your current finger table
            // and forward the query
            if (Integer.parseInt(nodeHash) != currentHash) {
                UpdateFingerTable(String.valueOf(currentPort), nodeHash, nodePort, succHash);
                String mySuccessor = getOwnSuccessor(String.valueOf(currentPort));
                Socket socket1 = new Socket(IP, Integer.valueOf(mySuccessor));
                DataOutputStream dos1 = new DataOutputStream(socket1.getOutputStream());
                dos1.writeBytes(command);
                dos1.flush();
                dos1.close();
                socket1.close();
            }
            // hit back on current node
            else {

                // first find min responsibility of this node
                int min = 0;
                try (BufferedReader reader = new BufferedReader(new FileReader(FINGER_TABLE_DIRECTORY + String.valueOf(currentPort) + "_files"))) {
                    String line;
                    if ((line = reader.readLine()) != null) {
                        min = Integer.parseInt(line.split("\t")[1]);
                    }
                }
                ArrayList<String> contents = new ArrayList<>();
                for (int i = 1; i <= logN; i++) {
                    int temp1 = (currentHash + (int) Math.pow(2, i - 1)) % N;

                    // if the value of temp1 is between min and currentHash then successor of temp1
                    // is definitely currentHash
                    if (isBetween(min, currentHash, temp1)) {
                        contents.add(String.valueOf(temp1) + "\t" + currentHash + "\t" + String.valueOf(currentPort));
                    } else {
                        Socket socket1 = new Socket(IP, Integer.parseInt(succPort));
                        DataOutputStream dos1 = new DataOutputStream(socket1.getOutputStream());
                        String toWrite = "successor\t" + String.valueOf(temp1) + "\n";
                        dos1.writeBytes(toWrite);
                        dos1.flush();
                        InputStreamReader isr1 = new InputStreamReader(socket1.getInputStream());
                        BufferedReader br1 = new BufferedReader(isr1);
                        String successorNodePort = br1.readLine();
                        String successorNodeHash = String.valueOf(getHash(Integer.parseInt(successorNodePort)));
                        socket1.close();
                        contents.add(String.valueOf(temp1) + "\t" + successorNodeHash + "\t" + successorNodePort);
                    }
                }
                WriteToFile(String.valueOf(currentPort), contents);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // update file responsibility table when a new node joins
    // filename: filename of the file to be updated
    // nodeHash1: nodeHash of the new node being joined
    // returns the responsibility of the newly formed node
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
            ex.printStackTrace();
        }
        return "";
    }


    // returns a nodes own successor. Node denoted by filename
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
            ex.printStackTrace();
        }
        return "";
    }


    // returns successor port from a node denoted by filename and query node denoted by queryNodeId
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
                    return getPortFromNextNode(queryNodeId, successorPort);
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
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            String toWrite = "successor\t" + queryNodeId + "\n";
            dos.writeBytes(toWrite);
            dos.flush();
            InputStreamReader isr = new InputStreamReader(socket.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            String successorNode = br.readLine();
            socket.close();
            return successorNode;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }


    // where to forward a query next
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
        } catch (Exception ignored) {

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
        } catch (Exception ignored) {
        }
    }

    public static int getSHA1Hash(String id) {
        MessageDigest md;
        byte[] bytes = id.getBytes();
        try {
            md = MessageDigest.getInstance("SHA-1");
            byte[] temp = md.digest(bytes);
            int hash = temp[1] | temp[0];
            return hash % N;
        } catch (NoSuchAlgorithmException e) {
            return 0;
        }
    }
}


