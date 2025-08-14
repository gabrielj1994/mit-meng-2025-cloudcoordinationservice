package dsg.ccsvc.client;

//import org.apache.zookeeper.cli.CliCommand;
//import org.apache.zookeeper.cli.CommandFactory;
//import org.apache.zookeeper.cli.CommandNotFoundException;

import dsg.ccsvc.command.CSvcCommand;
import dsg.ccsvc.command.CSvcCommandFactory;
import org.apache.commons.cli.ParseException;
//import org.apache.zookeeper.cli.CliCommand;
//import org.apache.zookeeper.cli.CliParseException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class CSvcSandboxParser {
// TODO: Consider the purpose of this class
    static final Map<String, String> commandMapStr = new HashMap<String, String>();
    static final Map<String, CSvcCommand> commandMap = new HashMap<String, CSvcCommand>();


    private Map<String, String> options = new HashMap<String, String>();
    private List<String> cmdArgs = null;
    private String command = null;
    public static final Pattern ARGS_PATTERN = Pattern.compile("\\s*([^\"\']\\S*|\"[^\"]*\"|'[^']*')\\s*");
    public static final Pattern QUOTED_PATTERN = Pattern.compile("^([\'\"])(.*)(\\1)$");

    public CSvcSandboxParser() {
        options.put("server", "localhost:2181");
        options.put("timeout", "30000");

        Stream.of(CSvcCommandFactory.Command.values())
                .map(command -> CSvcCommandFactory.getInstance(command))
                // add all commands to commandMapCli and commandMap
                .forEach(cliCommand ->{
                    cliCommand.addToMap(commandMap);
                    commandMapStr.put(
                            cliCommand.getCmdStr(),
                            cliCommand.getOptionStr());
                });
    }

    public String getCmdArgument(int index) {
        return cmdArgs.get(index);
    }

    public int getNumArguments() {
        return cmdArgs.size();
    }

    public String getCommand() {
        return command;
    }

    public String[] getArgArray() {
        return cmdArgs.toArray(new String[0]);
    }

    public boolean hasOption(String option) {
        return options.containsKey(option);
    }

    public String getOptionValue(String option) {
        return options.getOrDefault(option, "null");
    }

    public void resetParser() {
        options = new HashMap<>();
        options.put("server", "localhost:2181");
        options.put("timeout", "30000");
        cmdArgs = null;
        command = null;
    }

    /**
     * Parses a command line that may contain one or more flags
     * before an optional command string
     * @param args command line arguments
     * @return true if parsing succeeded, false otherwise.
     */
    public boolean parseOptions(String[] args) {
        List<String> argList = Arrays.asList(args);
        Iterator<String> it = argList.iterator();
//java -cp ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar dsg.tbd.SandboxLogServer 8001 /shared_data
//java -cp ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar dsg.tbd.SandboxCLI ec2-100-27-34-135.compute-1.amazonaws.com 8001 delete /membership_test/member2
//java -cp ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar dsg.tbd.modular.CoordinationSvcDriver -server c2-100-27-34-135.compute-1.amazonaws.com -port 8001 -root-path /shared_data -fs
//java -cp ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar dsg.tbd.modular.CoordinationSvcCLI -server ec2-100-27-34-135.compute-1.amazonaws.com -port 8001 delete /membership_test/member2
        while (it.hasNext()) {
            String opt = it.next();
            try {
                if (opt.equals("-server")) {
                    options.put("server", it.next());
                } else if (opt.equals("-port")) {
                    options.put("port", it.next());
                } else if (opt.equals("-root-path")) {
                    options.put("root-path", it.next());
                } else if (opt.equals("-fs")) {
                    options.put("fs-operator", "true");
                } else if (opt.equals("-timeout")) {
                    // TODO
                    options.put("timeout", it.next());
                } else if (opt.equals("-r")) {
                    // TODO
                    options.put("readonly", "true");
                } else if (opt.equals("-client-configuration")) {
                    // TODO
                    options.put("client-configuration", it.next());
                } else if (opt.equals("-waitforconnection")) {
                    // TODO
                    options.put("waitforconnection", "true");
                }
            } catch (NoSuchElementException e) {
                System.out.println("Error: no argument found for option " + opt);
                return false;
            }

            if (!opt.startsWith("-")) {
                command = opt;
                cmdArgs = new ArrayList<String>();
                cmdArgs.add(command);
                while (it.hasNext()) {
                    cmdArgs.add(it.next());
                }
                return true;
            }
        }
        return true;
//
//        if (args.length < 2) return;
//
//        int port = Integer.parseInt(args[0]);
//        root_path = args[1];
//        log_path = Paths.get(root_path, ".zk_log");
//        znode_root = "/";
//
////        initialize();
//
//        //TODO: Implement proper logging
//        System.out.println("Opening server port");
//
//        Socket socket;
//        try (ServerSocket serverSocket = new ServerSocket(port)) {
//            System.out.println(String.format("Listening on port %d", port));
//
//            while (true) {
//                //TODO: Handle more than happy path
//                socket = serverSocket.accept();
//                System.out.println("Accepted client connection");
////                Thread.sleep(2000);
////                Thread.sleep(250);
//
//                InputStream input = socket.getInputStream();
//                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
//
//                OutputStream outputStream = socket.getOutputStream();
//                PrintWriter writer = new PrintWriter(outputStream, true);
//
//                String command = reader.readLine();
//                System.out.println(String.format("[cmd=%s]", command));
//
//                String[] tokens = command.split(" ");
//                if (tokens.length > 0) {
//                    String output;
//
//                    switch (tokens[0]) {
//                        case "get":
//                            output = executeGet(tokens);
//                            break;
//                        case "create":
//                            output = executeCreate(tokens);
//                            break;
//                        case "set":
//                            output = executeSet(tokens);
//                            break;
//                        case "ls":
//                            output = executeList(tokens);
//                            break;
//                        case "delete":
//                            output = executeDelete(tokens);
//                            break;
//                        case "connect":
////                            writer.println(String.format("Connection Established [port=%d]", port));
//                            output = serviceClient(reader, writer);
//                            break;
//                        default:
//                            //TODO: Proper exception handling
//                            output = "Unsupported Operation.";
//                    }
//
//                    writer.println(output);
//                    System.out.println(String.format("[output=%s]", output));
////                    Thread.sleep(250);
//                }
//                socket.close();
//            }
//        }
////        } catch (InterruptedException e) {
////            throw new RuntimeException(e);
////        }
//        return false;
    }

    /**
     * Breaks a string into command + arguments.
     * @param cmdstring string of form "cmd arg1 arg2..etc"
     * @return true if parsing succeeded.
     */
    public boolean parseCommand(String cmdstring) {
        Matcher matcher = ARGS_PATTERN.matcher(cmdstring);

        List<String> args = new LinkedList<String>();
        while (matcher.find()) {
            String value = matcher.group(1);
            if (QUOTED_PATTERN.matcher(value).matches()) {
                // Strip off the surrounding quotes
                value = value.substring(1, value.length() - 1);
            }
            args.add(value);
        }
        if (args.isEmpty()) {
            return false;
        }
        command = args.get(0);
        cmdArgs = args;
        return true;
    }

//    public boolean processCommand() {
    public CSvcCommand processCommand() throws ParseException {
        // TODO: Consider diverging from ZK implementation more, as the parser instance is tied to 1 command parsing
        // TODO: Exception handling
        String[] args = this.getArgArray();
//        String cmd = command;
        if (args.length < 2) {
            throw new IllegalArgumentException("No command entered");
        }

        if (!commandMapStr.containsKey(command)) {
            usage();
            throw new IllegalArgumentException("Command not found " + command);
        }

//        int port = Integer.parseInt(args[0]);
//        root_path = args[1];
//        log_path = Paths.get(root_path, ".zk_log");
//        znode_root = "/";
        // execute from commandMap
//        CSvcCommand cmd = commandMap.get(command);
        if (commandMap.containsKey(command)) {
            return commandMap.get(command).parse(args);
        } else {
            usage();
            // TODO: Return error no-op command
            return null;
        }
    }

    void usage() {
        System.out.println("-server host -port port -client-configuration properties-file cmd args");
        //java -cp ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar dsg.tbd.SandboxCLI ec2-100-27-34-135.compute-1.amazonaws.com 8001 delete /membership_test/member2
        List<String> cmdList = new ArrayList<String>(commandMap.keySet());
        Collections.sort(cmdList);
        for (String cmd : cmdList) {
            System.out.println("\t" + cmd + " " + commandMap.get(cmd));
        }
    }

//    public CSvcCommand parse(String[] args) {
//        if (args.length < 2) return;
//
//        int port = Integer.parseInt(args[0]);
//        root_path = args[1];
//        log_path = Paths.get(root_path, ".zk_log");
//        znode_root = "/";
//
//        initialize();
//
//        //TODO: Implement proper logging
//        System.out.println("Opening server port");
//
//        Socket socket;
//        try (ServerSocket serverSocket = new ServerSocket(port)) {
//            System.out.println(String.format("Listening on port %d", port));
//
//            while (true) {
//                //TODO: Handle more than happy path
//                socket = serverSocket.accept();
//                System.out.println("Accepted client connection");
////                Thread.sleep(2000);
////                Thread.sleep(250);
//
//                InputStream input = socket.getInputStream();
//                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
//
//                OutputStream outputStream = socket.getOutputStream();
//                PrintWriter writer = new PrintWriter(outputStream, true);
//
//                String command = reader.readLine();
//                System.out.println(String.format("[cmd=%s]", command));
//
//                String[] tokens = command.split(" ");
//                if (tokens.length > 0) {
//                    String output;
//
//                    switch (tokens[0]) {
//                        case "get":
//                            output = executeGet(tokens);
//                            break;
//                        case "create":
//                            output = executeCreate(tokens);
//                            break;
//                        case "set":
//                            output = executeSet(tokens);
//                            break;
//                        case "ls":
//                            output = executeList(tokens);
//                            break;
//                        case "delete":
//                            output = executeDelete(tokens);
//                            break;
//                        case "connect":
////                            writer.println(String.format("Connection Established [port=%d]", port));
//                            output = serviceClient(reader, writer);
//                            break;
//                        default:
//                            //TODO: Proper exception handling
//                            output = "Unsupported Operation.";
//                    }
//
//                    writer.println(output);
//                    System.out.println(String.format("[output=%s]", output));
////                    Thread.sleep(250);
//                }
//                socket.close();
//            }
//        }
////        } catch (InterruptedException e) {
////            throw new RuntimeException(e);
////        }
//    }
//
//}

//
//        private Map<String, String> options = new HashMap<String, String>();
//        private List<String> cmdArgs = null;
//        private String command = null;
//        public static final Pattern ARGS_PATTERN = Pattern.compile("\\s*([^\"\']\\S*|\"[^\"]*\"|'[^']*')\\s*");
//        public static final Pattern QUOTED_PATTERN = Pattern.compile("^([\'\"])(.*)(\\1)$");
//
//        public MyCommandOptions() {
//            options.put("server", "localhost:2181");
//            options.put("timeout", "30000");
//        }
//
//        public String getOption(String opt) {
//            return options.get(opt);
//        }
//
//        public String getCommand() {
//            return command;
//        }
//
//        public String getCmdArgument(int index) {
//            return cmdArgs.get(index);
//        }
//
//        public int getNumArguments() {
//            return cmdArgs.size();
//        }
//
//        public String[] getArgArray() {
//            return cmdArgs.toArray(new String[0]);
//        }
//
//        /**
//         * Parses a command line that may contain one or more flags
//         * before an optional command string
//         * @param args command line arguments
//         * @return true if parsing succeeded, false otherwise.
//         */
//public boolean parseOptions(String[] args) {
//    List<String> argList = Arrays.asList(args);
//    Iterator<String> it = argList.iterator();
//
//    while (it.hasNext()) {
//        String opt = it.next();
//        try {
//            if (opt.equals("-server")) {
//                options.put("server", it.next());
//            } else if (opt.equals("-timeout")) {
//                options.put("timeout", it.next());
//            } else if (opt.equals("-r")) {
//                options.put("readonly", "true");
//            } else if (opt.equals("-client-configuration")) {
//                options.put("client-configuration", it.next());
//            } else if (opt.equals("-waitforconnection")) {
//                options.put("waitforconnection", "true");
//            }
//        } catch (NoSuchElementException e) {
//            System.err.println("Error: no argument found for option " + opt);
//            return false;
//        }
//
//        if (!opt.startsWith("-")) {
//            command = opt;
//            cmdArgs = new ArrayList<String>();
//            cmdArgs.add(command);
//            while (it.hasNext()) {
//                cmdArgs.add(it.next());
//            }
//            return true;
//        }
//    }
//    return true;
//}
//
//    /**
//     * Breaks a string into command + arguments.
//     * @param cmdstring string of form "cmd arg1 arg2..etc"
//     * @return true if parsing succeeded.
//     */
//    public boolean parseCommand(String cmdstring) {
//        Matcher matcher = ARGS_PATTERN.matcher(cmdstring);
//
//        List<String> args = new LinkedList<String>();
//        while (matcher.find()) {
//            String value = matcher.group(1);
//            if (QUOTED_PATTERN.matcher(value).matches()) {
//                // Strip off the surrounding quotes
//                value = value.substring(1, value.length() - 1);
//            }
//            args.add(value);
//        }
//        if (args.isEmpty()) {
//            return false;
//        }
//        command = args.get(0);
//        cmdArgs = args;
//        return true;
//    }
//
}


