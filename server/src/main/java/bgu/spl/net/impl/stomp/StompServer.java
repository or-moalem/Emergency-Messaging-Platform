package bgu.spl.net.impl.stomp;

import java.util.concurrent.ConcurrentHashMap;
import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Server;
import bgu.spl.net.impl.stomp.StompMessageEncoderDecoder;
import bgu.spl.net.impl.stomp.StompMessagingProtocolImpl;

public class StompServer {

    public static void main(String[] args) {// Determine the port from the command-line arguments, default to 7777
        // Default port number
        int port = 7777;
        // Check if port is provided
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]); // Parse the first argument as port
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number. Using default port 7777.");
            }
        }

        // Check if reactor/tpc is provided
        if (args.length < 2) {
            System.err.println("Missing server type (reactor/tpc).");
            System.exit(1);
        }

        String serverType = args[1].toLowerCase();

        switch (serverType) {
            case "reactor":
                // Reactor server
                int numThreads = Runtime.getRuntime().availableProcessors();
                Server.<String>reactor(
                        numThreads,
                        port,
                        StompMessagingProtocolImpl::new,
                        StompMessageEncoderDecoder::new
                ).serve();
                break;

            case "tpc":
                // Thread-Per-Client server
                Server.<String>threadPerClient(
                        port,
                        StompMessagingProtocolImpl::new,
                        StompMessageEncoderDecoder::new
                ).serve();
                break;

            default:
                System.err.println("Invalid server type. Use 'reactor' or 'tpc'.");
                System.exit(1);
        }
    }
}
