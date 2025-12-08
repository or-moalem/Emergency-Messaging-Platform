package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.impl.rci.Command;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.ConnectionsImpl;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;


public class StompMessagingProtocolImpl implements StompMessagingProtocol<String> {
//fileds
    private int connectionId;
    private Connections<String> connections;
    private boolean shouldTerminate;

    public static ConcurrentHashMap<String, String> registeredUsers = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> subscriptions = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Integer> activeConnections = new ConcurrentHashMap<>();


    //functions
    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
        this.shouldTerminate = false;
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }
    
    @Override
    public String process(String message) {
        String[] lines = message.split("\n");
        String command = lines[0];

        switch (command) {
            case "CONNECT":
                return processConnect(lines);
                
            case "SEND":
                return processSend(lines);
                
            case "SUBSCRIBE":
                return processSubscribe(lines);
                
            case "UNSUBSCRIBE":
                return processUnsubscribe(lines);
                
            case "DISCONNECT":
                return processDisconnect(lines);
            default:
                handleError("Unknown command: " + command, null);
                break;
        }
        return null;
    }

    private String processConnect(String[] lines) {
        String username = null;
        String password = null;
        String version = null;
        String host = null;
        
        for (String line : lines) {
            if (line.startsWith("login:")) {
                username = line.substring(6);
            } else if (line.startsWith("passcode:")) {
                password = line.substring(9);
            } else if (line.startsWith("accept-version:")) {
                version = line.substring(15).trim(); 
            }else if (line.startsWith("host:")) {
            host = line.substring(5).trim();
            }
        }
    if (username == null || password == null || version == null || host == null) {
        handleError("Missing required headers", null);
        return null;
    }

    if (registeredUsers.containsKey(username)) {
        if (!registeredUsers.get(username).equals(password)) {
            handleError("Wrong password", null);
            return null;
        }
        if (activeConnections.containsKey(username)) {
            handleError("User already logged in", null);
            return null;
        }
    } else {
        registeredUsers.put(username, password);
    }
    activeConnections.put(username, connectionId);

    StringBuilder builder = new StringBuilder();
    builder.append("CONNECTED");
    builder.append("\n");
    builder.append("version:");
    builder.append(version);
    builder.append("\n");
    builder.append("\n");

    String connectedFrame = builder.toString();

    return connectedFrame;
    }

    private String processSubscribe(String[] lines) {
        String destination = null;
        String subscriptionId = null;
        String receiptId = null;

        for (String line : lines) {
        if (line.startsWith("destination:")) {
            destination = line.substring(12).trim();

            if (destination.startsWith("/")) {
                destination = destination.substring(1);
            }
        } else if (line.startsWith("id:")) {
            subscriptionId = line.substring(3).trim(); 
        } else if (line.startsWith("receipt:")) {
            receiptId = line.substring(8).trim(); 
        }
    }

        if (destination == null || subscriptionId == null) {
            handleError("Missing destination or id header or recipt id ", receiptId);
            return null;
        }

        if (!connections.subscribe(destination, connectionId, subscriptionId)) {
        handleError("Failed to subscribe", receiptId);
        return null;
    }

    if (receiptId == null) {
        handleError("Missing receipt header", null);
        return null;
    }


        StringBuilder builder = new StringBuilder();
        builder.append("RECEIPT\n");
        builder.append("receipt-id:").append(receiptId).append("\n\n");

        System.out.println("Joined channel " + destination);
        String receipt = builder.toString();

        return receipt;
    }

    private String processSend(String[] lines) {
    String destination = null;

    // Extract destination
    for (String line : lines) {
        if (line.startsWith("destination:/")) {
            destination = line.substring(12);
            if (destination.startsWith("/")) {
                destination = destination.substring(1);
                }
                if (destination.endsWith("/")) {
                destination = destination.substring(0, destination.length() - 1); 
                }
            break;
        }
    }

    if (destination == null) {
        handleError("Missing destination header", null);
        return null;
    }

    // Check subscription
    if (!((ConnectionsImpl<String>)connections).isUserSubscribedToTopic(connectionId, destination)) {
        handleError("User not subscribed to topic " + destination, null);
        return null;
    }
    

    // Convert to MESSAGE frame
    String messageFrame = "MESSAGE\n" + String.join("\n", lines).replace("SEND\n", "");
    connections.send(destination, messageFrame);
    return null;
    }

    private String processUnsubscribe(String[] lines) {
    String subscriptionId = null;
    String receiptId = null;

    for (String line : lines) {
        if (line.startsWith("id:")) {
            subscriptionId = line.substring(3).trim();
        } else if (line.startsWith("receipt:")) {
            receiptId = line.substring(8).trim();
        }
    }

    if (subscriptionId == null || receiptId == null) {
        handleError("Missing subscription ID or receipt header", receiptId);
        return null;
    }

    if (!((ConnectionsImpl<String>) connections).unsubscribe(subscriptionId, connectionId)) {
        handleError("Not subscribed with this ID", receiptId);
        return null;
    }

    StringBuilder builder = new StringBuilder();
    builder.append("RECEIPT");
    builder.append("\n");
    builder.append("receipt-id:");
    builder.append(receiptId);
    builder.append("\n");
    builder.append("\n");

    String receiptFrame = builder.toString();

    return receiptFrame;
    }

    private void handleError(String errorMessage, String receiptId) {

        StringBuilder errorFrame = new StringBuilder();

        errorFrame.append("ERROR\n");

        errorFrame.append("message:").append(errorMessage).append("\n");

        if (receiptId != null && !receiptId.isEmpty()) {
        errorFrame.append("receipt-id:").append(receiptId).append("\n");
    }

        errorFrame.append("\n");
        connections.send(connectionId, errorFrame.toString());
        shouldTerminate = true;
    }  

    private String processDisconnect(String[] lines) {
    String receiptId = null;
    for (String line : lines) {
        if (line.startsWith("receipt:")) {
            receiptId = line.substring(8).trim();
            break;
        }
    }

    if (receiptId == null) {
        handleError("Missing receipt header", null);
        return null;
    }

    for (Map.Entry<String, Integer> entry : activeConnections.entrySet()) {
        if (entry.getValue() == connectionId) {
            activeConnections.remove(entry.getKey());
            break;
        }
    }

    subscriptions.forEach((destination, subscribers) -> subscribers.remove(connectionId));

    String receipt = String.format("RECEIPT\nreceipt-id:%s\n\n", receiptId);    
    shouldTerminate = true;
    return receipt;

    }
}
