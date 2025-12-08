package bgu.spl.net.srv;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionsImpl<T> implements Connections<T> {

    /*
     * a map that connect between connectionId and ConnectionHandler<T>
     * (object that represent the connection between server aand client)
     */
    private ConcurrentHashMap<Integer, ConnectionHandler<T>> clients;
    private final ConcurrentHashMap<String, Set<Integer>> channels;
    private ConcurrentHashMap<String, String> registeredUsers;
    private ConcurrentHashMap<String, Integer> activeConnections;

    public ConnectionsImpl() {
        this.clients = new ConcurrentHashMap<>();
        this.channels = new ConcurrentHashMap<>();
        this.registeredUsers = new ConcurrentHashMap<>();
        this.activeConnections = new ConcurrentHashMap<>();
    }
    
    
    @Override
    public boolean send(int connectionId, T msg) {
        ConnectionHandler<T> handler = clients.get(connectionId);
        if (handler != null) {
            handler.send(msg);
            return true;
        }
        return false;
    }

    @Override
    public void send(String channel, T msg) {
        Set<Integer> subscribers = channels.get(channel);
        if (subscribers != null) {
            for (Integer connectionId : subscribers) {
                send(connectionId, msg);
            }
        }
    }

    @Override
    public void disconnect(int connectionId) {
        //Remove client
        clients.remove(connectionId);

        //Remove from topics
        for (String topic : channels.keySet()) {
            if (channels.get(topic).contains(connectionId)) {
                channels.get(topic).remove(connectionId);
            }
        }

        //Remove from active connections
        String username = activeConnections.entrySet()
            .stream()
            .filter(entry -> entry.getValue() == connectionId)
            .map(Map.Entry::getKey)
            .findFirst()
            .orElse(null);

        if (username != null) {
            activeConnections.remove(username);
        }

    }

    public void addClient(int connectionId, ConnectionHandler<T> handler) {
        if(handler == null) {
            return;
        }
        clients.put(connectionId, handler);
    }


    public boolean registerUser(String username, String password) {
        if (!registeredUsers.containsKey(username)) {
            registeredUsers.put(username, password);
            return true;
        }
        boolean correctPassword = registeredUsers.get(username).equals(password);
        return correctPassword;
    }

    public boolean isUserActive(String username) {
        boolean isActive = activeConnections.containsKey(username);
        return isActive;
    }

    public void activateUser(String username, int connectionId) {
        activeConnections.put(username, connectionId);
    }

    @Override
    public boolean subscribe(String topic, int connectionId, String subscriptionId) {
        try {            
            //Initialize topic if doesn't exist
            channels.putIfAbsent(topic, ConcurrentHashMap.newKeySet());
            
            //Check if already subscribed
            if (channels.get(topic).contains(connectionId)) {
                return false;
            }
            
            //Add subscription
            boolean success = channels.get(topic).add(connectionId);
            return success;
            
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean unsubscribe(String topic, int connectionId) {
        try {
            for (Map.Entry<String, Set<Integer>> entry : channels.entrySet()) {
                Set<Integer> subscribers = entry.getValue();
                if (subscribers.contains(connectionId)) {
                    boolean removed = subscribers.remove(connectionId);
                    return removed;
                }
            }
            return false;
        } catch (Exception e) {
            System.err.println("[ERROR][Unsubscribe] Failed: " + e.getMessage());
            return false;
        }
    }

    public boolean isUserSubscribedToTopic(int connectionId, String topic) {
    
        Set<Integer> subscribers = channels.get(topic);
    
        boolean isSubscribed = subscribers != null && subscribers.contains(connectionId);
    
        return isSubscribed;
    }
}

