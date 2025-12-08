package bgu.spl.net.srv;

import java.io.IOException;

public interface Connections<T> {

    boolean send(int connectionId, T msg);

    void send(String channel, T msg);

    void disconnect(int connectionId);

    void addClient (int connectionId, ConnectionHandler<T> handler);

    boolean subscribe(String topic, int connectionId, String subscriptionId);

    public boolean unsubscribe(String topic, int connectionId);

}
