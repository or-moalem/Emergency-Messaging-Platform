package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.MessagingProtocol;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.function.Supplier;

/*
Thread-Per-Client (TPC) server manager.
Each new client that connects to the server is handled by a dedicated thread.
Suitable for a small number of clients.
*/
public abstract class BaseServer<T> implements Server<T> {

    private final int port;
    private final Supplier<MessagingProtocol<T>> protocolFactory;
    private final Supplier<MessageEncoderDecoder<T>> encdecFactory;
    private ServerSocket sock;
    private final ConnectionsImpl<T> connections;


    //Added for the connrction id gennerator
    private static int connectionCounter = 0; 

    public BaseServer(
            int port,
            Supplier<MessagingProtocol<T>> protocolFactory,
            Supplier<MessageEncoderDecoder<T>> encdecFactory) {

        this.port = port;
        this.protocolFactory = protocolFactory;
        this.encdecFactory = encdecFactory;
		this.sock = null;
        this.connections = new ConnectionsImpl<>();

    }

    @Override
    public void serve() {

        try (ServerSocket serverSock = new ServerSocket(port)) {
        
			System.out.println("Server started");

            this.sock = serverSock; //just to be able to close

            while (!Thread.currentThread().isInterrupted()) {

                Socket clientSock = serverSock.accept();

                int connectionId = generateConnectionId();

                BlockingConnectionHandler<T> handler = new BlockingConnectionHandler<>(
                        clientSock,
                        encdecFactory.get(),
                        protocolFactory.get(),
                        connectionId,
                        connections);
                connections.addClient(connectionId, handler);
                execute(handler);
            }
        } catch (IOException ex) {
            System.out.println("[dubug] entered catch!!!");
        }

        System.out.println("server closed!!!");
    }

    public static synchronized int generateConnectionId() {
        connectionCounter++; 
        return connectionCounter; 
    }

    @Override
    public void close() throws IOException {
		if (sock != null)
			sock.close();
    }

    protected  void execute(BlockingConnectionHandler<T>  handler){
        new Thread(handler).start();
    }
}
