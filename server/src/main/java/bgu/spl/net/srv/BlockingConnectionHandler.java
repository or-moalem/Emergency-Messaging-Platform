package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.MessagingProtocol;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

/*This pattern corresponds to the Thread-Per-Client (TPC) pattern,
 where each client is handled by a separate thread.*/

public class BlockingConnectionHandler<T> implements Runnable, ConnectionHandler<T> {

    //link between client and server, processing the messages received from the client
    private final MessagingProtocol<T> protocol;
    //Converts between textual/binary messages and objects (encoding and decoding)
    private final MessageEncoderDecoder<T> encdec;

    private final Socket sock;
    private BufferedInputStream in;
    private BufferedOutputStream out;
    private volatile boolean connected = true;
    private final Connections<T> connections;

    public BlockingConnectionHandler(Socket sock, MessageEncoderDecoder<T> reader, 
    MessagingProtocol<T> protocol, int connectionId, Connections<T> connections) {
        this.sock = sock;
        this.encdec = reader;
        this.protocol = protocol;
        this.connections = connections;
        protocol.start(connectionId, connections); 
    }

    @Override
    public void run() {
        
        try (Socket sock = this.sock) { //just for automatic closing
            in = new BufferedInputStream(sock.getInputStream());
            out = new BufferedOutputStream(sock.getOutputStream());

            while (!protocol.shouldTerminate() && connected )  { //&&   (read = in.read()) >= 0) 
                int read = in.read();
                if (read >= 0) {
                    T nextMessage = encdec.decodeNextByte((byte) read);
                    if (nextMessage != null) {
                        T response = protocol.process(nextMessage);
                        if (response != null) {
                            out.write(encdec.encode(response));
                            out.flush();
                        }
                    }
                }
            }



        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public void close() throws IOException {
        connected = false;
        sock.close();
    }

    @Override
    public void send(T msg) {
        try {
            out.write(encdec.encode(msg));
            out.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
