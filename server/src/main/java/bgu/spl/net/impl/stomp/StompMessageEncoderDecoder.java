package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.MessageEncoderDecoder;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;


public class StompMessageEncoderDecoder implements MessageEncoderDecoder<String>{

    private ByteArrayOutputStream buffer = new ByteArrayOutputStream(); // לשמירת הבייטים

    //Reads the next byte and adds it to the current message in the decoding process.
    //Returns the complete message (if the frame is complete). 
    //Returns null if the frame has not yet finished.
    @Override
    public String decodeNextByte(byte nextByte) {
        // buffer is used to collect bytes.
        // '\u0000' is an end of frame mark
        if (nextByte == '\u0000') {
            // Convert the collected bytes to a string
            String result = new String(buffer.toByteArray(), StandardCharsets.UTF_8);
            buffer.reset();
            return result;
        }
        // otherwize
        buffer.write(nextByte);
        return null; // frame has not yet finished
    }

    //Encodes a message from its representation (eg, a string) into a byte array suitable for transmission
    //Returns A byte array that represents the message
    @Override
    public byte[] encode(String message) {
        return (message + "\u0000").getBytes(StandardCharsets.UTF_8);
    }
}
