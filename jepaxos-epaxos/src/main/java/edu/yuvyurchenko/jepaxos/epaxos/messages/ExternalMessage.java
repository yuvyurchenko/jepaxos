package edu.yuvyurchenko.jepaxos.epaxos.messages;

import edu.yuvyurchenko.jepaxos.epaxos.model.Command;

public sealed interface ExternalMessage extends NetworkMessage permits ExternalMessage.Request,
                                                                       ExternalMessage.RequestReply,
                                                                       ExternalMessage.Read,
                                                                       ExternalMessage.ReadReply,
                                                                       ExternalMessage.RequestAndRead,
                                                                       ExternalMessage.RequestAndReadReply {
    record Request(String src, 
                   String dest, 
                   MessageMetadata meta, 
                   Command command) implements ExternalMessage {}
    
    record RequestReply(String src, 
                        String dest, 
                        MessageMetadata meta, 
                        boolean ok,
                        int errorCode,
                        String errorText) implements ExternalMessage {}

    record Read(String src, 
                String dest, 
                MessageMetadata meta,
                Command command) implements ExternalMessage {}
    
    record ReadReply(String src, 
                     String dest, 
                     MessageMetadata meta,
                     boolean ok,
                     Object value,
                     int errorCode,
                     String errorText) implements ExternalMessage {}
    
    record RequestAndRead(String src, 
                          String dest, 
                          MessageMetadata meta,
                          Command command) implements ExternalMessage {}
         
    record RequestAndReadReply(String src, 
                               String dest, 
                               MessageMetadata meta,
                               boolean ok,
                               Object value,
                               int errorCode,
                               String errorText) implements ExternalMessage {}

    public static RequestReply okRequestReply(String src, 
                                              String dest, 
                                              MessageMetadata meta) {
        return new RequestReply(src, dest, meta, true, 0, null);
    }

    public static RequestReply errorRequestReply(String src, 
                                                 String dest, 
                                                 MessageMetadata meta, 
                                                 int errorCode,
                                                 String errorText) {
        return new RequestReply(src, dest, meta, false, errorCode, errorText);
    }


    public static ReadReply okReadReply(String src, 
                                        String dest, 
                                        MessageMetadata meta,
                                        Object value) {
        return new ReadReply(src, dest, meta, true, value, 0, null);
    }

    public static ReadReply errorReadReply(String src, 
                                           String dest, 
                                           MessageMetadata meta,
                                           int errorCode,
                                           String errorText) {
        return new ReadReply(src, dest, meta, false, null, errorCode, errorText);
    }

    public static RequestAndReadReply okRequestAndReadReply(String src, 
                                                            String dest, 
                                                            MessageMetadata meta,
                                                            Object value) {
        return new RequestAndReadReply(src, dest, meta, true, value, 0, null);
    }

    public static RequestAndReadReply errorRequestAndReadReply(String src, 
                                                               String dest, 
                                                               MessageMetadata meta,
                                                               int errorCode,
                                                               String errorText) {
        return new RequestAndReadReply(src, dest, meta, false, null, errorCode, errorText);
    }

}
