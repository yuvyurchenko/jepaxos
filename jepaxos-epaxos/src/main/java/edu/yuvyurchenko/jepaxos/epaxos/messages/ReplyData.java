package edu.yuvyurchenko.jepaxos.epaxos.messages;

public record ReplyData(ReplyType type,
                        String clientId, 
                        MessageMetadata meta) {}
