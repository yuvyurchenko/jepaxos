package edu.yuvyurchenko.jepaxos.epaxos.messages;

public sealed interface NetworkMessage permits ExternalMessage, InternalMessage {
    String src();
    String dest();
    MessageMetadata meta();
}
