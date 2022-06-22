package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;

@FunctionalInterface
public interface Handler<M extends NetworkMessage> {
    void handle(M message);
}
