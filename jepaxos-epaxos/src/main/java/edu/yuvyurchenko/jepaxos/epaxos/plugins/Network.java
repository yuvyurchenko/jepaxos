package edu.yuvyurchenko.jepaxos.epaxos.plugins;

import edu.yuvyurchenko.jepaxos.epaxos.handlers.Handler;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;

public interface Network {
    void send(NetworkMessage msg);
    <R extends NetworkMessage> void registerHandler(Class<R> requestType, Handler<R> handler);
}
