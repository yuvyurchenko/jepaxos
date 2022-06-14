package edu.yuvyurchenko.jepaxos.epaxos.plugins;

import edu.yuvyurchenko.jepaxos.epaxos.handlers.HandlerOperation;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;

public interface ExecutingDriver {
    <R extends NetworkMessage> void enqueue(HandlerOperation<R> operation);
    void enqueue(Runnable operation);
    void schedule(Runnable operation);
    void sleep(long timeMs);
    void shutdown();
}
