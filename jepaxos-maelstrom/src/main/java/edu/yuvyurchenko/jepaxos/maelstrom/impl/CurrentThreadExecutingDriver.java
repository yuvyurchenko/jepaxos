package edu.yuvyurchenko.jepaxos.maelstrom.impl;

import edu.yuvyurchenko.jepaxos.epaxos.handlers.HandlerOperation;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.ExecutingDriver;

public class CurrentThreadExecutingDriver implements ExecutingDriver {
    private Runnable scheduledOperation;

    @Override
    public <R extends NetworkMessage> void enqueue(HandlerOperation<R> operation) {
        operation.run();
    }

    @Override
    public void enqueue(Runnable operation) {
        operation.run();
    }
    
    @Override
    public void schedule(Runnable operation) {
        this.scheduledOperation = operation;
    }

    @Override
    public void sleep(long timeMs) {
        
    }

    @Override
    public void shutdown() {

    }

    public void executeScheduled() {
        scheduledOperation.run();
    }
}
