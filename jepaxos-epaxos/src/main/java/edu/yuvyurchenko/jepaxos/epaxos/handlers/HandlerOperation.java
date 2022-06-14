package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;

public class HandlerOperation<R extends NetworkMessage> implements Runnable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(HandlerOperation.class);
    
    private final R message;
    private final Handler<R> handler;

    public HandlerOperation(R message, Handler<R> handler) {
        this.message = message;
        this.handler = handler;
    }
    
    @Override
    public void run() {
        try {
            handler.handle(message);
        } catch (Exception e) {
            LOGGER.error("Message handling raised an exception", e);
        }
    }
}
