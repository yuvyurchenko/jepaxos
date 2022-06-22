package edu.yuvyurchenko.jepaxos.epaxos.plugins;

import edu.yuvyurchenko.jepaxos.epaxos.Replica;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.Handler;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;

/**
 * Provides network implementation client<->replica 
 * and replica<->replica communications via {@link NetworkMessage}.
 * 
 * JEPaxos uses replica ids for routing, 
 * so mapping of replica ids to actual network hosts is up to the Network implementation.
 */
public interface Network {
    /**
     * Sends either {@link ExternalMessage} reply to the client 
     * or {@link InternalMessage} to another replica 
     */
    void send(NetworkMessage msg);
    /**
     * {@link Replica} uses this method to register inbound message handlers.
     * This Network implementation responsibility to invoke 
     * the corresponding handler while receive a message
     * @param <R> an inboud message type parameter
     * @param requestType {@link ExternalMessage} request or {@link InternalMessage} classes
     * @param handler corresponding message handler
     */
    <R extends NetworkMessage> void registerHandler(Class<R> requestType, Handler<R> handler);
}
