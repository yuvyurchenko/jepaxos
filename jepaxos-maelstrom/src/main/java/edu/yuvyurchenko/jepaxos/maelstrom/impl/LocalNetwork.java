package edu.yuvyurchenko.jepaxos.maelstrom.impl;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import edu.yuvyurchenko.jepaxos.epaxos.handlers.Handler;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;

public class LocalNetwork implements Network {
    final Map<Class<?>, Handler> handlerRegistry = new HashMap<>(); 
    final Queue<NetworkMessage> messageQueue = new LinkedList<>();

    @Override
    public void send(NetworkMessage msg) {
        this.messageQueue.add(msg);
    }
    
    @Override
    public <R extends NetworkMessage> void registerHandler(Class<R> requestType, Handler<R> handler) {
        this.handlerRegistry.put(requestType, handler);
    }

    public void receive(NetworkMessage msg) {
        handlerRegistry.get(msg.getClass()).handle(msg);
    }

    public int messageQueueSize() {
        return messageQueue.size();
    }

    public Stream<NetworkMessage> drainMessageQueue() {
        return IntStream.range(0, messageQueue.size()).mapToObj(i -> messageQueue.poll());
    }

    public NetworkMessage pollMessageQueue() {
        return messageQueue.poll();
    }
}
