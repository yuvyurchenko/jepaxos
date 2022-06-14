package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public abstract class AbstractHandler<M extends NetworkMessage> implements Handler<M> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHandler.class);

    protected final Cluster cluster;
    protected final Network network;
    protected final InstanceSpace instanceSpace;
    
    protected AbstractHandler(Cluster cluster, 
                              Network network,
                              InstanceSpace instanceSpace) {
        this.cluster = cluster;
        this.network = network;
        this.instanceSpace = instanceSpace;
    }

    protected <T extends NetworkMessage> void broadcast(Function<String, T> msgFactory) {
        cluster.getAllReplicaIds().stream()
            .filter(id -> !id.equals(cluster.getCurrReplicaId()))
            .map(msgFactory)
            .peek(msg -> LOGGER.debug("Broadcast message={}", msg))
            .forEach(network::send);
    }
    
    protected boolean hasQuorum(int oks) {
        return oks >= cluster.getAllReplicaIds().size() / 2;
    }
}
