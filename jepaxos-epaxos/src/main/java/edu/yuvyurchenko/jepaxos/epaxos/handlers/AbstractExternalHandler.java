package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.PreAccept;
import edu.yuvyurchenko.jepaxos.epaxos.model.Instance;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public abstract class AbstractExternalHandler<T extends ExternalMessage> extends AbstractHandler<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractExternalHandler.class);

    protected AbstractExternalHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    public void handle(T msg) {
        LOGGER.debug("Incomming External Message - msg={}", msg);
        var instance = registerNewCommandLeaderInstance(msg);

        instanceSpace.updateConflicts(instance);

        broadcast(extReplicaId -> new PreAccept(cluster.getCurrReplicaId(), 
                                                extReplicaId,
                                                msg.meta(),
                                                instance.getReplicaId(), 
                                                instance.getReplicaId(), 
                                                instance.getInstanceId(), 
                                                instance.getBallot(), 
                                                instance.getCommand(), 
                                                instance.getAttributes()));
    }

    protected abstract Instance registerNewCommandLeaderInstance(T msg);
    
}
