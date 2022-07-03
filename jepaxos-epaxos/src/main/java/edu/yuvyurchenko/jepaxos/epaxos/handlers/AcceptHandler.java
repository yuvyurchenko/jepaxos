package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class AcceptHandler extends AbstractHandler<Accept> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AcceptHandler.class);

    public AcceptHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    public void handle(Accept accept) {
        LOGGER.debug("Receive - accept={}", accept);
        var instance = instanceSpace.getInstance(accept.leaderId(), accept.instanceId());

        if (accept.attributes().seq() >= instanceSpace.getMaxSeq()) {
            instanceSpace.setMaxSeq(accept.attributes().seq() + 1);
        }

        if (instance != null && (instance.getStatus() == InstanceStatus.COMMITTED 
                              || instance.getStatus() == InstanceStatus.EXECUTED)) {
            LOGGER.debug("Exit - late message");
            return;
        }

        instanceSpace.adjustCrtInstanceId(accept.leaderId(), accept.instanceId());

        if (instance != null) {
            if (accept.ballot().lessThan(instance.getBallot())) {
                var reply = new AcceptReply(cluster.getCurrReplicaId(), 
                                            accept.leaderId(), 
                                            accept.meta(), 
                                            accept.replicaId(), 
                                            accept.instanceId(), 
                                            false, 
                                            instance.getBallot());
                network.send(reply);
                LOGGER.debug("Exit - send failed reply={}", reply);
                return;
            } else {
                instance.setStatus(InstanceStatus.ACCEPTED);
                instance.setAttributes(accept.attributes());
            }
        } else {
            instanceSpace.registerNewInstance(accept.leaderId(), 
                                              accept.instanceId(), 
                                              accept.command(), 
                                              accept.ballot(), 
                                              InstanceStatus.ACCEPTED, 
                                              accept.attributes());
        }

        var reply = new AcceptReply(cluster.getCurrReplicaId(), 
                                    accept.leaderId(), 
                                    accept.meta(), 
                                    accept.replicaId(), 
                                    accept.instanceId(), 
                                    true, 
                                    accept.ballot());
        network.send(reply);
        LOGGER.debug("Exit - send ok reply={}", reply);
    }

}
