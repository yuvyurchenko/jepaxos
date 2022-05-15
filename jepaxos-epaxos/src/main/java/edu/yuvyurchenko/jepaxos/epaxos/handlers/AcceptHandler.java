package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class AcceptHandler extends AbstractHandler<Accept> {
    
    public AcceptHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    public void handle(Accept accept) {
        var instance = instanceSpace.getInstance(accept.leaderId(), accept.instanceId());

        if (accept.attributes().seq() >= instanceSpace.getMaxSeq()) {
            instanceSpace.setMaxSeq(accept.attributes().seq() + 1);
        }

        if (instance != null && (instance.getStatus() == InstanceStatus.COMMITTED 
                              || instance.getStatus() == InstanceStatus.EXECUTED)) {
            return;
        }

        instanceSpace.adjustCrtInstanceId(accept.leaderId(), accept.instanceId());

        if (instance != null) {
            if (accept.ballot().lessThan(instance.getBallot())) {
                network.send(
                    new AcceptReply(cluster.getCurrReplicaId(), 
                                    accept.leaderId(), 
                                    accept.meta(), 
                                    accept.replicaId(), 
                                    accept.instanceId(), 
                                    false, 
                                    instance.getBallot()));
                return;
            } else {
                instance.setStatus(InstanceStatus.ACCEPTED);
                instance.setAttributes(accept.attributes());
            }
        } else {
            instanceSpace.registerNewInstance(accept.leaderId(), 
                                              accept.instanceId(), 
                                              null, 
                                              accept.ballot(), 
                                              InstanceStatus.ACCEPTED, 
                                              accept.attributes());
        }

        network.send(
            new AcceptReply(cluster.getCurrReplicaId(), 
                            accept.leaderId(), 
                            accept.meta(), 
                            accept.replicaId(), 
                            accept.instanceId(), 
                            true, 
                            accept.ballot()));
    }

}
