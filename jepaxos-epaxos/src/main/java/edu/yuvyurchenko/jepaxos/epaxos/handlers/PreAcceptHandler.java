package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class PreAcceptHandler extends AbstractHandler<PreAccept> {
    
    public PreAcceptHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    public void handle(PreAccept preAccept) {
        var instance = instanceSpace.getInstance(preAccept.leaderId(), preAccept.instanceId());

        if (instance != null && (instance.getStatus() == InstanceStatus.COMMITTED 
                              || instance.getStatus() == InstanceStatus.ACCEPTED)) {
            if (instance.getCommand() == null) {
                instance.setCommand(preAccept.command());
                instanceSpace.updateConflicts(preAccept.command(), 
                                              preAccept.replicaId(), 
                                              preAccept.instanceId(), 
                                              preAccept.attributes().seq());
            }
            
            return;
        }

        instanceSpace.adjustCrtInstanceId(preAccept.replicaId(), preAccept.instanceId());

        var attrUpdate = instanceSpace.updateAttributes(preAccept.replicaId(), 
                                                        preAccept.instanceId(), 
                                                        preAccept.command(), 
                                                        preAccept.attributes());
        var uncommittedDeps = instanceSpace.hasUncommittedDeps(attrUpdate.attributes().deps());
        
        var status = attrUpdate.changed() ? InstanceStatus.PREACCEPTED : InstanceStatus.PREACCEPTED_EQ;

        if (instance != null) {
            if (preAccept.ballot().lessThan(instance.getBallot())) {
                network.send(
                    new PreAcceptReply(cluster.getCurrReplicaId(), 
                                       preAccept.leaderId(),
                                       preAccept.meta(),
                                       preAccept.replicaId(), 
                                       preAccept.instanceId(), 
                                       false, 
                                       instance.getBallot(), 
                                       instance.getAttributes(),
                                       instanceSpace.commitedUpTo()));
                return;
            } else {
                instance.setCommand(preAccept.command());
                instance.setAttributes(attrUpdate.attributes());
                instance.setBallot(preAccept.ballot());
                instance.setStatus(status);
            }
        } else {
            instanceSpace.registerNewInstance(preAccept.replicaId(), 
                                              preAccept.instanceId(), 
                                              preAccept.command(), 
                                              preAccept.ballot(), 
                                              status, 
                                              attrUpdate.attributes());
        }

        instanceSpace.updateConflicts(preAccept.command(), 
                                      preAccept.replicaId(), 
                                      preAccept.instanceId(), 
                                      preAccept.attributes().seq());
        // should be attrUpdate.attributes().seq() ?
        // instanceSpace.updateConflicts(preAccept.command(), preAccept.replicaId(), preAccept.instanceId(), attrUpdate.attributes().seq());

        if (attrUpdate.changed() || uncommittedDeps 
                                 || preAccept.isNotFromCommandLeader() 
                                 || preAccept.isNotInitialBallot()) {
            network.send(
                new PreAcceptReply(cluster.getCurrReplicaId(), 
                                   preAccept.leaderId(),
                                   preAccept.meta(),
                                   preAccept.replicaId(), 
                                   preAccept.instanceId(), 
                                   true, 
                                   preAccept.ballot(), 
                                   attrUpdate.attributes(),
                                   instanceSpace.commitedUpTo()));
        } else {
            network.send(
                new PreAcceptOK(cluster.getCurrReplicaId(), 
                                preAccept.leaderId(),
                                preAccept.meta(),
                                preAccept.instanceId()));
        }


    }
}
