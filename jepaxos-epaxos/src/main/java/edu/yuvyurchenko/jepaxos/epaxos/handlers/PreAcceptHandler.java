package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class PreAcceptHandler extends AbstractHandler<PreAccept> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(PreAcceptHandler.class);

    public PreAcceptHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    public void handle(PreAccept preAccept) {
        LOGGER.debug("Receive - preAccept={}", preAccept);
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
            LOGGER.debug("Exit - PreAccept-Accept/COmmit reordering scenario");
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
                var reply = new PreAcceptReply(cluster.getCurrReplicaId(), 
                                               preAccept.leaderId(),
                                               preAccept.meta(),
                                               preAccept.replicaId(), 
                                               preAccept.instanceId(), 
                                               false, 
                                               instance.getBallot(), 
                                               instance.getAttributes(),
                                               instanceSpace.commitedUpTo());
                network.send(reply);
                LOGGER.debug("Exit - send failed reply={}", reply);
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

        NetworkMessage reply;
        if (attrUpdate.changed() || uncommittedDeps 
                                 || preAccept.isNotFromCommandLeader() 
                                 || preAccept.isNotInitialBallot()) {
            reply = new PreAcceptReply(cluster.getCurrReplicaId(), 
                                           preAccept.leaderId(),
                                           preAccept.meta(),
                                           preAccept.replicaId(), 
                                           preAccept.instanceId(), 
                                           true, 
                                           preAccept.ballot(), 
                                           attrUpdate.attributes(),
                                           instanceSpace.commitedUpTo());
        } else {
            reply = new PreAcceptOK(cluster.getCurrReplicaId(), 
                                    preAccept.leaderId(),
                                    preAccept.meta(),
                                    preAccept.instanceId());
        }
        network.send(reply);
        LOGGER.debug("Exit - send ok reply={}", reply);
    }
}
