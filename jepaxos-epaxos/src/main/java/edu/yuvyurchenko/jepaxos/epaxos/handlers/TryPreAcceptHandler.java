package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.yuvyurchenko.jepaxos.epaxos.CommandOperationRegistry;
import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class TryPreAcceptHandler extends AbstractRecoveryHandler<TryPreAccept> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(TryPreAcceptHandler.class);

    public TryPreAcceptHandler(Cluster cluster, 
                               Network network,
                               InstanceSpace instanceSpace,
                               CommandOperationRegistry operationRegistry) {
        super(cluster, network, instanceSpace, operationRegistry);
    }

    public void handle(TryPreAccept tryPreAccept) {
        LOGGER.debug("Receive - tryPreAccept={}", tryPreAccept);
        var instance = instanceSpace.getInstance(tryPreAccept.replicaId(), tryPreAccept.instanceId());
        
        if (instance != null && instance.getBallot().greaterThan(tryPreAccept.ballot())) {
            network.send(new TryPreAcceptReply(cluster.getCurrReplicaId(), 
                                               tryPreAccept.leaderId(), 
                                               tryPreAccept.meta(), 
                                               cluster.getCurrReplicaId(), 
                                               tryPreAccept.replicaId(), 
                                               tryPreAccept.instanceId(), 
                                               false, 
                                               instance.getBallot(), 
                                               tryPreAccept.replicaId(), 
                                               tryPreAccept.instanceId(), 
                                               instance.getStatus()));
        }
        
        var conflicts = findPreAcceptConflicts(tryPreAccept.command(), 
                                               tryPreAccept.replicaId(), 
                                               tryPreAccept.instanceId(), 
                                               tryPreAccept.attributes());
        
        if (conflicts.hasCoflicts()) {
            var reply = new TryPreAcceptReply(cluster.getCurrReplicaId(), 
                                              tryPreAccept.leaderId(), 
                                              tryPreAccept.meta(), 
                                              cluster.getCurrReplicaId(), 
                                              tryPreAccept.replicaId(), 
                                              tryPreAccept.instanceId(), 
                                              false, 
                                              instance.getBallot(), 
                                              conflicts.replicaId(), 
                                              conflicts.instanceId(), 
                                              instanceSpace.getInstance(conflicts.replicaId(), 
                                                                        conflicts.instanceId()).getStatus());
            network.send(reply);
            LOGGER.debug("Exit - send failed reply={}", reply);
        } else {
            // can pre-accept
            instanceSpace.adjustCrtInstanceId(tryPreAccept.replicaId(), tryPreAccept.instanceId());
            
            if (instance != null) {
                instance.setCommand(tryPreAccept.command());
                instance.setAttributes(tryPreAccept.attributes());
                instance.setStatus(InstanceStatus.PREACCEPTED);
                instance.setBallot(tryPreAccept.ballot());
            } else {
                instance = instanceSpace.registerNewInstance(tryPreAccept.replicaId(), 
                                                             tryPreAccept.instanceId(), 
                                                             tryPreAccept.command(), 
                                                             tryPreAccept.ballot(), 
                                                             InstanceStatus.PREACCEPTED, 
                                                             tryPreAccept.attributes());
            }
            var reply = new TryPreAcceptReply(cluster.getCurrReplicaId(), 
                                              tryPreAccept.leaderId(), 
                                              tryPreAccept.meta(), 
                                              cluster.getCurrReplicaId(), 
                                              tryPreAccept.replicaId(), 
                                              tryPreAccept.instanceId(), 
                                              true, 
                                              instance.getBallot(), 
                                              null, 
                                              0, 
                                              InstanceStatus.NONE);
            network.send(reply);
            LOGGER.debug("Exit - send ok reply={}", reply);
        }
    }

}
