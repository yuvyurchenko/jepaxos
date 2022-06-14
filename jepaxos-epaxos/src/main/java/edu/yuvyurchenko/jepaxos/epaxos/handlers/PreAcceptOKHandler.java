package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyType;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class PreAcceptOKHandler extends AbstractHandler<PreAcceptOK> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(PreAcceptOKHandler.class);

    public PreAcceptOKHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    public void handle(PreAcceptOK preAcceptOK) {
        LOGGER.debug("Receive - preAcceptOK={}", preAcceptOK);
        var instance = instanceSpace.getInstance(cluster.getCurrReplicaId(), preAcceptOK.instanceId());

        if (instance.getStatus() != InstanceStatus.PREACCEPTED) {
            // we've moved on, this is a delayed reply
            LOGGER.debug("Exit - delayed reply");
            return;
        }

        if (instance.isNotInitialBallot()) {
            LOGGER.debug("Exit - delayed reply after the new voting round started");
            return;
        }

        var lb = instance.leaderBookkeeping();
        lb.incPreAcceptOKs();
        var allCommitted = instanceSpace.updateCommittedDeps(instance, lb.getOriginalDeps());

        if (hasQuorum(lb.getPreAcceptOKs())) {
            if (lb.isAllEqual() && allCommitted && instance.isInitialBallot()) {
                instance.setStatus(InstanceStatus.COMMITTED);
                instanceSpace.updateCommitted(instance.getReplicaId());

                var replyData = instance.replyData();
                
                if (replyData.type() == ReplyType.REQUEST) {
                    network.send(
                        ExternalMessage.okRequestReply(cluster.getCurrReplicaId(), 
                                                       replyData.clientId(), 
                                                       replyData.meta()));    
                } // else we need to wait the command execution
                broadcast(extReplicaId -> new Commit(cluster.getCurrReplicaId(), 
                                                     extReplicaId, 
                                                     preAcceptOK.meta(), 
                                                     cluster.getCurrReplicaId(), 
                                                     cluster.getCurrReplicaId(), 
                                                     preAcceptOK.instanceId(), 
                                                     instance.getCommand(), 
                                                     instance.getAttributes()));
                LOGGER.debug("Exit - broadcast Commit");
            } else {
                instance.setStatus(InstanceStatus.ACCEPTED);
                broadcast(extReplicaId -> new Accept(cluster.getCurrReplicaId(), 
                                                     extReplicaId, 
                                                     preAcceptOK.meta(), 
                                                     cluster.getCurrReplicaId(), 
                                                     cluster.getCurrReplicaId(), 
                                                     preAcceptOK.instanceId(), 
                                                     instance.getBallot(), 
                                                     instance.getCommand(), 
                                                     instance.getAttributes()));
                LOGGER.debug("Exit - broadcast Accept");
            }
        }

    }
}
