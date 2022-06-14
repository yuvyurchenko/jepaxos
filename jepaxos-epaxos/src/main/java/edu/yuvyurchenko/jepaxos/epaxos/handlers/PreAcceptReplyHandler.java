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

public class PreAcceptReplyHandler extends AbstractHandler<PreAcceptReply> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreAcceptReplyHandler.class);
    
    public PreAcceptReplyHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    public void handle(PreAcceptReply preAcceptReply) {
        LOGGER.debug("Receive - preAcceptReply={}", preAcceptReply);
        var instance = instanceSpace.getInstance(preAcceptReply.replicaId(), preAcceptReply.instanceId());

        if (instance.getStatus() != InstanceStatus.PREACCEPTED) {
            // we've moved on, this is a delayed reply
            LOGGER.debug("Exit - delayed reply");
            return;
        }

        if (instance.getBallot().notEquals(preAcceptReply.ballot())) {
            LOGGER.debug("Instance Ballot = {} VS Reply Ballot = {}", instance.getBallot(), preAcceptReply.ballot());
            LOGGER.debug("Exit - delayed reply after the new voting round started");
            return;
        }

        var lb = instance.leaderBookkeeping();

        if (!preAcceptReply.ok()) {
            // there is probably another active leader
            lb.incNacks();
            if (preAcceptReply.ballot().greaterThan(lb.getMaxRecvBallot())) {
                lb.setMaxRecvBallot(preAcceptReply.ballot());
            }
            LOGGER.debug("Exit - got rejected, stop the flow");
            return;
        }

        lb.incPreAcceptOKs();
        var mergeResult = instanceSpace.mergeAttributes(instance.getAttributes(), preAcceptReply.attributes());
        if (!mergeResult.equal()) {
            instance.setAttributes(mergeResult.attributes());
            mergeResult.attributes().deps().forEach(instanceSpace::adjustCrtInstanceId);
        }
        if (cluster.getAllReplicaIds().size() <= 3 || lb.getPreAcceptOKs() > 1) {
            lb.setAllEqual(lb.isAllEqual() && mergeResult.equal());
        }

        var allCommitted = instanceSpace.updateCommittedDeps(instance, preAcceptReply.committedDeps());
        LOGGER.debug("allCommitted={}", allCommitted);

        if (hasQuorum(lb.getPreAcceptOKs())) {
            if (lb.isAllEqual() && allCommitted && instance.isInitialBallot()) {
                instance.setStatus(InstanceStatus.COMMITTED);
                instanceSpace.updateCommitted(preAcceptReply.replicaId());

                var replyData = instance.replyData();
                
                if (replyData.type() == ReplyType.REQUEST) {
                    var replyToClient = ExternalMessage.okRequestReply(cluster.getCurrReplicaId(), 
                                                                       replyData.clientId(), 
                                                                       replyData.meta());
                    network.send(replyToClient);
                    LOGGER.debug("Exit - reply to client = {}", replyToClient);
                } // else we need to wait the command execution
                
                broadcast(extReplicaId -> new Commit(cluster.getCurrReplicaId(), 
                                                     extReplicaId, 
                                                     preAcceptReply.meta(), 
                                                     cluster.getCurrReplicaId(), 
                                                     preAcceptReply.replicaId(), 
                                                     preAcceptReply.instanceId(), 
                                                     instance.getCommand(), 
                                                     instance.getAttributes()));
                LOGGER.debug("Exit - broadcast Commit");
            } else {
                instance.setStatus(InstanceStatus.ACCEPTED);
                broadcast(extReplicaId -> new Accept(cluster.getCurrReplicaId(), 
                                                     extReplicaId, 
                                                     preAcceptReply.meta(), 
                                                     cluster.getCurrReplicaId(), 
                                                     preAcceptReply.replicaId(), 
                                                     preAcceptReply.instanceId(), 
                                                     instance.getBallot(), 
                                                     instance.getCommand(), 
                                                     instance.getAttributes()));
                LOGGER.debug("Exit - broadcast Accept");
            }
        }
        
    }
}
