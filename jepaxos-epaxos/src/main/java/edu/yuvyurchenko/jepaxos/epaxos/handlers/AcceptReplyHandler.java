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

public class AcceptReplyHandler extends AbstractHandler<AcceptReply> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AcceptReplyHandler.class);

    public AcceptReplyHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    public void handle(AcceptReply acceptReply) {
        LOGGER.debug("Receive - acceptReply={}", acceptReply);
        var instance = instanceSpace.getInstance(acceptReply.replicaId(), acceptReply.instanceId());

        if (instance.getStatus() != InstanceStatus.ACCEPTED) {
            LOGGER.debug("Exit - late reply");
            return;
        }

        if (instance.getBallot().notEquals(acceptReply.ballot())) {
            LOGGER.debug("Exit - another voiting round started");
            return;
        }

        var lb = instance.leaderBookkeeping();
        if (!acceptReply.ok()) {
            lb.incNacks();
            if (acceptReply.ballot().greaterThan(lb.getMaxRecvBallot())) {
                lb.setMaxRecvBallot(acceptReply.ballot());
            }
            LOGGER.debug("Exit - rejected accept");
            return;
        }

        lb.incAcceptOKs();

        if (lb.getAcceptOKs() + 1 > cluster.getAllReplicaIds().size()/2) {
            instance.setStatus(InstanceStatus.COMMITTED);
            instanceSpace.updateCommitted(acceptReply.replicaId());

            var replyData = instance.replyData();
            
            if (replyData != null && replyData.type() == ReplyType.REQUEST) {
                var reply = ExternalMessage.okRequestReply(cluster.getCurrReplicaId(), 
                                                           replyData.clientId(), 
                                                           replyData.meta());
                network.send(reply);
                LOGGER.debug("Response to client = {}", reply);    
            } // else we need to wait the command execution

            broadcast(extReplicaId -> new Commit(cluster.getCurrReplicaId(), 
                                                 extReplicaId, 
                                                 acceptReply.meta(), 
                                                 cluster.getCurrReplicaId(), 
                                                 acceptReply.replicaId(), 
                                                 acceptReply.instanceId(), 
                                                 instance.getCommand(), 
                                                 instance.getAttributes()));
            LOGGER.debug("Broadcast commit");
        }
        LOGGER.debug("Exit - handled");
    }

}
