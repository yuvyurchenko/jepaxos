package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyType;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class AcceptReplyHandler extends AbstractHandler<AcceptReply> {
    
    public AcceptReplyHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    public void handle(AcceptReply acceptReply) {
        var instance = instanceSpace.getInstance(acceptReply.replicaId(), acceptReply.instanceId());

        if (instance.getStatus() != InstanceStatus.ACCEPTED) {
            return;
        }

        if (instance.getBallot() != acceptReply.ballot()) {
            return;
        }

        var lb = instance.leaderBookkeeping();
        if (!acceptReply.ok()) {
            lb.incNacks();
            if (acceptReply.ballot().greaterThan(lb.getMaxRecvBallot())) {
                lb.setMaxRecvBallot(acceptReply.ballot());
            }
            return;
        }

        lb.incAcceptOKs();

        if (lb.getAcceptOKs() + 1 > cluster.getAllReplicaIds().size()/2) {
            instance.setStatus(InstanceStatus.COMMITTED);
            instanceSpace.updateCommitted(acceptReply.replicaId());

            var replyData = lb.getReplyData();
            
            if (replyData.type() == ReplyType.REQUEST) {
                network.send(
                    ExternalMessage.okRequestReply(cluster.getCurrReplicaId(), 
                                                   replyData.clientId(), 
                                                   replyData.meta()));    
            } // else we need to wait the command execution

            broadcast(extReplicaId -> new Commit(cluster.getCurrReplicaId(), 
                                                 extReplicaId, 
                                                 acceptReply.meta(), 
                                                 cluster.getCurrReplicaId(), 
                                                 acceptReply.replicaId(), 
                                                 acceptReply.instanceId(), 
                                                 instance.getCommand(), 
                                                 instance.getAttributes()));
        }
    }

}
