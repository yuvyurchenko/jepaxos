package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyType;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class PreAcceptReplyHandler extends AbstractHandler<PreAcceptReply> {
    
    public PreAcceptReplyHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    public void handle(PreAcceptReply preAcceptReply) {
        var instance = instanceSpace.getInstance(preAcceptReply.replicaId(), preAcceptReply.instanceId());

        if (instance.getStatus() != InstanceStatus.PREACCEPTED) {
            // we've moved on, this is a delayed reply
            return;
        }

        if (instance.getBallot() != preAcceptReply.ballot()) {
            return;
        }

        var lb = instance.leaderBookkeeping();

        if (!preAcceptReply.ok()) {
            // there is probably another active leader
            lb.incNacks();
            if (preAcceptReply.ballot().greaterThan(lb.getMaxRecvBallot())) {
                lb.setMaxRecvBallot(preAcceptReply.ballot());
            }
            return;
        }

        lb.incPreAcceptOKs();
        var mergeResult =  instanceSpace.mergeAttributes(instance.getAttributes(), preAcceptReply.attributes());
        if (cluster.getAllReplicaIds().size() <= 3 || lb.getPreAcceptOKs() > 1) {
            lb.setAllEqual(lb.isAllEqual() && mergeResult.equal());
        }

        var allCommitted = instanceSpace.updateCommittedDeps(instance, preAcceptReply.committedDeps());

        if (hasQuorum(lb.getPreAcceptOKs())) {
            if (lb.isAllEqual() && allCommitted && instance.isInitialBallot()) {
                instance.setStatus(InstanceStatus.COMMITTED);
                instanceSpace.updateCommitted(preAcceptReply.replicaId());

                var replyData = lb.getReplyData();
                
                if (replyData.type() == ReplyType.REQUEST) {
                    network.send(
                        ExternalMessage.okRequestReply(cluster.getCurrReplicaId(), 
                                                       replyData.clientId(), 
                                                       replyData.meta()));    
                } // else we need to wait the command execution
                
                broadcast(extReplicaId -> new Commit(cluster.getCurrReplicaId(), 
                                                     extReplicaId, 
                                                     preAcceptReply.meta(), 
                                                     cluster.getCurrReplicaId(), 
                                                     preAcceptReply.replicaId(), 
                                                     preAcceptReply.instanceId(), 
                                                     instance.getCommand(), 
                                                     instance.getAttributes()));
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
            }
        }
        
    }
}
