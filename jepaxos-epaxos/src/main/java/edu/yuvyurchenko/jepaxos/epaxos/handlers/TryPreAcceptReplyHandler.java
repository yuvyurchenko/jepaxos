package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import edu.yuvyurchenko.jepaxos.epaxos.CommandOperationRegistry;
import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class TryPreAcceptReplyHandler extends AbstractRecoveryHandler<TryPreAcceptReply> {
    
    public TryPreAcceptReplyHandler(Cluster cluster, 
                                    Network network, 
                                    InstanceSpace instanceSpace,
                                    CommandOperationRegistry operationRegistry) {
        super(cluster, network, instanceSpace, operationRegistry);
    }

    public void handle(TryPreAcceptReply tryPreAcceptReply) {
        var instance = instanceSpace.getInstance(tryPreAcceptReply.replicaId(), tryPreAcceptReply.instanceId());

        if (instance == null || instance.leaderBookkeeping() == null 
                             || !instance.leaderBookkeeping().isTryingToPreAccept() 
                             || instance.leaderBookkeeping().recovertInstance() == null) {
            return;
        }

        var lb = instance.leaderBookkeeping();
        var ri = lb.recovertInstance();

        if (tryPreAcceptReply.ok()) {
            lb.incPreAcceptOKs();
            lb.incTryPreAcceptOKs();
            if (hasQuorum(lb.getPreAcceptOKs())) {
                instance.setCommand(ri.getCommand());
                instance.setAttributes(ri.getAttributes());
                instance.setStatus(InstanceStatus.ACCEPTED);
                lb.setTryingToPreAccept(false);
                lb.setAcceptOKs(0);
                broadcast(extReplicaId -> new Accept(cluster.getCurrReplicaId(), 
                                                     extReplicaId, 
                                                     tryPreAcceptReply.meta(), 
                                                     cluster.getCurrReplicaId(), 
                                                     tryPreAcceptReply.replicaId(), 
                                                     tryPreAcceptReply.instanceId(), 
                                                     instance.getBallot(), 
                                                     instance.getCommand(), 
                                                     instance.getAttributes()));
                return;
            }
        } else {
            lb.incNacks();
            if (tryPreAcceptReply.ballot().greaterThan(instance.getBallot())) {
                return;
            }
            lb.incTryPreAcceptOKs();
            if (tryPreAcceptReply.replicaId().equals(tryPreAcceptReply.conflictReplicaId()) 
                && tryPreAcceptReply.instanceId() == tryPreAcceptReply.conflictInstanceId()) {
                lb.setTryingToPreAccept(false);
                return;
            }
            lb.updatePossibleQuorum(tryPreAcceptReply.acceptorId(), false);
            lb.updatePossibleQuorum(tryPreAcceptReply.conflictReplicaId(), false);
            var notInQuorum = lb.countPossibleQuorum(false);
            if (tryPreAcceptReply.conflictStatus() == InstanceStatus.COMMITTED 
                || tryPreAcceptReply.conflictStatus() == InstanceStatus.EXECUTED 
                || notInQuorum > cluster.getAllReplicaIds().size() / 2) {
                //abandon recovery, restart from phase 1
                lb.setTryingToPreAccept(false);
                restartPhase1(tryPreAcceptReply.replicaId(), 
                              tryPreAcceptReply.instanceId(), 
                              instance.getBallot(), 
                              ri.getCommand(), 
                              lb.getReplyData());
            }
            if (notInQuorum == cluster.getAllReplicaIds().size() / 2) {
                //this is to prevent defer cycles
                var check = instanceSpace.deferredByInstance(tryPreAcceptReply.replicaId(), tryPreAcceptReply.instanceId());
                if (check.deffer() && lb.isPossibleQuorum(check.key().replicaId())) {
                    //an instance whose leader must have been in this instance's quorum has been deferred for this instance => contradiction
                    //abandon recovery, restart from phase 1
                    lb.setTryingToPreAccept(false);
                    restartPhase1(tryPreAcceptReply.replicaId(), 
                                  tryPreAcceptReply.instanceId(), 
                                  instance.getBallot(), 
                                  ri.getCommand(), 
                                  lb.getReplyData());    
                }
            }
            if (hasQuorum(lb.getTryPreAcceptOKs())) {
                //defer recovery and update deferred information
                instanceSpace.updateDeferred(tryPreAcceptReply.replicaId(), 
                                             tryPreAcceptReply.instanceId(), 
                                             tryPreAcceptReply.conflictReplicaId(), 
                                             tryPreAcceptReply.conflictInstanceId());
                lb.setTryingToPreAccept(false);
            }
        }
    }

}
