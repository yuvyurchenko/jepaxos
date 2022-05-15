package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import edu.yuvyurchenko.jepaxos.epaxos.CommandOperationRegistry;
import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyData;
import edu.yuvyurchenko.jepaxos.epaxos.model.Attributes;
import edu.yuvyurchenko.jepaxos.epaxos.model.Ballot;
import edu.yuvyurchenko.jepaxos.epaxos.model.Command;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public abstract class AbstractRecoveryHandler<M extends NetworkMessage> extends AbstractHandler<M> {
    
    private final CommandOperationRegistry operationRegistry;

    protected AbstractRecoveryHandler(Cluster cluster, 
                                      Network network,
                                      InstanceSpace instanceSpace,
                                      CommandOperationRegistry operationRegistry) {
        super(cluster, network, instanceSpace);
        this.operationRegistry = operationRegistry;
    }

    protected void restartPhase1(String replicaId, int instanceId, Ballot ballot, Command command, ReplyData replyData) {
        var instance = instanceSpace.resetCommandLeaderInstance(replicaId,
                                                                instanceId,
                                                                ballot,
                                                                command, 
                                                                replyData);
        instanceSpace.updateConflicts(instance);

        broadcast(extReplicaId -> new PreAccept(cluster.getCurrReplicaId(), 
                                                extReplicaId,
                                                replyData.meta(),
                                                instance.getReplicaId(), 
                                                instance.getReplicaId(), 
                                                instance.getInstanceId(), 
                                                instance.getBallot(), 
                                                instance.getCommand(), 
                                                instance.getAttributes()));
    }

    protected static record PreAcceptConflicts(boolean hasCoflicts, String replicaId, int instanceId) {}

    protected PreAcceptConflicts findPreAcceptConflicts(Command command, String replicaId, int instanceId, Attributes attributes) {
        var instance = instanceSpace.getInstance(replicaId, instanceId);

        if (instance != null && command != null) {
            if (instance.getStatus() == InstanceStatus.ACCEPTED || instance.getStatus() == InstanceStatus.COMMITTED 
                                                                || instance.getStatus() == InstanceStatus.EXECUTED) {
                // already ACCEPTED or COMMITTED
			    // we consider this a conflict because we shouldn't regress to PRE-ACCEPTED
                return new PreAcceptConflicts(true, replicaId, instanceId);
            }
            if (instance.getAttributes().equals(attributes)) {
                // already PRE-ACCEPTED, no point looking for conflicts again
                return new PreAcceptConflicts(false, replicaId, instanceId);
            }
        }

        for (var rId : cluster.getAllReplicaIds()) {
            for (var i : instanceSpace.notExecutedInstances(rId)) {
                if (i == null || i.getCommand() == null) {
                    continue;
                }
                if(replicaId.equals(i.getReplicaId()) && instanceId == i.getInstanceId()) {
                    // no point checking past instance in replica's row, since replica would have
                    // set the dependencies correctly for anything started after instance
                    break;
                }
                if (i.getInstanceId() == attributes.deps().getOrDefault(rId, -1)) {
                    //the instance cannot be a dependency for itself
				    continue;
                }
                if (i.getAttributes().deps().get(replicaId) >= instanceId) {
                    // instance q.i depends on instance replica.instance, it is not a conflict
				    continue;
                }
                if (inConflict(i.getCommand(), command)) {
                    int depInstId = attributes.deps().getOrDefault(rId, -1);
                    if (i.getInstanceId() > depInstId 
                        || (i.getInstanceId() < depInstId && i.getAttributes().seq() >= attributes.seq() 
                                                          && (!rId.equals(replicaId) || i.getStatus() == InstanceStatus.PREACCEPTED_EQ 
                                                                                     || i.getStatus() == InstanceStatus.ACCEPTED 
                                                                                     || i.getStatus() == InstanceStatus.COMMITTED 
                                                                                     || i.getStatus() == InstanceStatus.EXECUTED))) {
                        // this is a conflict
					    return new PreAcceptConflicts(true, rId, i.getInstanceId());
                    }
                }
                
            }
        }
        return new PreAcceptConflicts(false, "", -1);
    }

    private boolean inConflict(Command c1, Command c2) {
        if (c1 == null || c2 == null) {
            return false;
        }
        var o1 = operationRegistry.getOperation(c1.operation());
        var o2 = operationRegistry.getOperation(c2.operation());
        
        return c1.key().equals(c2.key()) && (!o1.isReadOnly() || !o2.isReadOnly());
    }

}
