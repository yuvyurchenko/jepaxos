package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.Ballot;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class CommitHandler extends AbstractHandler<Commit> {
    
    public CommitHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    public void handle(Commit commit) {
        var instance = instanceSpace.getInstance(commit.replicaId(), commit.instanceId());

        if (commit.attributes().seq() >= instanceSpace.getMaxSeq()) {
            instanceSpace.setMaxSeq(commit.attributes().seq() + 1);
        }

        instanceSpace.adjustCrtInstanceId(commit.replicaId(), commit.instanceId());

        if (instance != null) {
            instance.setAttributes(commit.attributes());
            instance.setStatus(InstanceStatus.COMMITTED);
        } else {
            instanceSpace.registerNewInstance(commit.replicaId(), 
                                              commit.instanceId(), 
                                              commit.command(), 
                                              new Ballot(0, commit.replicaId()), 
                                              InstanceStatus.COMMITTED, 
                                              commit.attributes());
            instanceSpace.updateConflicts(commit.command(), 
                                          commit.replicaId(), 
                                          commit.instanceId(), 
                                          commit.attributes().seq());
        }

        instanceSpace.updateCommitted(commit.replicaId());
    }

}
