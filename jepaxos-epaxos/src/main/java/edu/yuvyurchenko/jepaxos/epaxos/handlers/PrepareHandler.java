package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import java.util.Map;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.Attributes;
import edu.yuvyurchenko.jepaxos.epaxos.model.Ballot;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class PrepareHandler extends AbstractHandler<Prepare> {
    
    public PrepareHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    public void handle(Prepare prepare) {
        var instance = instanceSpace.getInstance(prepare.replicaId(), prepare.instanceId());
        
        PrepareReply prepareReply;
        if (instance == null) {
            instance = instanceSpace.registerNewInstance(prepare.replicaId(), 
                                                         prepare.instanceId(), 
                                                         null, 
                                                         prepare.ballot(), 
                                                         InstanceStatus.NONE, 
                                                         new Attributes(0, Map.of()));
            prepareReply = new PrepareReply(cluster.getCurrReplicaId(), 
                                            prepare.leaderId(), 
                                            prepare.meta(), 
                                            cluster.getCurrReplicaId(), 
                                            prepare.replicaId(), 
                                            prepare.instanceId(), 
                                            true, 
                                            new Ballot(-1, cluster.getCurrReplicaId()), 
                                            InstanceStatus.NONE, 
                                            null, 
                                            new Attributes(-1, Map.of()));
        } else {
            boolean ok = true;
            if (prepare.ballot().lessThan(instance.getBallot())) {
                ok = false;
            } else {
                instance.setBallot(prepare.ballot());
            }
            prepareReply = new PrepareReply(cluster.getCurrReplicaId(), 
                                            prepare.leaderId(), 
                                            prepare.meta(), 
                                            cluster.getCurrReplicaId(), 
                                            prepare.replicaId(), 
                                            prepare.instanceId(), 
                                            ok, 
                                            instance.getBallot(), 
                                            instance.getStatus(), 
                                            instance.getCommand(), 
                                            instance.getAttributes());
        }
        network.send(prepareReply);
    }

}
