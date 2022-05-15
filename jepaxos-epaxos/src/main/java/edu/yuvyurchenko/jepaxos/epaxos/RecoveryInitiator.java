package edu.yuvyurchenko.jepaxos.epaxos;

import java.util.Map;
import java.util.function.Function;

import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.Prepare;
import edu.yuvyurchenko.jepaxos.epaxos.messages.MessageMetadata;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;
import edu.yuvyurchenko.jepaxos.epaxos.model.Attributes;
import edu.yuvyurchenko.jepaxos.epaxos.model.Ballot;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class RecoveryInitiator {
    
    private final Cluster cluster;
    private final Network network;
    private final InstanceSpace instanceSpace;

    RecoveryInitiator(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        this.cluster = cluster;
        this.network = network;
        this.instanceSpace = instanceSpace;
    }

    public void startRecoveryForInstance(String replicaId, int instanceId) {
        var instance = instanceSpace.getInstance(replicaId, instanceId);

        if (instance == null) {
            instance = instanceSpace.registerNewInstance(replicaId, 
                                                         instanceId, 
                                                         null, 
                                                         new Ballot(0, replicaId), 
                                                         InstanceStatus.NONE, 
                                                         new Attributes(0, 
                                                                        Map.of()));
        }
        
        instance.switchToRecoveryLeaderBookkeeping();

        var lb = instance.leaderBookkeeping();
        if (instance.getStatus() == InstanceStatus.ACCEPTED) {
            lb.initRecoveryInstance(instance.getCommand(), 
                                    instance.getStatus(), 
                                    instance.getAttributes(), 
                                    0, 
                                    false);
            lb.setMaxRecvBallot(instance.getBallot());
        } else if (instance.getStatus() != InstanceStatus.NONE) {
            lb.initRecoveryInstance(instance.getCommand(), 
                                    instance.getStatus(), 
                                    instance.getAttributes(), 
                                    1, 
                                    cluster.getCurrReplicaId() == replicaId);           
        }

        instance.setBallot(makeBallotLargerThan(instance.getBallot()));

        var ballot = instance.getBallot();
        broadcast(extReplicaId -> new Prepare(cluster.getCurrReplicaId(), 
                                              extReplicaId, 
                                              new MessageMetadata(), 
                                              cluster.getCurrReplicaId(), 
                                              replicaId, 
                                              instanceId, 
                                              ballot));
    }

    private Ballot makeBallotLargerThan(Ballot ballot) {
        return new Ballot(ballot.number() + 1, cluster.getCurrReplicaId());
    }

    private <T extends NetworkMessage> void broadcast(Function<String, T> msgFactory) {
        cluster.getAllReplicaIds().stream()
            .filter(id -> !id.equals(cluster.getCurrReplicaId()))
            .map(msgFactory)
            .forEach(network::send);
    }

}
