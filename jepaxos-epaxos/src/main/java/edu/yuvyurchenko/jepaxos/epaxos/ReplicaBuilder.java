package edu.yuvyurchenko.jepaxos.epaxos;

import java.util.HashMap;
import java.util.Map;

import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.CommandOperation;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Storage;

public final class ReplicaBuilder {
    private Cluster cluster;
    private Storage storage;
    private Network network;
    private Map<String, CommandOperation> operationRegistry = new HashMap<>();

    public ReplicaBuilder withCluster(Cluster cluster) {
        this.cluster = cluster;
        return this;
    }

    public ReplicaBuilder withStorage(Storage storage) {
        this.storage = storage;
        return this;
    }

    public ReplicaBuilder withNetwork(Network network) {
        this.network = network;
        return this;
    }

    public ReplicaBuilder withCommandOparetion(String operationId, CommandOperation operation) {
        operationRegistry.put(operationId, operation);
        return this;
    }

    public Replica build() {
        return new Replica(cluster, 
                           storage, 
                           network, 
                           new CommandOperationRegistry(Map.copyOf(operationRegistry)));
    }
}
