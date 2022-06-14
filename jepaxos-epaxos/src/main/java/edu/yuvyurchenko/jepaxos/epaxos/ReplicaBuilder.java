package edu.yuvyurchenko.jepaxos.epaxos;

import java.util.HashMap;
import java.util.Map;

import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.CommandOperation;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.ExecutingDriver;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Storage;

public final class ReplicaBuilder {
    private Cluster cluster;
    private Storage storage;
    private Network network;
    private ExecutingDriver executingDriver; 
    private Map<String, CommandOperation> operationRegistry = new HashMap<>();
    private long commitGracePeriodMs = 10000L;
    private long waitCommitPeriodMs = 500L;
    private int maxWaitCommitTries = 5;

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

    public ExecutionDriverBuilder withExecutingDriver() {
        return new ExecutionDriverBuilder();
    }

    public ReplicaBuilder withCustomExecutingDriver(ExecutingDriver executingDriver) {
        this.executingDriver = executingDriver;
        return this;
    }

    public ReplicaBuilder withCommitGracePeriodMs(long commitGracePeriodMs) {
        this.commitGracePeriodMs = commitGracePeriodMs;
        return this;
    }

    public ReplicaBuilder withWaitCommitPeriodMs(long waitCommitPeriodMs) {
        this.waitCommitPeriodMs = waitCommitPeriodMs;
        return this;
    }

    public ReplicaBuilder withMaxWaitCommitTries(int maxWaitCommitTries) {
        this.maxWaitCommitTries = maxWaitCommitTries;
        return this;
    }

    public Replica build() {
        return new Replica(cluster, 
                           storage, 
                           network,
                           executingDriver, 
                           new CommandOperationRegistry(Map.copyOf(operationRegistry)),
                           commitGracePeriodMs,
                           waitCommitPeriodMs,
                           maxWaitCommitTries);
    }

    public class ExecutionDriverBuilder {
        private long initialDelay = 1000;
        private long period = 1000;

        private ExecutionDriverBuilder() {
            // hide default constructor
        }

        public ExecutionDriverBuilder setInitialDelay(long initialDelay) {
            this.initialDelay = initialDelay;
            return this;
        }

        public ExecutionDriverBuilder setPeriod(long period) {
            this.period = period;
            return this;
        }

        ReplicaBuilder build() {
            ReplicaBuilder.this.executingDriver = new DefaultExecutingDriver(initialDelay, period);
            return ReplicaBuilder.this;
        }
    }
}
