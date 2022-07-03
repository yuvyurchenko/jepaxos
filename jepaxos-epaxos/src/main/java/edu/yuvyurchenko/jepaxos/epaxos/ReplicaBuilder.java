package edu.yuvyurchenko.jepaxos.epaxos;

import java.util.HashMap;
import java.util.Map;

import edu.yuvyurchenko.jepaxos.epaxos.model.Command;
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
    private long commitGracePeriodMs = 7000L;
    private long commitGracePeriodShiftMs = 1000L;
    private long waitCommitPeriodMs = 500L;
    private int maxWaitCommitTries = 5;

    /**
     * sets up the cluster implementation
     * @param cluster
     * @return
     */
    public ReplicaBuilder withCluster(Cluster cluster) {
        this.cluster = cluster;
        return this;
    }

    /**
     * sets up the storage implementation
     * @param storage
     * @return
     */
    public ReplicaBuilder withStorage(Storage storage) {
        this.storage = storage;
        return this;
    }

    /**
     * sets up the network implementation
     * @param network
     * @return
     */
    public ReplicaBuilder withNetwork(Network network) {
        this.network = network;
        return this;
    }

    /**
     * binds the operation id used in the {@link Command#operation()} with the command operation implementation
     * @param operationId
     * @param operation
     * @return
     */
    public ReplicaBuilder withCommandOparetion(String operationId, CommandOperation operation) {
        operationRegistry.put(operationId, operation);
        return this;
    }

    /**
     * returns the default executing driver configuration builder
     * @return
     */
    public ExecutionDriverBuilder withExecutingDriver() {
        return new ExecutionDriverBuilder();
    }

    /**
     * sets up the custom executing driver implemetation
     * @param executingDriver
     * @return
     */
    public ReplicaBuilder withCustomExecutingDriver(ExecutingDriver executingDriver) {
        this.executingDriver = executingDriver;
        return this;
    }

    /**
     * configures time to wait for command commit. if timesout recovery process starts  
     * @param commitGracePeriodMs
     * @return
     */
    public ReplicaBuilder withCommitGracePeriodMs(long commitGracePeriodMs) {
        this.commitGracePeriodMs = commitGracePeriodMs;
        return this;
    }

    /**
     * configures the lag between different replicas to start recovery 
     * to reduce the probability of Propose life-locks
     * @param commitGracePeriodShiftMs
     * @return
     */
    public ReplicaBuilder withCommitGracePeriodShiftMs(long commitGracePeriodShiftMs) {
        this.commitGracePeriodShiftMs = commitGracePeriodShiftMs;
        return this;
    }

    /**
     * configures in place commit status re-check timeout
     * @param waitCommitPeriodMs
     * @return
     */
    public ReplicaBuilder withWaitCommitPeriodMs(long waitCommitPeriodMs) {
        this.waitCommitPeriodMs = waitCommitPeriodMs;
        return this;
    }

    /**
     * number of in-place commit status re-checks before forbidding the current command execution progress  
     * @param maxWaitCommitTries
     * @return
     */
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
                           commitGracePeriodShiftMs,
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
