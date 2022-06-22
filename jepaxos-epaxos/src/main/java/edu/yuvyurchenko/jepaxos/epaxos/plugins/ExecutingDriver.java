package edu.yuvyurchenko.jepaxos.epaxos.plugins;

import edu.yuvyurchenko.jepaxos.epaxos.handlers.HandlerOperation;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;

/**
 * API for underlying thread management implementation.
 * JEPaxos has an assumption that underlying implementation uses at most two sequences of operations:
 * - one for commit protocol, used mostly for message handlers execution.
 * - another for execution protocol
 */
public interface ExecutingDriver {
    /**
     * Submits the operation to the commit protocol thread. Method used for message handlers.
     * @param <R>
     * @param operation
     */
    <R extends NetworkMessage> void enqueue(HandlerOperation<R> operation);
    /**
     * Submits the operation to the commit protocol thread. Used by the recovery process
     * @param operation
     */
    void enqueue(Runnable operation);
    /**
     * Schedules the execution protocol operation.
     * @param operation
     */
    void schedule(Runnable operation);
    /**
     * Delays the current thread execution for the given time.
     * @param timeMs
     */
    void sleep(long timeMs);
    /**
     * Sends signal that the replica is going to shutdown so underlying excution service should do the same
     */
    void shutdown();
}
