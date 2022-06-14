package edu.yuvyurchenko.jepaxos.epaxos;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import edu.yuvyurchenko.jepaxos.epaxos.handlers.HandlerOperation;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.ExecutingDriver;

public class DefaultExecutingDriver implements ExecutingDriver {

    private final long initialDelay;
    private final long period;

    private final ExecutorService protocolExec = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService commandExec = Executors.newSingleThreadScheduledExecutor();

    DefaultExecutingDriver(long initialDelay,
                           long period) {
        this.initialDelay = initialDelay;
        this.period = period;
    }

    @Override
    public <R extends NetworkMessage> void enqueue(HandlerOperation<R> operation) {
        protocolExec.execute(operation);
    }

    @Override
    public void enqueue(Runnable operation) {
        protocolExec.execute(operation);
    }

    @Override
    public void schedule(Runnable operation) {
        commandExec.scheduleAtFixedRate(operation, initialDelay, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public void sleep(long timeMs) {
        try {
            Thread.sleep(timeMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void shutdown() {
        protocolExec.shutdown();
        commandExec.shutdown();
    }
    
}
