package edu.yuvyurchenko.jepaxos.epaxos;

import static edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus.COMMITTED;
import static edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus.EXECUTED;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace.Cell;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage;
import edu.yuvyurchenko.jepaxos.epaxos.model.Instance;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.CommandOperation;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.ExecutingDriver;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Storage;

public class CommandProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandProcessor.class);

    private final Cluster cluster;
    private final InstanceSpace instanceSpace;
    private final Storage storage;
    private final Network network;
    private final RecoveryInitiator recoveryInitiator;
    private final CommandOperationRegistry operationRegistry;
    private final ExecutingDriver driver;

    private final Map<String, ProblemInstance> problemInstances;

    private final long commitGracePeriodMs;
    private final long waitCommitPeriodMs;
    private final int maxWaitCommitTries;

    CommandProcessor(Cluster cluster,
                     InstanceSpace instanceSpace,
                     Storage storage,
                     Network network,
                     CommandOperationRegistry operationRegistry,
                     ExecutingDriver driver,
                     long commitGracePeriodMs,
                     long waitCommitPeriodMs,
                     int maxWaitCommitTries) {
        this.cluster = cluster;
        this.instanceSpace = instanceSpace;
        this.storage = storage;
        this.network = network;
        this.operationRegistry = operationRegistry;
        this.driver = driver;
        this.commitGracePeriodMs = commitGracePeriodMs;
        this.waitCommitPeriodMs = waitCommitPeriodMs;
        this.maxWaitCommitTries = maxWaitCommitTries;
        
        this.problemInstances = new HashMap<>();
        this.recoveryInitiator = new RecoveryInitiator(cluster, 
                                                       network, 
                                                       instanceSpace);
    }

    public boolean executeCommands() {
        boolean executed = false;
        for (var rId : cluster.getAllReplicaIds()) {
            for (var iCell : instanceSpace.notExecutedInstances(rId)) {
                LOGGER.trace("Try to execute - replicaId={}, instanceId={}", iCell.getReplicaId(), iCell.getInstanceId());
                var instance = iCell.getInstance();
                if (instance != null && instance.getStatus() == EXECUTED) {
                    instanceSpace.moveExecutedUpTo(rId, instance.getInstanceId());
                    continue;
                }
                if (instance == null || instance.getStatus() != COMMITTED) {
                    handleProblemInstance(rId, iCell.getInstanceId());
                    if (instance == null) {
                        continue;
                    }
                    break;
                }
                try {
                    if (executeCommand(instance)) {
                        instanceSpace.moveExecutedUpTo(rId, instance.getInstanceId());
                        executed = true;
                    }
                } catch (WaitCommittedException e) {
                    // we got a dependency to not-ready instance, 
                    // our progress for this replica is blocked until recovery
                    continue;
                }
            }
        }
        return executed;
    }

    static record ProblemInstance(int instanceId, long startGracePeriod) {}; 

    private void handleProblemInstance(String replicaId, int instanceId) {
        var problemInstance = problemInstances.get(replicaId);
        if (commitGracePeriodMs > 0) {
            if (problemInstance != null && problemInstance.instanceId == instanceId) {
                if (System.currentTimeMillis() - problemInstance.startGracePeriod >= commitGracePeriodMs) {
                    driver.enqueue(() -> recoveryInitiator.startRecoveryForInstance(replicaId, instanceId));
                    problemInstances.remove(replicaId);
                }
            } else {
                problemInstances.put(replicaId, new ProblemInstance(instanceId, System.currentTimeMillis()));
            }
        } else {
            driver.enqueue(() -> recoveryInitiator.startRecoveryForInstance(replicaId, instanceId));
        }
        
    }

    private boolean executeCommand(Instance instance) throws WaitCommittedException {
        LOGGER.trace("Execute instance - replicaId={}, instanceId={}", instance.getReplicaId(), instance.getInstanceId());
        
        if (instance.getStatus() == EXECUTED) {
            LOGGER.trace("Execute instance: already executed - replicaId={}, instanceId={}", instance.getReplicaId(), instance.getInstanceId());
            return true;
        }
        if (instance.getStatus() != COMMITTED) {
            LOGGER.trace("Execute instance: not committed - replicaId={}, instanceId={}", instance.getReplicaId(), instance.getInstanceId());
            return false;
        }

        if (!findSCC(instance)) {
            LOGGER.trace("Execute instance: scc failed - replicaId={}, instanceId={}", instance.getReplicaId(), instance.getInstanceId());
            return false;
        }

        LOGGER.trace("Execute instance: scc done - replicaId={}, instanceId={}", instance.getReplicaId(), instance.getInstanceId());
        return true;
    }

    private boolean findSCC(Instance instance) throws WaitCommittedException {
        var scc = new StronglyConnectedComponentsExecutor();
        LOGGER.trace("Find SCC: start - replicaId={}, instanceId={}", instance.getReplicaId(), instance.getInstanceId());
        return scc.findAndExcute(instance, 1);
    }

    // Tarjan's strongly connected components algorithm implementation
    private class StronglyConnectedComponentsExecutor {
        final Deque<Instance> stack = new ArrayDeque<>();
        final Map<VerticeKey, Vertice> vertices = new HashMap<>();

        private Vertice vertice(Instance i) {
            return vertices.computeIfAbsent(new VerticeKey(i), k -> new Vertice());
        }

        private boolean findAndExcute(Instance v, int index) throws WaitCommittedException {
            var verV = vertice(v);
            verV.index = index;
            verV.lowLink = index;
            LOGGER.trace("Find SCC: visit vertice - replicaId={}, instanceId={}, vertice={}, deps={}", 
                v.getReplicaId(), v.getInstanceId(), verV, v.getAttributes().deps());
            stack.push(v);
            index++;
    
            for (var rId : cluster.getAllReplicaIds()) {
                var depInstanceId = v.getAttributes().dep(rId);
                for (var wCell : instanceSpace.notExecutedInstances(rId, depInstanceId)) {
                    waitCommand(wCell);
                    if (wCell.getInstance().getStatus() == EXECUTED) {
                        continue;
                    }
                    waitCommitted(wCell);
                    var w = wCell.getInstance();
                    var verW = vertice(w);
                    LOGGER.trace("Find SCC: dependency vertice - replicaId={}, instanceId={}, vertice={}", 
                        v.getReplicaId(), v.getInstanceId(), verW);
                    if (verW.isNotVisited()) {
                        if (!findAndExcute(w, index)) {
                            Instance r;
                            do {
                                r = stack.pop();
                                vertice(r).markNotVisited();
                            } while (r != v);
                            return false;
                        }
                        verV.lowLink = Math.min(verW.lowLink, verV.lowLink);
                    } else {
                        verV.lowLink = Math.min(verW.index, verV.lowLink);
                    }
                    LOGGER.trace("Find SCC: update vertice - replicaId={}, instanceId={}, vertice={}", 
                        v.getReplicaId(), v.getInstanceId(), verV);
                }
            }
    
            if (verV.lowLink == verV.index) {
                var list = new ArrayList<Instance>();
                Instance r;
                do {
                    r = stack.pop();
                    list.add(r);
                } while (r != v);
                Collections.sort(list, Comparator.comparing(i -> i.getAttributes().seq()));
                if (LOGGER.isTraceEnabled()) {
                    for (var i : list) {
                        LOGGER.trace("Execution chain element - replicaId={}, instanceId={}, command={}", 
                            i.getReplicaId(), i.getInstanceId(), i.getCommand());
                    }
                }
                for (var i : list) {
                    var opId = i.getCommand().operation();
                    ExternalMessage reply;
                    try {
                        var result = operationRegistry.getOperation(opId).execute(storage, i.getCommand());
                        reply = okReply(i, result);
                    } catch (CommandOperation.Error err) {
                        reply = errorReply(i, err.code(), err.text());
                    }

                    if (reply != null) {
                        network.send(reply);
                    }
                    
                    i.setStatus(EXECUTED);
                }
            }
            return true;
        }

        private ExternalMessage okReply(Instance i, Object result) {
            ExternalMessage reply = null;
            if (i.replyData() != null) {
                var replyData = i.replyData();
                reply = switch(replyData.type()) {
                    case REQUEST -> null;
                    case READ -> ExternalMessage.okReadReply(i.getReplicaId(), 
                                                             replyData.clientId(), 
                                                             replyData.meta(), 
                                                             result);
                    case REQUEST_AND_READ -> ExternalMessage.okRequestAndReadReply(i.getReplicaId(), 
                                                                                   replyData.clientId(), 
                                                                                   replyData.meta(), 
                                                                                   result);
                };
            }
            LOGGER.debug("Command executed - replicaId={}, instanceId={}, result={}, reply={}", 
                i.getReplicaId(), i.getInstanceId(), result, reply);
            return reply;
        }
    
        private ExternalMessage errorReply(Instance i, int errorCode, String errorText) {
            ExternalMessage reply = null;
            if (i.replyData() != null) {
                var replyData = i.replyData();
                reply = switch(replyData.type()) {
                    case REQUEST -> null;
                    case READ -> ExternalMessage.errorReadReply(i.getReplicaId(), 
                                                                replyData.clientId(), 
                                                                replyData.meta(), 
                                                                errorCode, 
                                                                errorText);
                    case REQUEST_AND_READ -> ExternalMessage.errorRequestAndReadReply(i.getReplicaId(), 
                                                                                      replyData.clientId(), 
                                                                                      replyData.meta(), 
                                                                                      errorCode, 
                                                                                      errorText);
                };
                
            }
            LOGGER.debug("Command executed with error - replicaId={}, instanceId={}, errorCode={}, errorText={}, reply={}", 
                i.getReplicaId(), i.getInstanceId(), errorCode, errorText, reply);
            return reply;
        }
    
        private static record VerticeKey(String replicaId, int instanceId) {
            VerticeKey(Instance instance) {
                this(instance.getReplicaId(), instance.getInstanceId());
            }
        }

        private static class Vertice {
            int index, lowLink;
            boolean isNotVisited() {
                return index == 0;
            }
            void markNotVisited() {
                index = 0;
                lowLink = 0;
            }
            @Override
            public String toString() {
                return "Vertice [index=" + index + ", lowLink=" + lowLink + "]";
            }
            
        }

        private Instance waitCommand(Cell c) throws WaitCommittedException {
            int tries = 0;
            var i = c.getInstance();
            while (i == null || i.getCommand() == null) {
                if (tries > maxWaitCommitTries) {
                    LOGGER.error("Does not have a command to execute for - replicaId={}, instanceId={}", 
                                 c.getReplicaId(), c.getInstanceId());
                    throw new WaitCommittedException();
                }
                driver.sleep(waitCommitPeriodMs);
                i = c.getInstance();
                tries++;
            }
            return i;
        }

        private Instance waitCommitted(Cell c) throws WaitCommittedException {
            int tries = 0;
            var i = c.getInstance();
            while (i.getStatus() != COMMITTED) {
                if (tries > maxWaitCommitTries) {
                    LOGGER.error("Not committed command - replicaId={}, instanceId={}", 
                                 c.getReplicaId(), c.getInstanceId());
                    throw new WaitCommittedException();
                }
                driver.sleep(waitCommitPeriodMs);
                i = c.getInstance();
                tries++;
            }
            return i;
        }
    }

    private static class WaitCommittedException extends Exception {

    }
}
