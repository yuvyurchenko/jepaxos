package edu.yuvyurchenko.jepaxos.epaxos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage;
import edu.yuvyurchenko.jepaxos.epaxos.model.Instance;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.CommandOperation;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Storage;

public class CommandExecutor {

    private static long SLEEP_TIME_MS = 1L;
    private static long COMMIT_GRACE_PERIOD_MS = 10000L;

    private final Cluster cluster;
    private final InstanceSpace instanceSpace;
    private final Storage storage;
    private final Network network;
    private final RecoveryInitiator recoveryInitiator;
    private final CommandOperationRegistry operationRegistry;

    private Map<String, Integer> problemInstances;
    private Map<String, Long> timeout; 

    private volatile boolean shutdown;

    CommandExecutor(Cluster cluster,
                    InstanceSpace instanceSpace,
                    Storage storage,
                    Network network,
                    RecoveryInitiator recoveryInitiator,
                    CommandOperationRegistry operationRegistry) {
        this.cluster = cluster;
        this.instanceSpace = instanceSpace;
        this.storage = storage;
        this.network = network;
        this.recoveryInitiator = recoveryInitiator;
        this.operationRegistry = operationRegistry;

        this.problemInstances = new HashMap<>();
        this.timeout = new HashMap<>();
    }

    public void executeCommands() {
        while (!shutdown) {
            boolean executed = false;
            for (var rId : cluster.getAllReplicaIds()) {
                for (var iId : instanceSpace.notExecutedInstanceIds(rId)) {
                    var instance = instanceSpace.getInstance(rId, iId);
                    if (instance != null && instance.getStatus() == InstanceStatus.EXECUTED) {
                        if (instance.getInstanceId() == instanceSpace.getExecutedUpTo(rId) + 1) {
                            instanceSpace.putExecutedUpTo(rId, instance.getInstanceId());
                        }
                        continue;
                    }
                    if (instance == null || instance.getStatus() != InstanceStatus.COMMITTED) {
                        if (iId == problemInstances.getOrDefault(rId, -1)) {
                            var newTimeout = timeout.getOrDefault(rId, 0L) + SLEEP_TIME_MS;
                            timeout.put(rId, newTimeout);
                            if (newTimeout >= COMMIT_GRACE_PERIOD_MS) {
                                recoveryInitiator.startRecoveryForInstance(rId, iId);
                                timeout.remove(rId);
                            }
                        } else {
                            problemInstances.put(rId, instance.getInstanceId());
                            timeout.remove(rId);
                        }
                        if (instance == null) {
                            continue;
                        }
                        break;
                    }
                    if (executeCommand(instance)) {
                        executed = true;
                        if (instance.getInstanceId() == instanceSpace.getExecutedUpTo(rId) + 1) {
                            instanceSpace.putExecutedUpTo(rId, instance.getInstanceId());
                        }
                    }
                }
            }
            if (!executed) {
                try {
                    Thread.sleep(SLEEP_TIME_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    shutdown = true;
                }
            }
        }
    }

    private boolean executeCommand(Instance instance) {
        if (instance  == null) {
            return false;
        }
        if (instance.getStatus() == InstanceStatus.EXECUTED) {
            return true;
        }
        if (instance.getStatus() != InstanceStatus.COMMITTED) {
            return false;
        }

        if (!findSCC(instance)) {
            return false;
        }

        return true;
    }

    private boolean findSCC(Instance instance) {
        int index = 1;
        var stack = new Stack<Instance>();
        return strongconnect(instance, index, stack);
    }

    private boolean strongconnect(Instance v, int index, Stack<Instance> stack) {
        v.setIndex(index);
        v.setLowLink(index);
        index++;

        stack.push(v);

        for (var rId : cluster.getAllReplicaIds()) {
            var depInstanceId = v.getAttributes().deps().getOrDefault(rId, -1);
            for (var w : instanceSpace.notExecutedInstances(rId, depInstanceId)) {
                while (v.getCommand() == null || w.getCommand() == null) {
                    // Thread.sleep(10);
                }
                if (w.getStatus() == InstanceStatus.EXECUTED) {
                    continue;
                }
                while (w.getStatus() != InstanceStatus.COMMITTED) {
                    // Thread.sleep(10);
                }
                if (w.getIndex() == 0) {
                    if (!strongconnect(w, index, stack)) {
                        Instance r;
                        do {
                            r = stack.pop();
                            r.setIndex(0);
                            r.setLowLink(0);
                        } while (r != v);
                        return false;
                    }
                    v.setLowLink(Math.min(w.getLowLink(), v.getLowLink()));
                } else {
                    v.setLowLink(Math.min(w.getIndex(), v.getLowLink()));
                }
            }
        }

        if (v.getLowLink() == v.getIndex()) {
            List<Instance> list = new ArrayList<>();
            Instance r;
            do {
                r = stack.pop();
                list.add(r);
            } while (r != v);
            Collections.sort(list, Comparator.comparing(i -> i.getAttributes().seq()));
            for (var i : list) {
                while (i.getCommand() == null) {
                    // Thread.sleep(10);                    
                }

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
                
                i.setStatus(InstanceStatus.EXECUTED);
            }
        }
        return true;
    }

    private ExternalMessage okReply(Instance i, Object result) {
        if (i.leaderBookkeeping() != null && i.leaderBookkeeping().getReplyData() != null) {
            var replyData = i.leaderBookkeeping().getReplyData();
            return switch(replyData.type()) {
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
        return null;
    }

    private ExternalMessage errorReply(Instance i, int errorCode, String errorText) {
        if (i.leaderBookkeeping() != null && i.leaderBookkeeping().getReplyData() != null) {
            var replyData = i.leaderBookkeeping().getReplyData();
            return switch(replyData.type()) {
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
        return null;
    }
}
