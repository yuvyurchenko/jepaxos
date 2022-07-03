package edu.yuvyurchenko.jepaxos.epaxos;

import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyData;
import edu.yuvyurchenko.jepaxos.epaxos.model.Attributes;
import edu.yuvyurchenko.jepaxos.epaxos.model.Ballot;
import edu.yuvyurchenko.jepaxos.epaxos.model.Command;
import edu.yuvyurchenko.jepaxos.epaxos.model.Instance;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;

import static edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus.*;

/**
 * Thread Locality (some fields require sync):
 *  - created and published: protocol executing thread
 *  - read & modified: both protocol and command executing thread.
 *    command executing thread modifies only executedUpTo field
 */
public class InstanceSpace {
    private final Cluster cluster;
    private final Map<String, ReplicaMetadata> metadata;
    private final Map<Object, Integer> maxSeqPerKey;
    private final Map<DeferKey, DeferKey> deferMap;
    private int maxSeq;

    InstanceSpace(Cluster cluster) {
        this.cluster = cluster;
        this.metadata = cluster.getAllReplicaIds().stream()
            .collect(toUnmodifiableMap(i -> i, i -> new ReplicaMetadata(i)));
        this.maxSeqPerKey = new HashMap<>();
        this.deferMap = new HashMap<>();
    }

    public Instance registerNewCommandLeaderInstance(Command command, ReplyData replyData) {
        var meta = metadata.get(cluster.getCurrReplicaId());
        meta.rwLock.writeLock().lock();
        try {
            var attributes = createAttributes(command);
            var instance = new Instance(cluster.getCurrReplicaId(),
                                        meta.instances.size(), 
                                        command, 
                                        new Ballot(0, cluster.getCurrReplicaId()), 
                                        PREACCEPTED, 
                                        attributes,
                                        replyData);
            meta.instances.add(new Cell(instance.getReplicaId(), instance.getInstanceId(), instance));
            return instance;
        } finally {
            meta.rwLock.writeLock().unlock();
        }
    }

    public Instance resetCommandLeaderInstance(String sourceReplicaId, 
                                               int instanceId, 
                                               Ballot ballot, 
                                               Command command, 
                                               ReplyData replyData) {
        var meta = metadata.get(cluster.getCurrReplicaId());
        meta.rwLock.writeLock().lock();
        try {
            var updAttributes = updateAttributes(sourceReplicaId, instanceId, command, new Attributes(0, Map.of()));
            meta.adjustCells(instanceId);
            var instance = new Instance(cluster.getCurrReplicaId(),
                                        instanceId, 
                                        command, 
                                        ballot, 
                                        PREACCEPTED, 
                                        updAttributes.attributes(),
                                        replyData);
            meta.instances.get(instanceId).setInstance(instance);
            return instance;
        } finally {
            meta.rwLock.writeLock().unlock();
        }
    }

    public Instance registerNewInstance(String replicaId, 
                                        int instanceId, 
                                        Command command, 
                                        Ballot ballot, 
                                        InstanceStatus status, 
                                        Attributes attributes) {
        var meta = metadata.get(replicaId);
        meta.rwLock.writeLock().lock();
        try {
            meta.adjustCells(instanceId);
            var instance = new Instance(replicaId, instanceId, command, ballot, status, attributes);
            meta.instances.get(instanceId).setInstance(instance);
            return instance;
        } finally {
            meta.rwLock.writeLock().unlock();
        }
    }

    public Instance registerNewInstance(String replicaId, 
                                        int instanceId, 
                                        Command command, 
                                        InstanceStatus status, 
                                        Attributes attributes,
                                        Instance srcInstance) {
        var meta = metadata.get(replicaId);
        meta.rwLock.writeLock().lock();
        try {
            var instance = new Instance(replicaId, 
                                        instanceId, 
                                        command, 
                                        srcInstance.getBallot(), 
                                        status, 
                                        attributes, 
                                        srcInstance.leaderBookkeeping(),
                                        srcInstance.replyData());
            meta.adjustCells(instanceId);
            meta.instances.get(instanceId).setInstance(instance);
            return instance;
        } finally {
            meta.rwLock.writeLock().unlock();
        }
    }

    public Instance getInstance(String replicaId, int instanceId) {
        var meta = metadata.get(replicaId);
        meta.rwLock.readLock().lock();
        try {
            var instances = meta.instances;
            if (instances.size() > instanceId) {
                return instances.get(instanceId).getInstance();
            }
            return null;
        } finally {
            meta.rwLock.readLock().unlock();
        }
    }

    private Attributes createAttributes(Command command) {
        var key = command.key();
        var dependsOn = replicaConflicts()
            .filter(rc -> rc.conflicts().containsKey(key))
            .collect(toUnmodifiableMap(
                rc -> rc.replicaId(), 
                rc -> rc.conflicts().get(key)));

        int seq = dependsOn.entrySet().stream()
            .map(e -> getInstance(e.getKey(), e.getValue()))
            .mapToInt(i -> i.getAttributes().seq() + 1)
            .max().orElse(0);
        
        int knownMaxSeq = maxSeqPerKey(command.key());
        if (knownMaxSeq >= seq) {
            seq = knownMaxSeq + 1;
        }

        return new Attributes(seq, dependsOn);
    }

    public void adjustCrtInstanceId(String replicaId, int knownInstanceId) {
        var meta = metadata.get(replicaId);
        meta.rwLock.writeLock().lock();
        try {
            meta.adjustCells(knownInstanceId);
        } finally {
            meta.rwLock.writeLock().unlock();
        }
    }

    public AttributesUpdateResult updateAttributes(String sourceReplicaId,
                                                   int instanceId,
                                                   Command command,
                                                   Attributes attributes) {
        var key = command.key();
        
        var deltaDeps = replicaConflicts()
            .filter(rc -> !rc.replicaId().equals(sourceReplicaId))
            .filter(rc -> rc.conflict(key) > attributes.dep(rc.replicaId()))
            .collect(toUnmodifiableMap(
                rc -> rc.replicaId(), 
                rc -> rc.conflicts().get(key)));
        
        boolean changed = !deltaDeps.isEmpty();

        int newSeq = deltaDeps.entrySet().stream()
            .map(e -> getInstance(e.getKey(), e.getValue()))
            .mapToInt(i -> i.getAttributes().seq() + 1)
            .max().orElse(attributes.seq());
        
        int knownMaxSeq = maxSeqPerKey(command.key());
        if (knownMaxSeq >= newSeq) {
            newSeq = knownMaxSeq + 1;
            changed = true;
        }

        var newDeps = Stream.of(deltaDeps, attributes.deps())
            .map(Map::entrySet)
            .flatMap(Set::stream)
            .collect(toUnmodifiableMap(
                Map.Entry::getKey, 
                Map.Entry::getValue, 
                Math::max));

        return new AttributesUpdateResult(changed, new Attributes(newSeq, newDeps));
    }

    public AttributesMergeResult mergeAttributes(Attributes a1, Attributes a2) {
        boolean equal = a1.seq() == a2.seq();
        int newSeq = Math.max(a1.seq(), a2.seq());
        
        var allKeys = new HashSet<String>();
        allKeys.addAll(a1.deps().keySet());
        allKeys.addAll(a2.deps().keySet());

        var newDeps = new HashMap<String, Integer>();
        for (var k : allKeys) {
            if (k.equals(cluster.getCurrReplicaId())) {
                if (a1.deps().containsKey(k)) {
                    newDeps.put(k, a1.deps().get(k));
                }
            } else {
                int v1 = a1.dep(k);
                int v2 = a2.dep(k);
                equal = equal && v1 == v2;
                int newVal = Math.max(v1, v2);
                newDeps.put(k, newVal);
            }
        }
        return new AttributesMergeResult(equal, new Attributes(newSeq, Map.copyOf(newDeps)));
    }

    public void updateConflicts(Instance instance) {
        updateConflicts(
            instance.getCommand(), 
            instance.getReplicaId(), 
            instance.getInstanceId(), 
            instance.getAttributes().seq());
    }

    public void updateConflicts(Command command, String replicaId, int instanceId, int seq) {
        var key = command.key();
        
        var localConflicts = metadata.get(replicaId).conflicts;
        var conflictingInstace = localConflicts.get(key);
        if (conflictingInstace != null) {
            if (conflictingInstace < instanceId) {
                localConflicts.put(key, instanceId);
            }
        } else {
            localConflicts.put(key, instanceId);
        }

        int newSeq = seq;
        int maxSeq = maxSeqPerKey(key);
        if (maxSeq < newSeq) {
            maxSeqPerKey.put(key, newSeq);
        }
    }

    public boolean hasUncommittedDeps(Map<String, Integer> deps) {
        return deps.entrySet().stream()
            .anyMatch(p((replicaId, dependsOn) -> dependsOn > metadata.get(replicaId).commitedUpTo));
    }

    public boolean updateCommittedDeps(Instance instance, Map<String, Integer> srcDeps) {
        var lb = instance.leaderBookkeeping();
        var attrs = instance.getAttributes();

        boolean allCommitted = true;
        for (var replicaId : metadata.keySet()) {
            var commitedUpTo = metadata.get(replicaId).commitedUpTo;
            var srcDep = srcDeps.getOrDefault(replicaId, -1);
            var dep = attrs.dep(replicaId);

            if (lb.getCommittedDep(replicaId) < srcDep) {
                lb.putCommittedDep(replicaId, srcDep);
            }
            if (lb.getCommittedDep(replicaId) < commitedUpTo) {
                lb.putCommittedDep(replicaId, commitedUpTo);
            }
            if (lb.getCommittedDep(replicaId) < dep) {
                allCommitted = false;
            }
        }

        return allCommitted;
    }

    public void updateCommitted(String replicaId) {
        var meta = metadata.get(replicaId);
        meta.rwLock.readLock().lock();
        try {
            int nextCommitted = meta.commitedUpTo + 1;
            if (nextCommitted < meta.instances.size()) {
                var instance = meta.instances.get(nextCommitted).getInstance();
                if (instance != null && (instance.getStatus() == COMMITTED 
                                     || instance.getStatus() == EXECUTED)) {
                    meta.commitedUpTo++;
                }
            }
        } finally {
            meta.rwLock.readLock().unlock();
        }
    }

    public Map<String, Integer> commitedUpTo() {
        return metadata.entrySet().stream()
            .collect(toUnmodifiableMap(
                Map.Entry::getKey, 
                e -> e.getValue().commitedUpTo));
    }
    
    public int getMaxSeq() {
        return maxSeq;
    }

    public void setMaxSeq(int maxSeq) {
        this.maxSeq = maxSeq;
    }

    private Stream<ReplicaConflict> replicaConflicts() {
        return metadata.entrySet().stream()
            .map(e -> new ReplicaConflict(e.getKey(), e.getValue().conflicts));
    }

    private static <K, V> Predicate<Map.Entry<K, V>> p(BiPredicate<K, V> bp) {
        return e -> bp.test(e.getKey(), e.getValue());
    }

    public void moveExecutedUpTo(String replicaId, int instanceId) {
        var meta = metadata.get(replicaId);
        meta.rwLock.writeLock().lock();
        try {
            var nextExecuted = meta.executedUpTo + 1;
            if (instanceId == nextExecuted) {
                meta.executedUpTo = instanceId;
            }
        } finally {
            meta.rwLock.writeLock().unlock();
        }
    }

    public List<Cell> notExecutedInstances(String replicaId) {
        var meta = metadata.get(replicaId);
        meta.rwLock.readLock().lock();
        try {
            int start = meta.executedUpTo + 1;
            int end = meta.instances.size();
            if (start < end) {
                return meta.instances.subList(start, end);
            }
            return Collections.emptyList();
        } finally {
            meta.rwLock.readLock().unlock();
        }
    }

    public int[] notExecutedInstanceIds(String replicaId) {
        var meta = metadata.get(replicaId);
        meta.rwLock.readLock().lock();
        try {
            int start = meta.executedUpTo + 1;
            int end = meta.instances.size();
            if (start < end) {
                return IntStream.range(start, end).toArray();
            }
            return new int[0];
        } finally {
            meta.rwLock.readLock().unlock();
        }
    }

    public List<Cell> notExecutedInstances(String replicaId, int upperBoundInstanceId) {
        var meta = metadata.get(replicaId);
        meta.rwLock.readLock().lock();
        try {
            int start = meta.executedUpTo + 1;
            int end = upperBoundInstanceId + 1;
            if (start < end) {
                meta.adjustCells(upperBoundInstanceId);
                return meta.instances.subList(start, end);
            }
            return Collections.emptyList();
        } finally {
            meta.rwLock.readLock().unlock();
        }
    }

    public int[] notExecutedInstanceIds(String replicaId, int upperBoundInstanceId) {
        var meta = metadata.get(replicaId);
        meta.rwLock.readLock().lock();
        try {
            int start = meta.executedUpTo + 1;
            int end = upperBoundInstanceId;
            if (start <= end) {
                return IntStream.rangeClosed(start, end).toArray();
            }
            return new int[0];
        } finally {
            meta.rwLock.readLock().unlock();
        }
    }

    public void updateDeferred(String dReplicaId, int dInstanceId, String replicaId, int instanceId) {
        var daux = new DeferKey(dReplicaId, dInstanceId);
        var aux = new DeferKey(replicaId, instanceId);
        deferMap.put(aux, daux);
    }

    public DeferCheck deferredByInstance(String replicaId, int instanceId) {
        var aux = new DeferKey(replicaId, instanceId);
        var daux = deferMap.get(aux);
        if (daux == null) {
            return new DeferCheck(false, null);
        }
        return new DeferCheck(true, daux);
    }

    private int maxSeqPerKey(Object key) {
        return maxSeqPerKey.getOrDefault(key, -1);
    }

    private static class ReplicaMetadata {
        // guards instances and executedUpTo since all other fields 
        // are changed in the protocol executing thread only
        private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
        private final String replicaId;
        private int executedUpTo = -1;
        private final List<Cell> instances = new ArrayList<>();
        // protocol executing thread specific
        private final Map<Object, Integer> conflicts = new HashMap<>();
        private int commitedUpTo = -1;

        ReplicaMetadata(String replicaId) {
            this.replicaId = replicaId;
        }

        void adjustCells(int upperInstanceId) {
            if (instances.size() <= upperInstanceId) {
                IntStream.rangeClosed(instances.size(), upperInstanceId).forEach(i -> 
                    instances.add(new Cell(replicaId, i, null)));
            }
        }
    }

    public static record DeferKey(String replicaId, int instanceId) {}
    public static record DeferCheck(boolean deffer, DeferKey key) {}
    public static record AttributesUpdateResult(boolean changed, Attributes attributes) {}
    public static record AttributesMergeResult(boolean equal, Attributes attributes) {}
    public static record ReplicaConflict(String replicaId, Map<Object, Integer> conflicts) {
        public int conflict(Object key) {
            return conflicts.getOrDefault(key, -1);
        }
    }

    public static class Cell {
        private final String replicaId;
        private final int instanceId;
        private volatile Instance instance;

        private Cell(String replicaId, int instanceId, Instance instance) {
            this.replicaId = replicaId;
            this.instanceId = instanceId;
            this.instance = instance;
        }

        public String getReplicaId() {
            return replicaId;
        }

        public int getInstanceId() {
            return instanceId;
        }

        public Instance getInstance() {
            return instance;
        }

        public void setInstance(Instance instance) {
            this.instance = instance;
        }

    }

}
