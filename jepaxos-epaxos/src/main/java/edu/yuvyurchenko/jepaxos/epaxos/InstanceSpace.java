package edu.yuvyurchenko.jepaxos.epaxos;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyData;
import edu.yuvyurchenko.jepaxos.epaxos.model.Attributes;
import edu.yuvyurchenko.jepaxos.epaxos.model.Ballot;
import edu.yuvyurchenko.jepaxos.epaxos.model.Command;
import edu.yuvyurchenko.jepaxos.epaxos.model.Instance;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;

public class InstanceSpace {
    private final Cluster cluster;
    private final Map<String, ReplicaMetadata> metadata;
    private final Map<Object, Integer> maxSeqPerKey;
    private int maxSeq;
    private Map<DeferKey, DeferKey> deferMap;

    InstanceSpace(Cluster cluster) {
        this.cluster = cluster;
        this.metadata = cluster.getAllReplicaIds().stream()
            .collect(toMap(i -> i, i -> new ReplicaMetadata()));
        this.maxSeqPerKey = new HashMap<>();
    }

    public Instance registerNewCommandLeaderInstance(Command command, ReplyData replyData) {
        var attributes = createAttributes(command);

        var replicaId = cluster.getCurrReplicaId();
        var instance = new Instance(replicaId,
                                    metadata.get(replicaId).instances.size(), 
                                    command, 
                                    new Ballot(0, replicaId), 
                                    InstanceStatus.PREACCEPTED, 
                                    attributes,
                                    replyData);

        metadata.get(replicaId).instances.add(instance);
        
        return instance;
    }

    public Instance resetCommandLeaderInstance(String sourceReplicaId, 
                                               int instanceId, 
                                               Ballot ballot, 
                                               Command command, 
                                               ReplyData replyData) {
        var updAttributes = updateAttributes(sourceReplicaId, instanceId, command, new Attributes(0, Map.of()));
        
        
        var instance = new Instance(cluster.getCurrReplicaId(),
                                    instanceId, 
                                    command, 
                                    ballot, 
                                    InstanceStatus.PREACCEPTED, 
                                    updAttributes.attributes(),
                                    replyData);

        var slots = metadata.get(cluster.getCurrReplicaId()).instances;
        if (slots.size() <= instanceId) {
            IntStream.rangeClosed(slots.size(), instanceId + 1).forEach(i -> slots.add(null));
        }
        slots.set(instanceId, instance);
        
        return instance;
    }

    public Instance registerNewInstance(String replicaId, 
                                        int instanceId, 
                                        Command command, 
                                        Ballot ballot, 
                                        InstanceStatus status, 
                                        Attributes attributes) {
        var instance = new Instance(replicaId, instanceId, command, ballot, status, attributes);
        var slots = metadata.get(replicaId).instances;
        if (slots.size() <= instanceId) {
            IntStream.rangeClosed(slots.size(), instanceId + 1).forEach(i -> slots.add(null));
        }
        slots.set(instanceId, instance);
        return instance;
    }

    public Instance registerNewInstance(String replicaId, 
                                        int instanceId, 
                                        Command command, 
                                        InstanceStatus status, 
                                        Attributes attributes,
                                        Instance srcInstance) {
        var instance = new Instance(replicaId, 
                                    instanceId, 
                                    command, 
                                    srcInstance.getBallot(), 
                                    status, 
                                    attributes, 
                                    srcInstance.leaderBookkeeping());
        var slots = metadata.get(replicaId).instances;
        if (slots.size() <= instanceId) {
            IntStream.rangeClosed(slots.size(), instanceId + 1).forEach(i -> slots.add(null));
        }
        slots.set(instanceId, instance);
        return instance;
    }

    public Instance getInstance(String replicaId, int instanceId) {
        var instances = metadata.get(replicaId).instances;
        if (instances.size() > instanceId) {
            return instances.get(instanceId);
        }
        return null;
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
        
        int knownMaxSeq = maxSeqPerKey.getOrDefault(command.key(), -1);
        if (knownMaxSeq >= seq) {
            seq = knownMaxSeq + 1;
        }

        return new Attributes(seq, dependsOn);
    }

    public void adjustCrtInstanceId(String replicaId, int knownInstanceId) {
        var slots = metadata.get(replicaId).instances;
        int crtInstanceId = slots.size();
        if (knownInstanceId >= crtInstanceId) {
            IntStream.range(slots.size(), knownInstanceId + 1).forEach(i -> slots.add(null));
        }
    }

    public AttributesUpdateResult updateAttributes(String sourceReplicaId,
                                                   int instanceId,
                                                   Command command,
                                                   Attributes attributes) {
        var key = command.key();
        
        var deltaDeps = replicaConflicts()
            .filter(rc -> !rc.replicaId().equals(sourceReplicaId))
            .filter(rc -> rc.conflicts().getOrDefault(key, -1) > attributes.deps().getOrDefault(rc.replicaId(), -1))
            .collect(toUnmodifiableMap(
                rc -> rc.replicaId(), 
                rc -> rc.conflicts().get(key)));
        
        boolean changed = !deltaDeps.isEmpty();

        int newSeq = deltaDeps.entrySet().stream()
            .map(e -> getInstance(e.getKey(), e.getValue()))
            .mapToInt(i -> i.getAttributes().seq() + 1)
            .max().orElse(attributes.seq());
        
        int knownMaxSeq = maxSeqPerKey.getOrDefault(command.key(), -1);
        if (knownMaxSeq >= newSeq) {
            newSeq = knownMaxSeq + 1;
            changed = true;
        }

        var newDeps = Stream.of(deltaDeps, attributes.deps()).map(Map::entrySet).flatMap(Set::stream)
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
                int v1 = a1.deps().getOrDefault(k, -1);
                int v2 = a2.deps().getOrDefault(k, -1);
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
        int maxSeq = maxSeqPerKey.getOrDefault(key, -1);
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
        var deps = instance.getAttributes().deps();

        boolean allCommitted = true;
        for (var replicaId : metadata.keySet()) {
            var commitedUpTo = metadata.get(replicaId).commitedUpTo;
            var srcDep = srcDeps.getOrDefault(replicaId, -1);
            var dep = deps.get(replicaId);

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
        int nextCommitted = meta.commitedUpTo + 1;
        var instance = meta.instances.get(nextCommitted);
        if (instance != null && instance.getStatus() == InstanceStatus.COMMITTED || instance.getStatus() == InstanceStatus.EXECUTED) {
            meta.commitedUpTo++;
        }
    }

    public Map<String, Integer> commitedUpTo() {
        return metadata.entrySet().stream()
            .collect(toUnmodifiableMap(
                Map.Entry::getKey, 
                e -> e.getValue().commitedUpTo));
    }
    
    public int getExecutedUpTo(String replicaId) {
        return metadata.get(replicaId).executedUpTo;
    }

    public void putExecutedUpTo(String replicaId, int instanceId) {
        metadata.get(replicaId).executedUpTo = instanceId;
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

    public List<Instance> notExecutedInstances(String replicaId) {
        int executedUpTo = metadata.get(replicaId).executedUpTo;
        var slots = metadata.get(replicaId).instances;
        return slots.subList(executedUpTo + 1, slots.size());
    }

    public List<Integer> notExecutedInstanceIds(String replicaId) {
        int executedUpTo = metadata.get(replicaId).executedUpTo;
        var slots = metadata.get(replicaId).instances;
        return IntStream.range(executedUpTo + 1, slots.size()).boxed().collect(Collectors.toList());
    }

    public List<Instance> notExecutedInstances(String replicaId, int upperBoundInstanceId) {
        int executedUpTo = metadata.get(replicaId).executedUpTo;
        var slots = metadata.get(replicaId).instances;
        return slots.subList(executedUpTo + 1, upperBoundInstanceId+1);
    }

    public List<Integer> notExecutedInstanceIds(String replicaId, int upperBoundInstanceId) {
        int executedUpTo = metadata.get(replicaId).executedUpTo;
        return IntStream.rangeClosed(executedUpTo + 1, upperBoundInstanceId).boxed().collect(Collectors.toList());
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

    private static class ReplicaMetadata {
        private final ArrayList<Instance> instances = new ArrayList<>();
        private final Map<Object, Integer> conflicts = new HashMap<>();
        private int commitedUpTo;
        private int executedUpTo = -1;
    }

    public static record DeferKey(String replicaId, int instanceId) {}
    public static record DeferCheck(boolean deffer, DeferKey key) {}
    public static record AttributesUpdateResult(boolean changed, Attributes attributes) {}
    public static record AttributesMergeResult(boolean equal, Attributes attributes) {}
    public static record ReplicaConflict(String replicaId, Map<Object, Integer> conflicts) {}

}
