package edu.yuvyurchenko.jepaxos.epaxos.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Thread Locality (some fields require sync):
 *  - created and published: protocol executing thread
 *  - read and modified: protocol executing thread
 */
public class LeaderBookkeeping {
    private Ballot maxRecvBallot;
    private int prepareOKs;
    private boolean allEqual;
    private int preAcceptOKs;
    private int acceptOKs;
    private int tryPreAcceptOKs;
    private int nacks;
    private boolean preparing;
    private final Map<String, Integer> originalDeps;
    private final Map<String, Integer> committedDeps;
    private Map<String, Boolean> possibleQuorum;
    private RecoveryInstance recoveryInstance;
    private boolean tryingToPreAccept;

    LeaderBookkeeping(Map<String, Integer> originalDeps) {
        this.allEqual = true;
        this.originalDeps = new HashMap<>(originalDeps);
        this.committedDeps = new HashMap<>();
        this.possibleQuorum = new HashMap<>();
        this.preparing = false;
    }

    LeaderBookkeeping(Map<String, Integer> originalDeps,
                      boolean preparing) {
        this.allEqual = true;
        this.originalDeps = new HashMap<>(originalDeps);
        this.committedDeps = new HashMap<>();
        this.possibleQuorum = new HashMap<>();
        this.preparing = preparing;
    }

    public void incPrepareOKs() {
        prepareOKs++;
    }

    public void incPreAcceptOKs() {
        preAcceptOKs++;
    }

    public void incAcceptOKs() {
        acceptOKs++;
    }

    public void incTryPreAcceptOKs() {
        tryPreAcceptOKs++;
    }

    public void incNacks() {
        nacks++;
    }

    public void setNacks(int nacks) {
        this.nacks = nacks;
    }

    public Ballot getMaxRecvBallot() {
        return maxRecvBallot;
    }

    public void setMaxRecvBallot(Ballot maxRecvBallot) {
        this.maxRecvBallot = maxRecvBallot;
    }

    public Map<String, Integer> getOriginalDeps() {
        return Collections.unmodifiableMap(originalDeps);
    }

    public int getOriginalDep(String replicaId) {
        return originalDeps.getOrDefault(replicaId, -1);
    }

    public void putOriginalDep(String replicaId, int instanceId) {
        originalDeps.put(replicaId, instanceId);
    }

    public int getCommittedDep(String replicaId) {
        return committedDeps.getOrDefault(replicaId, -1);
    }

    public void putCommittedDep(String replicaId, int instanceId) {
        committedDeps.put(replicaId, instanceId);
    }

    public int getPrepareOKs() {
        return prepareOKs;
    }

    public int getPreAcceptOKs() {
        return preAcceptOKs;
    }

    public void setPreAcceptOKs(int preAcceptOKs) {
        this.preAcceptOKs = preAcceptOKs;
    }

    public int getAcceptOKs() {
        return acceptOKs;
    }

    public void setAcceptOKs(int acceptOKs) {
        this.acceptOKs = acceptOKs;
    }

    public int getTryPreAcceptOKs() {
        return tryPreAcceptOKs;
    }

    public boolean isAllEqual() {
        return allEqual;
    }

    public void setAllEqual(boolean allEqual) {
        this.allEqual = allEqual;
    }

    public boolean getPreparing() {
        return preparing;
    }

    public void setPreparing(boolean preparing) {
        this.preparing = preparing;
    }

    public void initRecoveryInstance(Command command,
                                     InstanceStatus status,
                                     Attributes attributes,
                                     int preAcceptCount,
                                     boolean leaderResponded) {
        this.recoveryInstance = new RecoveryInstance(command, status, attributes, preAcceptCount, leaderResponded);
    }

    public RecoveryInstance recoveryInstance() {
        return recoveryInstance;
    }

    public void initPossibleQuorum(List<String> allReplicaIds) {
        this.possibleQuorum = allReplicaIds.stream().collect(Collectors.toMap(i -> i, i -> true));
    }

    public void updatePossibleQuorum(String replicaId, boolean flag) {
        this.possibleQuorum.put(replicaId, flag);
    }

    public long countPossibleQuorum(boolean val) {
        return possibleQuorum.values().stream().filter(v -> Objects.equals(v, val)).count();
    }

    public boolean isPossibleQuorum(String replicaId) {
        return this.possibleQuorum.getOrDefault(replicaId, false);
    }

    public boolean isTryingToPreAccept() {
        return this.tryingToPreAccept;
    }

    public void setTryingToPreAccept(boolean tryingToPreAccept) {
        this.tryingToPreAccept = tryingToPreAccept;
    }

    @Override
    public String toString() {
        return "LeaderBookkeeping [acceptOKs=" + acceptOKs + ", allEqual=" + allEqual + ", committedDeps="
                + committedDeps + ", maxRecvBallot=" + maxRecvBallot + ", nacks=" + nacks + ", originalDeps="
                + originalDeps + ", possibleQuorum=" + possibleQuorum + ", preAcceptOKs=" + preAcceptOKs
                + ", prepareOKs=" + prepareOKs + ", preparing=" + preparing + ", recoveryInstance=" + recoveryInstance
                + ", tryPreAcceptOKs=" + tryPreAcceptOKs + ", tryingToPreAccept=" + tryingToPreAccept + "]";
    }
    
    
}
