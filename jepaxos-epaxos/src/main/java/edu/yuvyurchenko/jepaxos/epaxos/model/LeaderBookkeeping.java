package edu.yuvyurchenko.jepaxos.epaxos.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyData;

public class LeaderBookkeeping {
    private final ReplyData replyData;
    private Ballot maxRecvBallot;
    private int prepareOKs;
    private boolean allEqual;
    private int preAcceptOKs;
    private int acceptOKs;
    private int tryPreAcceptOKs;
    private int nacks;
    private boolean preparing;
    private Map<String, Integer> originalDeps;
    private Map<String, Integer> committedDeps;
    private RecoveryInstance recoveryInstance;
    private Map<String, Boolean> possibleQuorum;
    private boolean tryingToPreAccept;

    LeaderBookkeeping(ReplyData replyData, 
                      Map<String, Integer> originalDeps) {
        this.replyData = replyData;
        this.allEqual = true;
        this.originalDeps = originalDeps;
        this.preparing = false;
    }

    LeaderBookkeeping(ReplyData replyData, 
                      Map<String, Integer> originalDeps,
                      boolean preparing) {
        this.replyData = replyData;
        this.allEqual = true;
        this.originalDeps = originalDeps;
        this.preparing = preparing;
    }

    public ReplyData getReplyData() {
        return replyData;
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

    public RecoveryInstance recovertInstance() {
        return recoveryInstance;
    }

    public void initPossibleQuorum(List<String> allReplicaIds) {
        this.possibleQuorum = allReplicaIds.stream().collect(Collectors.toMap(i -> i, i -> true));
    }

    public void updatePossibleQuorum(String replicaId, boolean flag) {
        this.possibleQuorum.put(replicaId, flag);
    }

    public long countPossibleQuorum(boolean val) {
        return possibleQuorum.values().stream().filter(v -> v == val).count();
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
}
