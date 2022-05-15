package edu.yuvyurchenko.jepaxos.epaxos.model;

import java.util.Map;

import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyData;

public class Instance {
    // keys
    private final String replicaId;
    private final int instanceId;
    
    private Command command;
    private Ballot ballot;
    private InstanceStatus status;
    private Attributes attributes;
    private LeaderBookkeeping leaderBookkeeping;

    // for Tarjan's algorithm
    private int index;
    private int lowLink;
    private boolean onStack;

    public Instance(String replicaId,
                    int instanceId,
                    Command command,
                    Ballot ballot,
                    InstanceStatus status,
                    Attributes attributes) {
        this.replicaId = replicaId;
        this.instanceId = instanceId;
        this.command = command;
        this.ballot = ballot;
        this.status = status;
        this.attributes = attributes;
        this.leaderBookkeeping = null;
    }

    public Instance(String replicaId,
                    int instanceId,
                    Command command,
                    Ballot ballot,
                    InstanceStatus status,
                    Attributes attributes,
                    ReplyData replyData) {
        this.replicaId = replicaId;
        this.instanceId = instanceId;
        this.command = command;
        this.ballot = ballot;
        this.status = status;
        this.attributes = attributes;
        this.leaderBookkeeping = new LeaderBookkeeping(replyData, attributes.deps());
    }

    public Instance(String replicaId,
                    int instanceId,
                    Command command,
                    Ballot ballot,
                    InstanceStatus status,
                    Attributes attributes,
                    LeaderBookkeeping leaderBookkeeping) {
        this.replicaId = replicaId;
        this.instanceId = instanceId;
        this.command = command;
        this.ballot = ballot;
        this.status = status;
        this.attributes = attributes;
        this.leaderBookkeeping = leaderBookkeeping;
    }

    public String getReplicaId() {
        return replicaId;
    }

    public int getInstanceId() {
        return instanceId;
    }

    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    public Ballot getBallot() {
        return ballot;
    }

    public void setBallot(Ballot ballot) {
        this.ballot = ballot;
    }

    public InstanceStatus getStatus() {
        return status;
    }

    public void setStatus(InstanceStatus status) {
        this.status = status;
    }

    public Attributes getAttributes() {
        return this.attributes;
    }

    public void setAttributes(Attributes attributes) {
        this.attributes = attributes;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getLowLink() {
        return lowLink;
    }

    public void setLowLink(int lowLink) {
        this.lowLink = lowLink;
    }

    public boolean isOnStack() {
        return onStack;
    }

    public void setOnStack(boolean onStack) {
        this.onStack = onStack;
    }

    public LeaderBookkeeping leaderBookkeeping() {
        return leaderBookkeeping;
    }

    public void switchToRecoveryLeaderBookkeeping() {
        if (this.leaderBookkeeping == null) {
            this.leaderBookkeeping = new LeaderBookkeeping(null, Map.of(), true);
        } else {
            this.leaderBookkeeping = new LeaderBookkeeping(this.leaderBookkeeping.getReplyData(), Map.of(), true);
        }
    }

    public boolean isInitialBallot() {
        return ballot.number() == 0;
    }

    public boolean isNotInitialBallot() {
        return !isInitialBallot();
    }

}
