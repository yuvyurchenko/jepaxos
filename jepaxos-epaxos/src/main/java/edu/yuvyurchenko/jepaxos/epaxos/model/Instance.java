package edu.yuvyurchenko.jepaxos.epaxos.model;

import java.util.Map;

import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyData;

/**
 * Thread Locality (some fields require sync):
 *  - created and published: protocol executing thread
 *  - read & modified: both protocol and command executing thread.
 *    command executing thread modifies only status
 */
public class Instance {
    // keys
    private final String replicaId;
    private final int instanceId;

    private final ReplyData replyData;
    
    private volatile Command command;
    private volatile Ballot ballot;
    private volatile InstanceStatus status;
    private volatile Attributes attributes;
    private volatile LeaderBookkeeping leaderBookkeeping;

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
        this.replyData = null;
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
        this.leaderBookkeeping = new LeaderBookkeeping(attributes.deps());
        this.replyData = replyData;
    }

    public Instance(String replicaId,
                    int instanceId,
                    Command command,
                    Ballot ballot,
                    InstanceStatus status,
                    Attributes attributes,
                    LeaderBookkeeping leaderBookkeeping,
                    ReplyData replyData) {
        this.replicaId = replicaId;
        this.instanceId = instanceId;
        this.command = command;
        this.ballot = ballot;
        this.status = status;
        this.attributes = attributes;
        this.leaderBookkeeping = leaderBookkeeping;
        this.replyData = replyData;
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

    public LeaderBookkeeping leaderBookkeeping() {
        return leaderBookkeeping;
    }

    public ReplyData replyData() {
        return replyData;
    }

    public void switchToRecoveryLeaderBookkeeping() {
        this.leaderBookkeeping = new LeaderBookkeeping(Map.of(), true);
    }

    public boolean isInitialBallot() {
        return ballot.number() == 0;
    }

    public boolean isNotInitialBallot() {
        return !isInitialBallot();
    }

}
