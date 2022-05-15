package edu.yuvyurchenko.jepaxos.epaxos.model;

public class RecoveryInstance {
    private final Command command;
    private InstanceStatus status;
    private Attributes attributes;
    private int preAcceptCount;
    private boolean leaderResponded;

    RecoveryInstance(Command command,
                     InstanceStatus status,
                     Attributes attributes,
                     int preAcceptCount,
                     boolean leaderResponded) {
        this.command = command;
        this.status = status;
        this.attributes = attributes;
        this.preAcceptCount = preAcceptCount;
        this.leaderResponded = leaderResponded;
    }

    public Command getCommand() {
        return command;
    }

    public InstanceStatus getStatus() {
        return status;
    }

    public Attributes getAttributes() {
        return attributes;
    }

    public void incPreAcceptCount() {
        preAcceptCount++;
    }

    public void setLeaderResponded(boolean leaderResponded) {
        this.leaderResponded = leaderResponded;
    }

    public int getPreAcceptCount() {
        return preAcceptCount;
    }

    public boolean getLeaderResponded() {
        return leaderResponded;
    }    
}
