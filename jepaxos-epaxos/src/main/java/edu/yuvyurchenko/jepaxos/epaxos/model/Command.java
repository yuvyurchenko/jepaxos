package edu.yuvyurchenko.jepaxos.epaxos.model;

public record Command(String operation, Object key, Object value, Object ifValue) {
    public Command(String operation, Object key) {
        this(operation, key, null, null);
    }
    public Command(String operation, Object key, Object value) {
        this(operation, key, value, null);
    }
}
