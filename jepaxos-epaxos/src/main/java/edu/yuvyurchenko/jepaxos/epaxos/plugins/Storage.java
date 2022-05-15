package edu.yuvyurchenko.jepaxos.epaxos.plugins;

public interface Storage {
    Object get(Object key);
    void put(Object key, Object value);
    void remove(Object key);
    boolean cas(Object key, Object value, Object ifValue);
    boolean contains(Object key);
}
