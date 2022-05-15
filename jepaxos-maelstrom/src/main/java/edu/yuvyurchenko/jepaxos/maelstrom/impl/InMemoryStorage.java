package edu.yuvyurchenko.jepaxos.maelstrom.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.yuvyurchenko.jepaxos.epaxos.plugins.Storage;

public class InMemoryStorage implements Storage {
    private final Map<Object, Object> map = new ConcurrentHashMap<>();

    @Override
    public Object get(Object key) {
        return map.get(key);
    }

    @Override
    public void put(Object key, Object value) {
        map.put(key, value);
    }

    @Override
    public void remove(Object key) {
        map.remove(key);
    }

    @Override
    public boolean cas(Object key, Object value, Object ifValue) {
        return map.replace(key, ifValue, value);
    }

    @Override
    public boolean contains(Object key) {
        return map.containsKey(key);
    }

}
