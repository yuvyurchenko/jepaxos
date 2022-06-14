package edu.yuvyurchenko.jepaxos.epaxos.messages;

import java.util.HashMap;
import java.util.Map;

public class MessageMetadata {
    private final Map<String, Object> ctx = new HashMap<>();

    public <T> T get(String name) {
        return (T) ctx.get(name);
    }

    public void put(String name, Object value) {
        ctx.put(name, value);
    }

    @Override
    public String toString() {
        return "MessageMetadata [" + ctx + "]";
    }
    
}
