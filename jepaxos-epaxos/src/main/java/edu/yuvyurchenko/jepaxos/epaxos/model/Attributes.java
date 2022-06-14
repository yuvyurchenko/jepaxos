package edu.yuvyurchenko.jepaxos.epaxos.model;

import java.util.Map;

public record Attributes(int seq, Map<String, Integer> deps) {
    public int dep(String replicaId) {
        return deps.getOrDefault(replicaId, -1);
    }
}
