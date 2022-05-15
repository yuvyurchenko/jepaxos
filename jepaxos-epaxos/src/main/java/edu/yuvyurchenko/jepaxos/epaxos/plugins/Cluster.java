package edu.yuvyurchenko.jepaxos.epaxos.plugins;

import java.util.List;

public interface Cluster {
    String getCurrReplicaId();
    List<String> getAllReplicaIds();
}
