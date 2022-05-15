package edu.yuvyurchenko.jepaxos.maelstrom.impl;

import java.util.List;

import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;

public class PojoCluster implements Cluster {

    private final String currReplicaId;
    private final List<String> allReplicaIds;
    
    public PojoCluster(String currReplicaId, List<String> allReplicaIds) {
        this.currReplicaId = currReplicaId;
        this.allReplicaIds = allReplicaIds;
    }

    @Override
    public String getCurrReplicaId() {
        return currReplicaId;
    }

    @Override
    public List<String> getAllReplicaIds() {
        return allReplicaIds;
    }
    
}
