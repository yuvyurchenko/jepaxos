package edu.yuvyurchenko.jepaxos.maelstrom.impl;

import java.util.List;

import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;

public record PojoCluster(String getCurrReplicaId, List<String> getAllReplicaIds) implements Cluster {}
