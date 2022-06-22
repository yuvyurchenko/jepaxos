package edu.yuvyurchenko.jepaxos.epaxos.plugins;

import java.util.List;

/**
 * Provides information about the current cluster topology
 */
public interface Cluster {
    /**
     * Returns the current replica id
     * @return replica id
     */
    String getCurrReplicaId();
    /**
     * Returns the list of all replica ids in the cluster 
     * including the current replica id
     * @return listo of replica ids
     */
    List<String> getAllReplicaIds();
}
