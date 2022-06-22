package edu.yuvyurchenko.jepaxos.epaxos.plugins;

/**
 * Key-value storage API for replica-local implementation.
 * In a nutshell JEPaxos main gal is to ensures
 * the same sequence of Storage API calls on each replica in the cluster
 */
public interface Storage {
    /**
     * Returns the current value by a key.
     * @param key 
     * @return the current value or null if the storage has no assiciated value with the storage 
     */
    Object get(Object key);
    /**
     * Puts the new value for the key blindly overwriting the previous value if existed.
     * @param key
     * @param value
     */
    void put(Object key, Object value);
    /**
     * Deletes the value for the key if existed
     * @param key
     */
    void remove(Object key);
    /**
     * Conditionally overwrites the value for the given key if the current value equals ifValue.
     * @param key
     * @param value
     * @param ifValue
     * @return true if operation succeeded and false otherwise 
     */
    boolean cas(Object key, Object value, Object ifValue);
    /**
     * Verifies if the key has a mapping to any value in the storage 
     * @param key
     * @return returns true if the storage has a value for the key and false otherwise
     */
    boolean contains(Object key);
}
