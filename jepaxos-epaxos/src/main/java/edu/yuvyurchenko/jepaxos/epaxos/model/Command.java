package edu.yuvyurchenko.jepaxos.epaxos.model;

public record Command(String operation, Object key, Object value, Object ifValue) {}
