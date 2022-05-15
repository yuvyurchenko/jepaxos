package edu.yuvyurchenko.jepaxos.epaxos.model;

import java.util.Map;

public record Attributes(int seq, Map<String, Integer> deps) {}
