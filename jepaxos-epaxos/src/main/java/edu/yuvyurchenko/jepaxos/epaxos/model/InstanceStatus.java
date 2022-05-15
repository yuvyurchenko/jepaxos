package edu.yuvyurchenko.jepaxos.epaxos.model;

public enum InstanceStatus {
    NONE,
	PREACCEPTED,
	PREACCEPTED_EQ,
	ACCEPTED,
	COMMITTED,
	EXECUTED
}
