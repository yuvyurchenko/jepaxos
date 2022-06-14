package edu.yuvyurchenko.jepaxos.epaxos;

import java.util.Map;

import edu.yuvyurchenko.jepaxos.epaxos.plugins.CommandOperation;

public class CommandOperationRegistry {
    private final Map<String, CommandOperation> operationRegistry;

    CommandOperationRegistry(Map<String, CommandOperation> operationRegistry) {
        this.operationRegistry = operationRegistry;
    }

    public CommandOperation getOperation(String commandOperation) {
        return operationRegistry.get(commandOperation);
    }

}