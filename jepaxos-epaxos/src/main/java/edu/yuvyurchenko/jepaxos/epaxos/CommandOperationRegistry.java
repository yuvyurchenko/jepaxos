package edu.yuvyurchenko.jepaxos.epaxos;

import java.util.HashMap;
import java.util.Map;

import edu.yuvyurchenko.jepaxos.epaxos.model.Command;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.CommandOperation;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Storage;

public class CommandOperationRegistry {
    private final Map<String, CommandOperation> operationRegistry;

    CommandOperationRegistry(Map<String, CommandOperation> operationRegistry) {
        var tmpMap = new HashMap<String, CommandOperation>(operationRegistry);
        tmpMap.put(Command.NOOP.operation(), new CommandOperation() {
            public Object execute(Storage storage, Command command) { return null; }
            public boolean isReadOnly() { return true; }
        });
        this.operationRegistry = Map.copyOf(tmpMap);
    }

    public CommandOperation getOperation(String commandOperation) {
        return operationRegistry.get(commandOperation);
    }

}