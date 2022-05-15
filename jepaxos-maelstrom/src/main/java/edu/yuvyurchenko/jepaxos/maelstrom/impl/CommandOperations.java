package edu.yuvyurchenko.jepaxos.maelstrom.impl;

import edu.yuvyurchenko.jepaxos.epaxos.model.Command;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.CommandOperation;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Storage;

public enum CommandOperations {
    GET("get", new CommandOperation() {
        public Object execute(Storage storage, Command command) throws Error {
            return storage.get(command.key());
        }
        public boolean isReadOnly() {
            return true;
        }
    }),
    PUT("put", new CommandOperation() {
        public Object execute(Storage storage, Command command) throws Error {
            storage.put(command.key(), command.value());
            return command.value();
        }
        public boolean isReadOnly() {
            return false;
        }
    }),
    CAS("cas", new CommandOperation() {
        public Object execute(Storage storage, Command command) throws Error {
            if (!storage.contains(command.key())) {
                throw new Error(20, "unknown key");
            }
            if (!storage.cas(command.key(), command.value(), command.ifValue())) {
                throw new Error(22, "wrong expected value");
            }
            return command.value();
        }
        public boolean isReadOnly() {
            return false;
        }
    });
    
    private final String id;
    private final CommandOperation operation;

    CommandOperations(String id, CommandOperation operation) {
        this.id = id;
        this.operation = operation;
    }

    public String id() {
        return id;
    }

    public CommandOperation operation() {
        return operation;
    }


}
