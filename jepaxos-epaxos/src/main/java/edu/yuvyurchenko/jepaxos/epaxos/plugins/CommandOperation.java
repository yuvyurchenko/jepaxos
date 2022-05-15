package edu.yuvyurchenko.jepaxos.epaxos.plugins;

import edu.yuvyurchenko.jepaxos.epaxos.model.Command;

public interface CommandOperation {
    
    Object execute(Storage storage, Command command) throws Error;

    boolean isReadOnly();

    public static class Error extends Exception {
        private final int code;
        private final String text;
        
        public Error(int code, String text) {
            this.code = code;
            this.text = text;
        }        

        public int code() {
            return code;
        }

        public String text() {
            return text;
        }

        @Override
        public String toString() {
            return "Error [code=" + code + ", text=" + text + "]";
        }
        
    }
}
