package edu.yuvyurchenko.jepaxos.epaxos.plugins;

import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage;
import edu.yuvyurchenko.jepaxos.epaxos.model.Command;

/**
 * This is API for operations associated with a particular {@link Command}
 * by {@link Command#operation()} value.
 * In other words all {@link Command} having the same {@link Command#operation()}
 * are mapped to the same {@link CommandOperation} implementation.
 */
public interface CommandOperation {
    /**
     * Applies parameters from the command to the storage.
     * Output of the method is used by JEpaxos 
     * to form {@link ExternalMessage.ReadReply} and {@link ExternalMessage.RequestAndReadReply} messages.
     * {@link ExternalMessage.RequestReply} does not care about the execution results.
     * @param storage
     * @param command
     * @return output for a sunny-day execution scenario 
     * @throws Error output for a rainy-day execution scenario
     */
    Object execute(Storage storage, Command command) throws Error;

    /**
     * Marks operation as read only which is important for JEPaxos during the dependency analysys phase.
     * @return true if the operation only reads from the storage
     */
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
