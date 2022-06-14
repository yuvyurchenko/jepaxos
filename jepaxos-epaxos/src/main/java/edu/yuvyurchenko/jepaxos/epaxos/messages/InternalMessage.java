package edu.yuvyurchenko.jepaxos.epaxos.messages;

import java.util.Map;
import java.util.Objects;

import edu.yuvyurchenko.jepaxos.epaxos.model.Attributes;
import edu.yuvyurchenko.jepaxos.epaxos.model.Ballot;
import edu.yuvyurchenko.jepaxos.epaxos.model.Command;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;

public sealed interface InternalMessage extends NetworkMessage permits InternalMessage.PreAccept,
                                                                       InternalMessage.PreAcceptOK, 
                                                                       InternalMessage.PreAcceptReply,
                                                                       InternalMessage.Commit,
                                                                       InternalMessage.Accept, 
                                                                       InternalMessage.AcceptReply,
                                                                       InternalMessage.Prepare,
                                                                       InternalMessage.PrepareReply,
                                                                       InternalMessage.TryPreAccept,
                                                                       InternalMessage.TryPreAcceptReply {
    
    int instanceId();

    record PreAccept(String src, 
                     String dest,
                     MessageMetadata meta,
                     String leaderId,
                     String replicaId,
                     int instanceId,
                     Ballot ballot,
                     Command command,
                     Attributes attributes) implements InternalMessage {
        public boolean isNotFromCommandLeader() {
            return !Objects.equals(leaderId, replicaId);
        }
        public boolean isNotInitialBallot() {
            return ballot.number() != 0;
        }
    }

    record PreAcceptOK(String src, 
                       String dest, 
                       MessageMetadata meta, 
                       int instanceId) implements InternalMessage {}

    record PreAcceptReply(String src, 
                          String dest,
                          MessageMetadata meta, 
                          String replicaId, 
                          int instanceId,
                          boolean ok, 
                          Ballot ballot, 
                          Attributes attributes,
                          Map<String, Integer> committedDeps) implements InternalMessage {}

    record Commit(String src, 
                  String dest,
                  MessageMetadata meta,
                  String leaderId,
                  String replicaId,
                  int instanceId,
                  Command command,
                  Attributes attributes) implements InternalMessage {}
    
    record Accept(String src, 
                  String dest,
                  MessageMetadata meta,
                  String leaderId,
                  String replicaId,
                  int instanceId,
                  Ballot ballot,
                  Command command,
                  Attributes attributes) implements InternalMessage {}

    record AcceptReply(String src, 
                       String dest,
                       MessageMetadata meta,
                       String replicaId,
                       int instanceId,
                       boolean ok,
                       Ballot ballot) implements InternalMessage {}

    record Prepare(String src, 
                   String dest,
                   MessageMetadata meta,
                   String leaderId,
                   String replicaId,
                   int instanceId,
                   Ballot ballot) implements InternalMessage {}
    
    record PrepareReply(String src, 
                        String dest,
                        MessageMetadata meta,
                        String acceptorId,
                        String replicaId,
                        int instanceId,
                        boolean ok,
                        Ballot ballot,
                        InstanceStatus status,
                        Command command,
                        Attributes attributes) implements InternalMessage {}

    record TryPreAccept(String src, 
                        String dest,
                        MessageMetadata meta, 
                        String leaderId,
                        String replicaId,
                        int instanceId,
                        Ballot ballot,
                        Command command,
                        Attributes attributes) implements InternalMessage {}

    record TryPreAcceptReply(String src, 
                             String dest,
                             MessageMetadata meta,
                             String acceptorId,
                             String replicaId,
                             int instanceId,
                             boolean ok,
                             Ballot ballot,
                             String conflictReplicaId,
                             int conflictInstanceId,
                             InstanceStatus conflictStatus) implements InternalMessage {}
}
