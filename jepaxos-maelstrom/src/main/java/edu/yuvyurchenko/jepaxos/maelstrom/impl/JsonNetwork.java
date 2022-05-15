package edu.yuvyurchenko.jepaxos.maelstrom.impl;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import edu.yuvyurchenko.jepaxos.epaxos.handlers.Handler;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.messages.MessageMetadata;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;
import edu.yuvyurchenko.jepaxos.epaxos.model.Command;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;
import edu.yuvyurchenko.jepaxos.maelstrom.Message;
import edu.yuvyurchenko.jepaxos.maelstrom.MessageType;

public class JsonNetwork implements Network {
    
    private static final String MSG_ID = "msg_id";
    private static final String RESP_TYPE = "resp_type";
    
    private final Consumer<Message> outChannel;
    private final AtomicInteger msgIdCounter;
    private final Map<Class<?>, Handler> handlerRegistry;

    public JsonNetwork(Consumer<Message> outChannel, 
                       AtomicInteger msgIdCounter) {
        this.outChannel = outChannel;
        this.msgIdCounter = msgIdCounter;
        this.handlerRegistry = new ConcurrentHashMap<>();
    }

    @Override
    public void send(NetworkMessage msg) {
        if (msg instanceof ReadReply r) {
            outChannel.accept(fromReadReply(r));
        } else if (msg instanceof RequestReply r) {
            outChannel.accept(fromRequestReply(r));
        } else if (msg instanceof Accept r) {
            outChannel.accept(fromAccept(r));
        } else if (msg instanceof AcceptReply r) {
            outChannel.accept(fromAcceptReply(r));
        } else if (msg instanceof Commit r) {
            outChannel.accept(fromCommit(r));
        } else if (msg instanceof PreAccept r) {
            outChannel.accept(fromPreAccept(r));
        } else if (msg instanceof PreAcceptOK r) {
            outChannel.accept(fromPreAcceptOK(r));
        } else if (msg instanceof PreAcceptReply r) {
            outChannel.accept(fromPreAcceptReply(r));
        } else if (msg instanceof Prepare r) {
            outChannel.accept(fromPrepare(r));
        } else if (msg instanceof PrepareReply r) {
            outChannel.accept(fromPrepareReply(r));
        } else if (msg instanceof TryPreAccept r) {
            outChannel.accept(fromTryPreAccept(r));
        } else if (msg instanceof TryPreAcceptReply r) {
            outChannel.accept(fromTryPreAcceptReply(r));
        }
    }
    
    @Override
    public <R extends NetworkMessage> void registerHandler(Class<R> requestType, Handler<R> handler) {
        handlerRegistry.put(requestType, handler);
    }

    public void receive(Message msg) {
        var type = msg.getType();
        NetworkMessage networkMessage = switch (type) {
            // external
            case read -> toRead(msg);
            case write -> toRequest(msg);
            case cas -> toRequest(msg);
            // internal
            case accept -> toAccept(msg);
            case accept_reply -> toAcceptReply(msg);
            case commit -> toCommit(msg);
            case preaccept -> toPreAccept(msg);
            case preaccept_ok -> toPreAcceptOK(msg);
            case preaccept_reply -> toPreAcceptReply(msg);
            case prepare -> toPrepare(msg);
            case prepare_reply -> toPrepareReply(msg);
            case trypreaccept -> toTryPreAccept(msg);
            case trypreaccept_reply -> toTryPreAcceptReply(msg);
            default -> throw new IllegalArgumentException("Unknown message type" + type);
        };
        var handler = handlerRegistry.get(networkMessage.getClass());
        if (handler == null) {
            throw new IllegalArgumentException(
                "Newtork message class without registered handler" 
                + networkMessage.getClass());
        }
        handler.handle(networkMessage);
    }

    private Read toRead(Message msg) {
        var meta = new MessageMetadata();
        meta.put(MSG_ID, msg.getMsgId());
        var command = new Command(CommandOperations.GET.name(), msg.getKey(), null, null);
        return new Read(msg.getSrc(), msg.getDest(), meta, command);
    }

    private Message fromReadReply(ReadReply reply) {
        int msgId = reply.meta().get(MSG_ID);
        var msg = new Message();
        msg.setSrc(reply.dest());
        msg.setDest(reply.src());
        msg.setInReplyTo(msgId);
        msg.setMsgId(msgIdCounter.getAndIncrement());
        if (reply.ok()) {
            msg.setType(MessageType.read_ok);
            msg.setValue(Objects.toString(reply.value(), null));
        } else {
            msg.setType(MessageType.error);
            msg.setCode(reply.errorCode());
            msg.setText(reply.errorText());
        }
        return msg;
    }

    private Request toRequest(Message msg) {
        var meta = new MessageMetadata();
        meta.put(MSG_ID, msg.getMsgId());
        Command command;
        if (msg.getType() == MessageType.write) {
            meta.put(RESP_TYPE, MessageType.write_ok);
            command = new Command(CommandOperations.PUT.name(), msg.getKey(), msg.getValue(), null);
        } else { // cas
            meta.put(RESP_TYPE, MessageType.cas_ok);
            command = new Command(CommandOperations.CAS.name(), msg.getKey(), msg.getTo(), msg.getFrom());
        }
        return new Request(msg.getSrc(), msg.getDest(), meta, command);
    }

    private Message fromRequestReply(RequestReply reply) {
        int msgId = reply.meta().get(MSG_ID);
        var msg = new Message();
        msg.setSrc(reply.dest());
        msg.setDest(reply.src());
        msg.setType(reply.meta().get(RESP_TYPE));
        msg.setInReplyTo(msgId);
        msg.setMsgId(msgIdCounter.getAndIncrement());
        if (reply.ok()) {
            msg.setType(reply.meta().get(RESP_TYPE));
        } else {
            msg.setType(MessageType.error);
            msg.setCode(reply.errorCode());
            msg.setText(reply.errorText());
        }
        return msg;
    }

    private Accept toAccept(Message msg) {
        return new Accept(msg.getSrc(), 
                          msg.getDest(),
                          toMeta(msg),
                          msg.getLeaderId(),
                          msg.getReplicaId(),
                          msg.getInstanceId(),
                          msg.getBallot(),
                          msg.getCommand(),
                          msg.getAttributes());
    }

    private Message fromAccept(Accept reply) {
        var msg = new Message();
        msg.setSrc(reply.src());
        msg.setDest(reply.dest());
        msg.setType(MessageType.accept);
        msg.setMsgId(msgIdCounter.getAndIncrement());
        msg.setLeaderId(reply.leaderId());
        msg.setReplicaId(reply.replicaId());
        msg.setInstanceId(reply.instanceId());
        msg.setBallot(reply.ballot());
        msg.setCommand(reply.command());
        msg.setAttributes(reply.attributes());
        return msg;
    }

    private AcceptReply toAcceptReply(Message msg) {
        return new AcceptReply(msg.getSrc(), 
                               msg.getDest(), 
                               toMeta(msg), 
                               msg.getReplicaId(), 
                               msg.getInstanceId(), 
                               msg.getOk(), 
                               msg.getBallot());
    }

    private Message fromAcceptReply(AcceptReply reply) {
        var msg = new Message();
        msg.setSrc(reply.src());
        msg.setDest(reply.dest());
        msg.setType(MessageType.accept_reply);
        msg.setMsgId(msgIdCounter.getAndIncrement());
        msg.setInReplyTo(reply.meta().get(MSG_ID));
        msg.setReplicaId(reply.replicaId());
        msg.setInstanceId(reply.instanceId());
        msg.setOk(reply.ok());
        msg.setBallot(reply.ballot());
        return msg;
    }

    private Commit toCommit(Message msg) {
        return new Commit(msg.getSrc(), 
                          msg.getDest(), 
                          toMeta(msg), 
                          msg.getLeaderId(), 
                          msg.getReplicaId(), 
                          msg.getInstanceId(), 
                          msg.getCommand(), 
                          msg.getAttributes());
    }

    private Message fromCommit(Commit reply) {
        var msg = new Message();
        msg.setSrc(reply.src());
        msg.setDest(reply.dest());
        msg.setType(MessageType.commit);
        msg.setMsgId(msgIdCounter.getAndIncrement());
        msg.setLeaderId(reply.leaderId());
        msg.setReplicaId(reply.replicaId());
        msg.setInstanceId(reply.instanceId());
        msg.setCommand(reply.command());
        msg.setAttributes(reply.attributes());
        return msg;
    }

    private PreAccept toPreAccept(Message msg) {
        return new PreAccept(msg.getSrc(), 
                             msg.getDest(), 
                             toMeta(msg), 
                             msg.getLeaderId(), 
                             msg.getReplicaId(), 
                             msg.getInstanceId(), 
                             msg.getBallot(), 
                             msg.getCommand(), 
                             msg.getAttributes());
    }

    private Message fromPreAccept(PreAccept reply) {
        var msg = new Message();
        msg.setSrc(reply.src());
        msg.setDest(reply.dest());
        msg.setType(MessageType.preaccept);
        msg.setMsgId(msgIdCounter.getAndIncrement());
        msg.setLeaderId(reply.leaderId());
        msg.setReplicaId(reply.replicaId());
        msg.setInstanceId(reply.instanceId());
        msg.setBallot(reply.ballot());
        msg.setCommand(reply.command());
        msg.setAttributes(reply.attributes());
        return msg;
    }

    private PreAcceptOK toPreAcceptOK(Message msg) {
        return new PreAcceptOK(msg.getSrc(), 
                               msg.getDest(), 
                               toMeta(msg), 
                               msg.getInstanceId());
    }

    private Message fromPreAcceptOK(PreAcceptOK reply) {
        var msg = new Message();
        msg.setSrc(reply.src());
        msg.setDest(reply.dest());
        msg.setType(MessageType.preaccept_ok);
        msg.setMsgId(msgIdCounter.getAndIncrement());
        msg.setInReplyTo(reply.meta().get(MSG_ID));
        msg.setInstanceId(reply.instanceId());
        return msg;
    }

    private PreAcceptReply toPreAcceptReply(Message msg) {
        return new PreAcceptReply(msg.getSrc(), 
                                  msg.getDest(), 
                                  toMeta(msg), 
                                  msg.getReplicaId(), 
                                  msg.getInstanceId(), 
                                  msg.getOk(), 
                                  msg.getBallot(), 
                                  msg.getAttributes(), 
                                  msg.getCommittedDeps());
    }

    private Message fromPreAcceptReply(PreAcceptReply reply) {
        var msg = new Message();
        msg.setSrc(reply.src());
        msg.setDest(reply.dest());
        msg.setType(MessageType.preaccept_reply);
        msg.setMsgId(msgIdCounter.getAndIncrement());
        msg.setInReplyTo(reply.meta().get(MSG_ID));
        msg.setReplicaId(reply.replicaId());
        msg.setInstanceId(reply.instanceId());
        msg.setOk(reply.ok());
        msg.setBallot(reply.ballot());
        msg.setAttributes(reply.attributes());
        msg.setCommittedDeps(reply.committedDeps());
        return msg;
    }

    private Prepare toPrepare(Message msg) {
        return new Prepare(msg.getSrc(), 
                           msg.getDest(), 
                           toMeta(msg), 
                           msg.getLeaderId(), 
                           msg.getReplicaId(), 
                           msg.getInstanceId(), 
                           msg.getBallot());
    }

    private Message fromPrepare(Prepare reply) {
        var msg = new Message();
        msg.setSrc(reply.src());
        msg.setDest(reply.dest());
        msg.setType(MessageType.prepare);
        msg.setMsgId(msgIdCounter.getAndIncrement());
        msg.setLeaderId(reply.leaderId());
        msg.setReplicaId(reply.replicaId());
        msg.setInstanceId(reply.instanceId());
        msg.setBallot(reply.ballot());
        return msg;
    }

    private PrepareReply toPrepareReply(Message msg) {
        return new PrepareReply(msg.getSrc(), 
                                msg.getDest(), 
                                toMeta(msg), 
                                msg.getAcceptorId(), 
                                msg.getReplicaId(), 
                                msg.getInstanceId(), 
                                msg.getOk(), 
                                msg.getBallot(), 
                                msg.getInstanceStatus(), 
                                msg.getCommand(), 
                                msg.getAttributes());
    }

    private Message fromPrepareReply(PrepareReply reply) {
        var msg = new Message();
        msg.setSrc(reply.src());
        msg.setDest(reply.dest());
        msg.setType(MessageType.prepare_reply);
        msg.setMsgId(msgIdCounter.getAndIncrement());
        msg.setInReplyTo(reply.meta().get(MSG_ID));
        msg.setAcceptorId(reply.acceptorId());
        msg.setReplicaId(reply.replicaId());
        msg.setInstanceId(reply.instanceId());
        msg.setOk(reply.ok());
        msg.setBallot(reply.ballot());
        msg.setInstanceStatus(reply.status());
        msg.setCommand(reply.command());
        msg.setAttributes(reply.attributes());
        return msg;
    }

    private TryPreAccept toTryPreAccept(Message msg) {
        return new TryPreAccept(msg.getSrc(), 
                                msg.getDest(), 
                                toMeta(msg), 
                                msg.getLeaderId(), 
                                msg.getReplicaId(), 
                                msg.getInstanceId(), 
                                msg.getBallot(), 
                                msg.getCommand(), 
                                msg.getAttributes());
    }

    private Message fromTryPreAccept(TryPreAccept reply) {
        var msg = new Message();
        msg.setSrc(reply.src());
        msg.setDest(reply.dest());
        msg.setType(MessageType.trypreaccept);
        msg.setMsgId(msgIdCounter.getAndIncrement());
        msg.setLeaderId(reply.leaderId());
        msg.setReplicaId(reply.replicaId());
        msg.setInstanceId(reply.instanceId());
        msg.setBallot(reply.ballot());
        msg.setCommand(reply.command());
        msg.setAttributes(reply.attributes());
        return msg;
    }

    private TryPreAcceptReply toTryPreAcceptReply(Message msg) {
        return new TryPreAcceptReply(msg.getSrc(), 
                                     msg.getDest(), 
                                     toMeta(msg), 
                                     msg.getAcceptorId(), 
                                     msg.getReplicaId(), 
                                     msg.getInstanceId(), 
                                     msg.getOk(), 
                                     msg.getBallot(), 
                                     msg.getConflictReplicaId(), 
                                     msg.getConflictInstanceId(), 
                                     msg.getConflictInstanceStatus());
    }

    private Message fromTryPreAcceptReply(TryPreAcceptReply reply) {
        var msg = new Message();
        msg.setSrc(reply.src());
        msg.setDest(reply.dest());
        msg.setType(MessageType.trypreaccept_reply);
        msg.setMsgId(msgIdCounter.getAndIncrement());
        msg.setInReplyTo(reply.meta().get(MSG_ID));
        msg.setAcceptorId(reply.acceptorId());
        msg.setReplicaId(reply.replicaId());
        msg.setInstanceId(reply.instanceId());
        msg.setOk(reply.ok());
        msg.setBallot(reply.ballot());
        msg.setConflictReplicaId(reply.conflictReplicaId());
        msg.setConflictInstanceId(reply.conflictInstanceId());
        msg.setConflictInstanceStatus(reply.conflictStatus());
        return msg;
    }

    private MessageMetadata toMeta(Message msg) {
        var meta = new MessageMetadata();
        meta.put(MSG_ID, msg.getMsgId());
        return meta;
    }

}
