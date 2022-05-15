package edu.yuvyurchenko.jepaxos.maelstrom;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.yuvyurchenko.jepaxos.epaxos.model.Attributes;
import edu.yuvyurchenko.jepaxos.epaxos.model.Ballot;
import edu.yuvyurchenko.jepaxos.epaxos.model.Command;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;

public class Message {
    private final ObjectNode root;
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    public Message(ObjectNode root) {
        this.root = root;
    }

    public Message(JsonNode root) {
        this.root = (ObjectNode) root;
    }

    public Message() {
        root = JsonNodeFactory.instance.objectNode();
    }

    public String getSrc() {
        return root.get("src").asText();
    }

    public void setSrc(String src) {
        root.put("src", src);
    }

    public String getDest() {
        return root.get("dest").asText();
    }

    public void setDest(String dest) {
        root.put("dest", dest);
    }

    public MessageType getType() {
        return MessageType.valueOf(getBody().get("type").asText());
    }

    public void setType(MessageType type) {
        getBody().put("type", type.name());
    }

    public int getMsgId() {
        return getBody().get("msg_id").asInt();
    }

    public void setMsgId(int msgId) {
        getBody().put("msg_id", msgId);
    }

    public int getInReplyTo() {
        return getBody().get("in_reply_to").asInt();
    }

    public void setInReplyTo(int inReplyTo) {
        getBody().put("in_reply_to", inReplyTo);
    }

    public int getCode() {
        return getBody().get("code").asInt();
    }

    public void setCode(int code) {
        getBody().put("code", code);
    }

    public String getText() {
        return getBody().get("text").asText();
    }

    public void setText(String text) {
        getBody().put("text", text);
    }

    public String getKey() {
        return getBody().get("key").asText();
    }

    public void setKey(String key) {
        getBody().put("key", key);
    }

    public String getValue() {
        return getBody().get("value").asText();
    }

    public void setValue(String value) {
        getBody().put("value", value);
    }

    public String getFrom() {
        return getBody().get("from").asText();
    }

    public void setFrom(String from) {
        getBody().put("from", from);
    }

    public String getTo() {
        return getBody().get("to").asText();
    }

    public void setTo(String to) {
        getBody().put("to", to);
    }

    public String getNodeId() {
        return getBody().get("node_id").asText();
    }

    public List<String> getNodeIds() {
        var allReplicaIds = new ArrayList<String>();
        var nodeIds = getBody().get("node_ids");
        for (var e : nodeIds) {
            allReplicaIds.add(e.asText());
        }
        return allReplicaIds;
    }

    public String getLeaderId() {
        return getBody().get("leader_id").asText();
    }

    public void setLeaderId(String leaderId) {
        getBody().put("leader_id", leaderId);
    }

    public String getReplicaId() {
        return getBody().get("replica_id").asText();
    }

    public void setReplicaId(String replicaId) {
        getBody().put("replica_id", replicaId);
    }

    public int getInstanceId() {
        return getBody().get("instance_id").asInt();
    }

    public void setInstanceId(int instanceId) {
        getBody().put("instance_id", instanceId);
    }

    public boolean getOk() {
        return getBody().get("ok").asBoolean();
    }

    public void setOk(boolean ok) {
        getBody().put("ok", ok);
    }

    public String getAcceptorId() {
        return getBody().get("acceptor_id").asText();
    }

    public void setAcceptorId(String acceptorId) {
        getBody().put("acceptor_id", acceptorId);
    }

    public String getConflictReplicaId() {
        return getBody().get("conflict_replica_id").asText();
    }

    public void setConflictReplicaId(String conflictReplicaId) {
        getBody().put("conflict_replica_id", conflictReplicaId);
    }

    public int getConflictInstanceId() {
        return getBody().get("conflict_instance_id").asInt();
    }

    public void setConflictInstanceId(int conflictInstanceId) {
        getBody().put("conflict_instance_id", conflictInstanceId);
    }

    public InstanceStatus getConflictInstanceStatus() {
        return InstanceStatus.valueOf(getBody().get("conflict_instance_status").asText());
    }

    public void setConflictInstanceStatus(InstanceStatus conflictInstanceStatus) {
        getBody().put("conflict_instance_status", conflictInstanceStatus.name());
    }

    public Ballot getBallot() {
        var b = getBody().get("ballot");
        try {
            return objectMapper.treeToValue(b, Ballot.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        // if (b == null) {
        //     return null;
        // }
        // return new Ballot(b.get("number").asInt(), b.get("replicaId").asText());
    }

    public void setBallot(Ballot ballot) {
        var b = objectMapper.valueToTree(ballot);
        getBody().set("ballot", b);
        // var b = getBody().putObject("ballot");
        // b.put("number", ballot.number());
        // b.put("replicaId", ballot.replicaId());
    }

    public Command getCommand() {
        var c = getBody().get("command");
        try {
            return objectMapper.treeToValue(c, Command.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // var c = (ObjectNode) getBody().get("command");
        // var op = CommandOperation.valueOf(c.get("operation").asText());
        // var key = c.get("key").asText();
        // var val = Optional.ofNullable(c.get("value")).map(JsonNode::asText).orElse(null);
        // var ifVal = Optional.ofNullable(c.get("ifValue")).map(JsonNode::asText).orElse(null);
        // return new Command(op, key, val, ifVal);
    }

    public void setCommand(Command command) {
        var c = objectMapper.valueToTree(command);
        getBody().set("command", c);
        // var c = getBody().putObject("command");
        // c.put("operation", command.operation().name());
        // c.put("key", (String) command.key());
        // c.put("value", (String) command.value());
        // c.put("ifValue", (String) command.ifValue());
    }

    public Attributes getAttributes() {
        var a = getBody().get("attributes");
        try {
            return objectMapper.treeToValue(a, Attributes.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setAttributes(Attributes attributes) {
        var a = objectMapper.valueToTree(attributes);
        getBody().set("attributes", a);
    }

    public InstanceStatus getInstanceStatus() {
        return InstanceStatus.valueOf(getBody().get("instance_status").asText());
    }

    public void setInstanceStatus(InstanceStatus instanceStatus) {
        getBody().put("instance_status", instanceStatus.name());
    }

    public Map<String, Integer> getCommittedDeps() {
        var cd = getBody().get("committed_deps");
        try {
            return objectMapper.treeToValue(cd, Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setCommittedDeps(Map<String, Integer> committedDeps) {
        var cd = objectMapper.valueToTree(committedDeps);
        getBody().set("committed_deps", cd);
    }

    public ObjectNode toObjectNode() {
        return root;
    }

    private ObjectNode getBody() {
        var body = root.get("body");
        if (body == null) {
            return root.putObject("body");
        }
        return (ObjectNode) body;
    }

}
