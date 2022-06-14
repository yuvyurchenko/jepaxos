package edu.yuvyurchenko.jepaxos.epaxos;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.Read;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.ReadReply;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.Request;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.RequestAndRead;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.RequestAndReadReply;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.RequestReply;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.Commit;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.PreAccept;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.PreAcceptOK;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.PreAcceptReply;
import edu.yuvyurchenko.jepaxos.epaxos.messages.NetworkMessage;
import edu.yuvyurchenko.jepaxos.epaxos.model.Command;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.CommandOperation;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Storage;
import edu.yuvyurchenko.jepaxos.maelstrom.impl.CurrentThreadExecutingDriver;
import edu.yuvyurchenko.jepaxos.maelstrom.impl.InMemoryStorage;
import edu.yuvyurchenko.jepaxos.maelstrom.impl.LocalNetwork;
import edu.yuvyurchenko.jepaxos.maelstrom.impl.PojoCluster;

public class ProtocolTest {

    private static final String CLIENT_ID_1 = "Client 1";
    private static final String CLIENT_ID_2 = "Client 2";
    private static final String REPL_ID_1 = "Replica 1";
    private static final String REPL_ID_2 = "Replica 2";
    private static final String REPL_ID_3 = "Replica 3";

    private static final List<String> ALL_REPL_IDS = List.of(REPL_ID_1, REPL_ID_2, REPL_ID_3);

    private static final String GET_OP_ID = "get";
    private static final String PUT_OP_ID = "put";
    private static final String CAS_OP_ID = "cas";
    
    private static final Map<String, CommandOperation> OPERATIONS = Map.of(
        GET_OP_ID, new CommandOperation() {
            public Object execute(Storage storage, Command command) throws Error {
                return storage.get(command.key());
            }
            public boolean isReadOnly() {
                return true;
            }
        },
        PUT_OP_ID, new CommandOperation() {
            public Object execute(Storage storage, Command command) throws Error {
                storage.put(command.key(), command.value());
                return command.value();
            }
            public boolean isReadOnly() {
                return false;
            } 
        },
        CAS_OP_ID, new CommandOperation() {
            public Object execute(Storage storage, Command command) throws Error {
                if (storage.cas(command.key(), command.value(), command.ifValue())) {
                    return command.value();
                }
                return storage.get(command.key());
            }
            public boolean isReadOnly() {
                return false;
            }
        });

    private Map<String, LocalNetwork> networks;
    private Map<String, CurrentThreadExecutingDriver> drivers;
    private Map<String, InMemoryStorage> storages;
    private Map<String, Replica> replicas;

    @BeforeMethod
    public void initReplicas() {
        networks = ALL_REPL_IDS.stream().collect(Collectors.toMap(rId -> rId, rId -> new LocalNetwork()));
        drivers = ALL_REPL_IDS.stream().collect(Collectors.toMap(rId -> rId, rId -> new CurrentThreadExecutingDriver()));
        storages = ALL_REPL_IDS.stream().collect(Collectors.toMap(rId -> rId, rId -> new InMemoryStorage()));
        replicas = ALL_REPL_IDS.stream().collect(Collectors.toMap(rId -> rId, rId -> {
            var b = new ReplicaBuilder().withCluster(new PojoCluster(rId, ALL_REPL_IDS))
                                        .withStorage(storages.get(rId))
                                        .withNetwork(networks.get(rId))
                                        .withCustomExecutingDriver(drivers.get(rId))
                                        .withCommitGracePeriodMs(0)
                                        .withWaitCommitPeriodMs(0)
                                        .withMaxWaitCommitTries(0);
            OPERATIONS.forEach((k, v) -> b.withCommandOparetion(k, v));
            var r = b.build();
            r.start();
            return r;
        }));
    }

    @Test
    public void fastPathSunnDayTest() {
        var request = new Request(CLIENT_ID_1, REPL_ID_1, null, new Command(PUT_OP_ID, "key1", "value1"));
        var requestResponse = fastPath(request);
        assertEquals(requestResponse.getClass(), RequestReply.class);
        var requestReply = (RequestReply) requestResponse;
        assertEquals(requestReply.dest(), CLIENT_ID_1);
        assertEquals(requestReply.ok(), true);
        drivers.values().forEach(CurrentThreadExecutingDriver::executeScheduled);
        storages.values().forEach(s -> assertEquals(s.get("key1"), "value1"));
        
        var read = new Read(CLIENT_ID_1, REPL_ID_1, null, new Command(GET_OP_ID, "key1"));
        var readResponse = fastPath(read);
        assertEquals(readResponse.getClass(), ReadReply.class);
        var readReply = (ReadReply) readResponse;
        assertEquals(readReply.dest(), CLIENT_ID_1);
        assertEquals(readReply.ok(), true);
        assertEquals(readReply.value(), "value1");

        storages.values().forEach(s -> assertEquals(s.get("key1"), "value1"));

        var cas = new RequestAndRead(CLIENT_ID_1, REPL_ID_1, null, new Command(CAS_OP_ID, "key1", "value2", "value1"));
        var casResponse = fastPath(cas);
        assertEquals(casResponse.getClass(), RequestAndReadReply.class);
        var casReply = (RequestAndReadReply) casResponse;
        assertEquals(casReply.dest(), CLIENT_ID_1);
        assertEquals(casReply.ok(), true);
        assertEquals(casReply.value(), "value2");
    }

    @Test
    public void fastPathLeaderChangeTest() {
        var request1 = new Request(CLIENT_ID_1, REPL_ID_1, null, new Command(PUT_OP_ID, "key1", "value1"));
        var requestResponse1 = fastPath(request1);
        assertEquals(requestResponse1.getClass(), RequestReply.class);
        var requestReply1 = (RequestReply) requestResponse1;
        assertEquals(requestReply1.dest(), CLIENT_ID_1);
        assertEquals(requestReply1.ok(), true);
        drivers.values().forEach(CurrentThreadExecutingDriver::executeScheduled);
        storages.values().forEach(s -> assertEquals(s.get("key1"), "value1"));

        var request2 = new Request(CLIENT_ID_1, REPL_ID_2, null, new Command(PUT_OP_ID, "key1", "value2"));
        var requestResponse2 = fastPath(request2);
        assertEquals(requestResponse2.getClass(), RequestReply.class);
        var requestReply2 = (RequestReply) requestResponse2;
        assertEquals(requestReply2.dest(), CLIENT_ID_1);
        assertEquals(requestReply2.ok(), true);
        drivers.values().forEach(CurrentThreadExecutingDriver::executeScheduled);
        storages.values().forEach(s -> assertEquals(s.get("key1"), "value2"));
    }

    @Test
    public void concurrentWritesTest() {
        var writeReq1 = new Request(CLIENT_ID_1, REPL_ID_1, null, new Command(PUT_OP_ID, "key1", "value1"));
        var writeReq2 = new Request(CLIENT_ID_2, REPL_ID_2, null, new Command(PUT_OP_ID, "key1", "value2"));

        net(REPL_ID_1).receive(writeReq1);
        var preAccepts1 = drainByDestination(REPL_ID_1);
        net(REPL_ID_3).receive(preAccepts1.get(REPL_ID_3));
        net(REPL_ID_2).receive(writeReq2);
        net(REPL_ID_2).receive(preAccepts1.get(REPL_ID_2));
        
        var preAcceptOk_1_3 = net(REPL_ID_3).pollMessageQueue();
        net(REPL_ID_1).receive(preAcceptOk_1_3);
        var writeResps1 = drainByType(REPL_ID_1);
        assertNotNull(writeResps1.get(RequestReply.class));
        writeResps1.get(Commit.class).forEach(c -> {
            net(c.dest()).receive(c);
        });

        var preAccepts2 = drainByType(REPL_ID_2);
        var preAcceptReplyDelayed = preAccepts2.get(PreAcceptReply.class).get(0);
        
        net(preAcceptReplyDelayed.dest()).receive(preAcceptReplyDelayed);
        assertEquals(net(REPL_ID_1).messageQueueSize(), 0);

        preAccepts2.get(PreAccept.class).forEach(pa -> net(pa.dest()).receive(pa));
        net(REPL_ID_2).receive(net(REPL_ID_1).pollMessageQueue());
        net(REPL_ID_2).receive(net(REPL_ID_3).pollMessageQueue());
        
        net(REPL_ID_2).drainMessageQueue().forEach(pa -> net(pa.dest()).receive(pa));
        net(REPL_ID_2).receive(net(REPL_ID_1).pollMessageQueue());
        net(REPL_ID_2).receive(net(REPL_ID_3).pollMessageQueue());
        
        var writeResps2 = drainByType(REPL_ID_2);
        assertNotNull(writeResps2.get(RequestReply.class));
        writeResps2.get(Commit.class).forEach(c -> {
            net(c.dest()).receive(c);
        });

        drivers.values().forEach(CurrentThreadExecutingDriver::executeScheduled);
        storages.values().forEach(s -> assertEquals(s.get("key1"), "value2"));
    }

    @Test
    public void networkPartitioningTest() {
        var writeReq = new Request(CLIENT_ID_1, REPL_ID_1, null, new Command(PUT_OP_ID, "key1", "value1"));
        net(REPL_ID_1).receive(writeReq);
        var preAccepts = drainByDestination(REPL_ID_1);
        net(REPL_ID_2).receive(preAccepts.get(REPL_ID_2));
        net(REPL_ID_1).receive(net(REPL_ID_2).pollMessageQueue());
        var writeResps = drainByType(REPL_ID_1);
        assertNotNull(writeResps.get(RequestReply.class));
        writeResps.get(Commit.class).stream().filter(c -> c.dest().equals(REPL_ID_2)).findAny().ifPresent(c -> net(c.dest()).receive(c));
        drivers.get(REPL_ID_1).executeScheduled();
        drivers.get(REPL_ID_2).executeScheduled();
        assertEquals(storages.get(REPL_ID_1).get("key1"), "value1");
        assertEquals(storages.get(REPL_ID_2).get("key1"), "value1");

        var read = new Read(CLIENT_ID_1, REPL_ID_3, null, new Command(GET_OP_ID, "key1"));
        net(REPL_ID_3).receive(read);
        // PreAccept
        net(REPL_ID_3).drainMessageQueue().forEach(m -> net(m.dest()).receive(m));
        var preAcceptReply1 = net(REPL_ID_1).pollMessageQueue();
        var preAcceptReply2 = net(REPL_ID_2).pollMessageQueue();
        net(REPL_ID_3).receive(preAcceptReply1);
        net(REPL_ID_3).receive(preAcceptReply2);
        // Accept
        net(REPL_ID_3).drainMessageQueue().forEach(m -> net(m.dest()).receive(m));
        var acceptReply1 = net(REPL_ID_1).pollMessageQueue();
        var acceptReply2 = net(REPL_ID_2).pollMessageQueue();
        net(REPL_ID_3).receive(acceptReply1);
        net(REPL_ID_3).receive(acceptReply2);
        // Commit
        net(REPL_ID_3).drainMessageQueue().forEach(m -> net(m.dest()).receive(m));
        assertEquals(net(REPL_ID_1).messageQueueSize(), 0);
        assertEquals(net(REPL_ID_2).messageQueueSize(), 0);
        // drivers.values().forEach(CurrentThreadExecutingDriver::executeScheduled);

        drivers.get(REPL_ID_3).executeScheduled();

        net(REPL_ID_3).drainMessageQueue().forEach(m -> net(m.dest()).receive(m));
        var preapreReply1 = net(REPL_ID_1).pollMessageQueue();
        var preapreReply2 = net(REPL_ID_2).pollMessageQueue();
        net(REPL_ID_3).receive(preapreReply1);
        net(REPL_ID_3).receive(preapreReply2);

        net(REPL_ID_3).drainMessageQueue().forEach(m -> {
            assertEquals(m.getClass(), Commit.class);
            net(m.dest()).receive(m);
        });
        assertEquals(net(REPL_ID_1).messageQueueSize(), 0);
        assertEquals(net(REPL_ID_2).messageQueueSize(), 0);

        drivers.get(REPL_ID_3).executeScheduled();

        var m = net(REPL_ID_3).pollMessageQueue();
        assertNotNull(m);
        assertEquals(m.getClass(), ReadReply.class);
        var readReply = (ReadReply) m;
        assertEquals(readReply.dest(), CLIENT_ID_1);
        assertEquals(readReply.ok(), true);
        assertEquals(readReply.value(), "value1");

        storages.values().forEach(s -> assertEquals(s.get("key1"), "value1"));
    }

    private <Req extends ExternalMessage> Object fastPath(Req request) {
        var comLeaderNtwk = net(request.dest());
        
        comLeaderNtwk.receive(request);
        
        assertEquals(comLeaderNtwk.messageQueueSize(), 2);

        var preAcceptReplies = comLeaderNtwk.drainMessageQueue().map(msg -> {
            assertEquals(msg.getClass(), PreAccept.class);
            
            var ntwk = net(msg.dest()); 
            ntwk.receive(msg);
            
            assertEquals(ntwk.messageQueueSize(), 1);
            
            var reply = ntwk.pollMessageQueue();
            assertEquals(reply.getClass(), PreAcceptOK.class);
            
            return reply;
        }).collect(Collectors.toList());

        preAcceptReplies.forEach(comLeaderNtwk::receive);

        drv(request.dest()).executeScheduled();
        
        assertEquals(comLeaderNtwk.messageQueueSize(), 3);

        var responseCandidates = comLeaderNtwk.drainMessageQueue().map(msg -> {
            if (msg instanceof Commit) {
                var ntwk = net(msg.dest()); 
                ntwk.receive(msg);
                drv(msg.dest()).executeScheduled();

                assertEquals(ntwk.messageQueueSize(), 0);
                return null;
            } else {
                return msg;
            }            
        }).filter(Objects::nonNull).collect(Collectors.toList());

        assertEquals(responseCandidates.size(), 1);
        
        return responseCandidates.get(0);
    }

    private LocalNetwork net(String id) {
        return networks.get(id);
    }

    private CurrentThreadExecutingDriver drv(String id) {
        return drivers.get(id);
    }

    private Map<String, NetworkMessage> drainByDestination(String rId) {
        return net(rId).drainMessageQueue().collect(toMap(NetworkMessage::dest, identity()));        
    }

    private Map<Class<?>, List<NetworkMessage>> drainByType(String rId) {
        return net(rId).drainMessageQueue().collect(Collectors.groupingBy(Object::getClass));
    }

}
