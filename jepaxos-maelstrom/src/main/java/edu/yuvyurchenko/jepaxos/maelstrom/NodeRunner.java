package edu.yuvyurchenko.jepaxos.maelstrom;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.yuvyurchenko.jepaxos.epaxos.Replica;
import edu.yuvyurchenko.jepaxos.epaxos.ReplicaBuilder;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Storage;
import edu.yuvyurchenko.jepaxos.maelstrom.impl.CommandOperations;
import edu.yuvyurchenko.jepaxos.maelstrom.impl.CurrentThreadExecutingDriver;
import edu.yuvyurchenko.jepaxos.maelstrom.impl.InMemoryStorage;
import edu.yuvyurchenko.jepaxos.maelstrom.impl.JsonNetwork;
import edu.yuvyurchenko.jepaxos.maelstrom.impl.PojoCluster;

public class NodeRunner {
    
    public static void main(String... args) throws IOException {
        var mapper = new ObjectMapper();
        var stdin = new BufferedReader(new InputStreamReader(System.in));
        Node node = new Node(jn -> {
            try {
                System.out.println(mapper.writeValueAsString(jn.toObjectNode()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });

        while (true) {
            try {
                var msgStr = stdin.readLine();
                if (isPoisonPill(msgStr)) {
                    break;
                }
                var msg = new Message(mapper.readTree(msgStr));
                var type = msg.getType();
                switch(type) {
                    case init -> node.handleInit(msg);
                    default -> node.handleMessage(msg);
                }
            } catch (Exception e) {
                System.err.println(e);
                e.printStackTrace(System.err);
            }
            
        }
        if (node.replica != null) {
            node.replica.shutdown();
        }
    }

    private static class Node {
        final Consumer<Message> outChannel;
        final AtomicInteger msgIdCounter;
        final JsonNetwork network;
        final CurrentThreadExecutingDriver driver;
        volatile Replica replica;

        Node(Consumer<Message> outChannel) {
            this.outChannel = outChannel;
            this.msgIdCounter = new AtomicInteger();
            this.network = new JsonNetwork(outChannel, msgIdCounter);
            this.driver = new CurrentThreadExecutingDriver();
        }

        void handleInit(Message msg) {
            var currReplicaId = msg.getNodeId();
            var allReplicaIds = msg.getNodeIds();

            Storage storage = new InMemoryStorage();
            Cluster cluster = new PojoCluster(currReplicaId, allReplicaIds);
            Replica replica = new ReplicaBuilder()
                .withCluster(cluster)
                .withNetwork(network)
                .withStorage(storage)
                .withCustomExecutingDriver(driver)
                .withCommandOparetion(CommandOperations.GET.id(), CommandOperations.GET.operation())
                .withCommandOparetion(CommandOperations.PUT.id(), CommandOperations.PUT.operation())
                .withCommandOparetion(CommandOperations.CAS.id(), CommandOperations.CAS.operation())
                .withCommitGracePeriodMs(1000L)
                .withCommitGracePeriodShiftMs(200L)
                .withWaitCommitPeriodMs(0)
                .withMaxWaitCommitTries(0)
                .build();
            
            this.replica = replica;
            replica.start();

            var resp = new Message();
            resp.setSrc(msg.getDest());
            resp.setDest(msg.getSrc());
            resp.setType(MessageType.init_ok);
            resp.setInReplyTo(msg.getMsgId());
            resp.setMsgId(msgIdCounter.getAndIncrement());
            outChannel.accept(resp);
        }

        void handleMessage(Message msg) {
            if (replica == null) {
                var resp = new Message();
                resp.setSrc(msg.getDest());
                resp.setDest(msg.getSrc());
                resp.setType(MessageType.error);
                resp.setInReplyTo(msg.getMsgId());
                resp.setMsgId(msgIdCounter.getAndIncrement());
                resp.setCode(22);
                resp.setText("replica must be initialized first");
                outChannel.accept(resp);
                return;    
            }
            
            network.receive(msg);
            driver.executeScheduled();
        }
    }

    private static boolean isPoisonPill(String msg) {
        return false;
    }
}
