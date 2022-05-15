package edu.yuvyurchenko.jepaxos.epaxos;

import edu.yuvyurchenko.jepaxos.epaxos.handlers.AcceptHandler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.AcceptReplyHandler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.CommitHandler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.PreAcceptHandler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.PreAcceptOKHandler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.PreAcceptReplyHandler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.PrepareHandler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.PrepareReplyHandler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.ReadHandler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.RequestAndReadHandler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.RequestHandler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.TryPreAcceptHandler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.TryPreAcceptReplyHandler;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Storage;

import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;

public class Replica {
    private final Cluster cluster;
    private final Storage storage;
    private final Network network;
    private final InstanceSpace instanceSpace;
    private final CommandExecutor commandExecutor;

    Replica(Cluster cluster,
            Storage storage,
            Network network,
            CommandOperationRegistry operationRegistry) {
        this.cluster = cluster;
        this.instanceSpace = new InstanceSpace(cluster);
        this.storage = storage;
        this.network = network;
        this.commandExecutor = new CommandExecutor(cluster, 
                                                   instanceSpace, 
                                                   storage, 
                                                   network, 
                                                   new RecoveryInitiator(cluster, 
                                                                         network, 
                                                                         instanceSpace), 
                                                   operationRegistry);
        network.registerHandler(Request.class, new RequestHandler(cluster, network, instanceSpace));
        network.registerHandler(Read.class, new ReadHandler(cluster, network, instanceSpace));
        network.registerHandler(RequestAndRead.class, new RequestAndReadHandler(cluster, network, instanceSpace));
        network.registerHandler(PreAccept.class, new PreAcceptHandler(cluster, network, instanceSpace));
        network.registerHandler(PreAcceptOK.class, new PreAcceptOKHandler(cluster, network, instanceSpace));
        network.registerHandler(PreAcceptReply.class, new PreAcceptReplyHandler(cluster, network, instanceSpace));
        network.registerHandler(Commit.class, new CommitHandler(cluster, network, instanceSpace));
        network.registerHandler(Accept.class, new AcceptHandler(cluster, network, instanceSpace)); 
        network.registerHandler(AcceptReply.class, new AcceptReplyHandler(cluster, network, instanceSpace));
        network.registerHandler(Prepare.class, new PrepareHandler(cluster, network, instanceSpace));
        network.registerHandler(PrepareReply.class, new PrepareReplyHandler(cluster, network, instanceSpace, operationRegistry));
        network.registerHandler(TryPreAccept.class, new TryPreAcceptHandler(cluster, network, instanceSpace, operationRegistry));
        network.registerHandler(TryPreAcceptReply.class, new TryPreAcceptReplyHandler(cluster, network, instanceSpace, operationRegistry));
    }

    public void start() {

    }

    public void shutdown() {

    }

}
