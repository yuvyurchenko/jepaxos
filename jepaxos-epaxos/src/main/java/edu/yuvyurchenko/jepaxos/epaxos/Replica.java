package edu.yuvyurchenko.jepaxos.epaxos;

import edu.yuvyurchenko.jepaxos.epaxos.handlers.Handler;
import edu.yuvyurchenko.jepaxos.epaxos.handlers.HandlerOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import edu.yuvyurchenko.jepaxos.epaxos.plugins.ExecutingDriver;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Storage;

import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.*;

public class Replica {

    private static final Logger LOGGER = LoggerFactory.getLogger(Replica.class);

    private final Network network;
    private final InstanceSpace instanceSpace;
    private final CommandProcessor commandProcessor;
    private final ExecutingDriver driver;

    private final Handler<Request> requestHandler;
    private final Handler<Read> readHandler;
    private final Handler<RequestAndRead> requestAndReadHandler;
    private final Handler<PreAccept> preAcceptHandler;
    private final Handler<PreAcceptOK> preAcceptOKHandler;
    private final Handler<PreAcceptReply> preAcceptReplyHandler;
    private final Handler<Commit> commitHandler;
    private final Handler<Accept> acceptHandler; 
    private final Handler<AcceptReply> acceptReplyHandler;
    private final Handler<Prepare> prepareHandler;
    private final Handler<PrepareReply> prepareReplyHandler;
    private final Handler<TryPreAccept> tryPreAcceptHandler;
    private final Handler<TryPreAcceptReply> tryPreAcceptReplyHandler;

    Replica(Cluster cluster,
            Storage storage,
            Network network,
            ExecutingDriver driver,
            CommandOperationRegistry operationRegistry,
            long commitGracePeriodMs,
            long commitGracePeriodShiftMs,
            long waitCommitPeriodMs,
            int maxWaitCommitTries) {
        this.instanceSpace = new InstanceSpace(cluster);
        this.network = network;
        this.driver = driver;
        this.commandProcessor = new CommandProcessor(cluster, 
                                                     instanceSpace, 
                                                     storage, 
                                                     network, 
                                                     operationRegistry,
                                                     driver, 
                                                     commitGracePeriodMs,
                                                     commitGracePeriodShiftMs,
                                                     waitCommitPeriodMs,
                                                     maxWaitCommitTries);

        requestHandler = new RequestHandler(cluster, network, instanceSpace);
        readHandler = new ReadHandler(cluster, network, instanceSpace);
        requestAndReadHandler = new RequestAndReadHandler(cluster, network, instanceSpace);
        preAcceptHandler = new PreAcceptHandler(cluster, network, instanceSpace);
        preAcceptOKHandler = new PreAcceptOKHandler(cluster, network, instanceSpace);
        preAcceptReplyHandler = new PreAcceptReplyHandler(cluster, network, instanceSpace);
        commitHandler = new CommitHandler(cluster, network, instanceSpace);
        acceptHandler = new AcceptHandler(cluster, network, instanceSpace); 
        acceptReplyHandler = new AcceptReplyHandler(cluster, network, instanceSpace);
        prepareHandler = new PrepareHandler(cluster, network, instanceSpace);
        prepareReplyHandler = new PrepareReplyHandler(cluster, network, instanceSpace, operationRegistry);
        tryPreAcceptHandler = new TryPreAcceptHandler(cluster, network, instanceSpace, operationRegistry);
        tryPreAcceptReplyHandler = new TryPreAcceptReplyHandler(cluster, network, instanceSpace, operationRegistry);
    }

    public void start() {
        network.registerHandler(Request.class, r -> driver.enqueue(new HandlerOperation<>(r, requestHandler)));
        network.registerHandler(Read.class, r -> driver.enqueue(new HandlerOperation<>(r, readHandler)));
        network.registerHandler(RequestAndRead.class, r -> driver.enqueue(new HandlerOperation<>(r, requestAndReadHandler)));
        network.registerHandler(PreAccept.class, r -> driver.enqueue(new HandlerOperation<>(r, preAcceptHandler)));
        network.registerHandler(PreAcceptOK.class, r -> driver.enqueue(new HandlerOperation<>(r, preAcceptOKHandler)));
        network.registerHandler(PreAcceptReply.class, r -> driver.enqueue(new HandlerOperation<>(r, preAcceptReplyHandler)));
        network.registerHandler(Commit.class, r -> driver.enqueue(new HandlerOperation<>(r, commitHandler)));
        network.registerHandler(Accept.class, r -> driver.enqueue(new HandlerOperation<>(r, acceptHandler))); 
        network.registerHandler(AcceptReply.class, r -> driver.enqueue(new HandlerOperation<>(r, acceptReplyHandler)));
        network.registerHandler(Prepare.class, r -> driver.enqueue(new HandlerOperation<>(r, prepareHandler)));
        network.registerHandler(PrepareReply.class, r -> driver.enqueue(new HandlerOperation<>(r, prepareReplyHandler)));
        network.registerHandler(TryPreAccept.class, r -> driver.enqueue(new HandlerOperation<>(r, tryPreAcceptHandler)));
        network.registerHandler(TryPreAcceptReply.class, r -> driver.enqueue(new HandlerOperation<>(r, tryPreAcceptReplyHandler)));

        driver.schedule(() -> commandProcessor.executeCommands());

        LOGGER.info("Replica started");
    }

    public void shutdown() {
        driver.shutdown();
    }

}
