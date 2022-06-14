package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.yuvyurchenko.jepaxos.epaxos.CommandOperationRegistry;
import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.Accept;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.Commit;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.PrepareReply;
import edu.yuvyurchenko.jepaxos.epaxos.messages.InternalMessage.TryPreAccept;
import edu.yuvyurchenko.jepaxos.epaxos.model.Attributes;
import edu.yuvyurchenko.jepaxos.epaxos.model.InstanceStatus;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class PrepareReplyHandler extends AbstractRecoveryHandler<PrepareReply> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(PrepareReplyHandler.class);

    public PrepareReplyHandler(Cluster cluster, 
                               Network network, 
                               InstanceSpace instanceSpace, 
                               CommandOperationRegistry operationRegistry) {
        super(cluster, network, instanceSpace, operationRegistry);
    }

    public void handle(PrepareReply prepareReply) {
        LOGGER.debug("Receive - prepareReply={}", prepareReply);
        var instance = instanceSpace.getInstance(prepareReply.replicaId(), prepareReply.instanceId());
        var lb = instance.leaderBookkeeping();
        if (lb == null || !lb.getPreparing()) {
            // we've moved on -- these are delayed replies, so just ignore
		    // TODO: should replies for non-current ballots be ignored?
            LOGGER.debug("Exit - delayed reply");
		    return;
        }
        if (!prepareReply.ok()) {
            lb.incNacks();
            LOGGER.debug("Exit - rejected Prepare");
            return;
        }
        lb.incPrepareOKs();
        if (prepareReply.status() == InstanceStatus.COMMITTED || prepareReply.status() == InstanceStatus.EXECUTED) {
            instanceSpace.registerNewInstance(prepareReply.replicaId(), 
                                              prepareReply.instanceId(), 
                                              prepareReply.command(), 
                                              instance.getBallot(), 
                                              InstanceStatus.COMMITTED, 
                                              prepareReply.attributes());
            broadcast(extReplicaId -> new Commit(cluster.getCurrReplicaId(), 
                                                 extReplicaId, 
                                                 prepareReply.meta(), 
                                                 cluster.getCurrReplicaId(), 
                                                 prepareReply.replicaId(), 
                                                 prepareReply.instanceId(), 
                                                 instance.getCommand(), 
                                                 prepareReply.attributes()));
            LOGGER.debug("Exit - broadcast Commit");
            return;
        }
        if (prepareReply.status() == InstanceStatus.ACCEPTED) {
            if (lb.recoveryInstance() == null || lb.getMaxRecvBallot() == null || lb.getMaxRecvBallot().lessThan(prepareReply.ballot())) {
                lb.initRecoveryInstance(prepareReply.command(), 
                                        prepareReply.status(), 
                                        prepareReply.attributes(), 
                                        0, 
                                        false);
                lb.setMaxRecvBallot(prepareReply.ballot());
            }
        }
        if ((prepareReply.status() == InstanceStatus.PREACCEPTED || prepareReply.status() == InstanceStatus.PREACCEPTED_EQ) 
            && (lb.recoveryInstance() == null || lb.recoveryInstance().getStatus() == InstanceStatus.NONE 
                                              || lb.recoveryInstance().getStatus() == InstanceStatus.PREACCEPTED 
                                              || lb.recoveryInstance().getStatus() == InstanceStatus.PREACCEPTED_EQ)) {
            if (lb.recoveryInstance() == null) {
                lb.initRecoveryInstance(prepareReply.command(), 
                                        prepareReply.status(), 
                                        prepareReply.attributes(), 
                                        1, 
                                        false);
            } else if (Objects.equals(prepareReply.attributes(), instance.getAttributes())) {
                lb.recoveryInstance().incPreAcceptCount();
            } else if (prepareReply.status() == InstanceStatus.PREACCEPTED_EQ) {
                // If we get different ordering attributes from pre-acceptors, we must go with the ones
			    // that agreed with the initial command leader (in case we do not use Thrifty).
			    // This is safe if we use thrifty, although we can also safely start phase 1 in that case.
                lb.initRecoveryInstance(prepareReply.command(), 
                                        prepareReply.status(), 
                                        prepareReply.attributes(), 
                                        1, 
                                        false);
            }
            if (Objects.equals(prepareReply.acceptorId(), prepareReply.replicaId())) {
                //if the reply is from the initial command leader, then it's safe to restart phase 1
                lb.recoveryInstance().setLeaderResponded(true);
                LOGGER.debug("Exit - Leader Responded");
                return;
            }
        }
        if (lb.getPrepareOKs() < cluster.getAllReplicaIds().size() / 2) {
            LOGGER.debug("Exit - Not enough replies for quorum");
            return;
        }

        //Received Prepare replies from a majority

        var ri = lb.recoveryInstance();

        if (ri != null) {
            if (ri.getStatus() == InstanceStatus.ACCEPTED 
                || (!ri.getLeaderResponded() && hasQuorum(ri.getPreAcceptCount()) 
                                             && ri.getStatus() == InstanceStatus.PREACCEPTED_EQ)) {
                instance.setCommand(ri.getCommand());
                instance.setAttributes(ri.getAttributes());
                instance.setStatus(InstanceStatus.ACCEPTED);
                lb.setPreparing(false);
                broadcast(extReplicaId -> new Accept(cluster.getCurrReplicaId(), 
                                                     extReplicaId, 
                                                     prepareReply.meta(), 
                                                     cluster.getCurrReplicaId(), 
                                                     prepareReply.replicaId(), 
                                                     prepareReply.instanceId(), 
                                                     instance.getBallot(), 
                                                     instance.getCommand(), 
                                                     instance.getAttributes()));
            } else if (!ri.getLeaderResponded() && ri.getPreAcceptCount() >= (cluster.getAllReplicaIds().size()/2+1)/2) {
                lb.setPreAcceptOKs(0);
                lb.setNacks(0);
                lb.initPossibleQuorum(cluster.getAllReplicaIds());
                var conflicts = findPreAcceptConflicts(ri.getCommand(), 
                                                       prepareReply.replicaId(), 
                                                       prepareReply.instanceId(), 
                                                       ri.getAttributes());
                if (conflicts.hasCoflicts()) {
                    var conflictInstance = instanceSpace.getInstance(conflicts.replicaId(), conflicts.instanceId());
                    if (conflictInstance.getStatus() == InstanceStatus.COMMITTED 
                        || conflictInstance.getStatus() == InstanceStatus.EXECUTED) {
                        restartPhase1(prepareReply.replicaId(), 
                                      prepareReply.instanceId(), 
                                      instance.getBallot(), 
                                      ri.getCommand(), 
                                      instance.replyData());
                        return;
                    } else {
                        lb.setNacks(1);
                        lb.updatePossibleQuorum(cluster.getCurrReplicaId(), false);
                    }
                } else {
                    instance.setCommand(ri.getCommand());
                    instance.setAttributes(ri.getAttributes());
                    instance.setStatus(InstanceStatus.PREACCEPTED);
                    lb.setPreAcceptOKs(1);
                }
                lb.setPreparing(false);
                lb.setTryingToPreAccept(true);
                broadcast(extReplicaId -> new TryPreAccept(cluster.getCurrReplicaId(), 
                                                           extReplicaId, 
                                                           prepareReply.meta(),
                                                           cluster.getCurrReplicaId(),
                                                           prepareReply.replicaId(), 
                                                           prepareReply.instanceId(), 
                                                           instance.getBallot(), 
                                                           instance.getCommand(), 
                                                           instance.getAttributes()));
                LOGGER.debug("Exit - broadcast TryPreAccept");
            } else {
                //start Phase1 in the initial leader's instance
                lb.setPreparing(false);
                restartPhase1(prepareReply.replicaId(), 
                              prepareReply.instanceId(), 
                              instance.getBallot(), 
                              ri.getCommand(), 
                              instance.replyData());
            }
        } else {
            var noopDeps = Map.of(prepareReply.replicaId(), prepareReply.instanceId() - 1);
            lb.setPreparing(false);
            instanceSpace.registerNewInstance(prepareReply.replicaId(), 
                                              prepareReply.instanceId(), 
                                              null, 
                                              InstanceStatus.ACCEPTED, 
                                              new Attributes(0, noopDeps),
                                              instance);
            broadcast(extReplicaId -> new Accept(cluster.getCurrReplicaId(), 
                                                 extReplicaId, 
                                                 prepareReply.meta(), 
                                                 cluster.getCurrReplicaId(), 
                                                 prepareReply.replicaId(), 
                                                 prepareReply.instanceId(), 
                                                 instance.getBallot(), 
                                                 null, 
                                                 new Attributes(0, noopDeps)));
            LOGGER.debug("Exit - broadcast Accept");
        }
    }

}
