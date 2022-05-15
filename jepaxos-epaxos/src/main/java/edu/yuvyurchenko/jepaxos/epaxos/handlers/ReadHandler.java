package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyData;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyType;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.Instance;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class ReadHandler extends AbstractExternalHandler<Read> {
    
    public ReadHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    @Override
    protected Instance registerNewCommandLeaderInstance(Read request) {
        return instanceSpace.registerNewCommandLeaderInstance(request.command(), 
                                                              new ReplyData(ReplyType.READ,
                                                                            request.src(), 
                                                                            request.meta()));
    }

}
