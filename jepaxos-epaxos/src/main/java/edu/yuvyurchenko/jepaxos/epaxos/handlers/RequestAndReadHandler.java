package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyData;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyType;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.Instance;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class RequestAndReadHandler extends AbstractExternalHandler<RequestAndRead> {
    
    public RequestAndReadHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    @Override
    protected Instance registerNewCommandLeaderInstance(RequestAndRead request) {
        return instanceSpace.registerNewCommandLeaderInstance(request.command(), 
                                                              new ReplyData(ReplyType.REQUEST_AND_READ,
                                                                            request.src(), 
                                                                            request.meta()));
    }

}
