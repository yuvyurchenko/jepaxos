package edu.yuvyurchenko.jepaxos.epaxos.handlers;

import edu.yuvyurchenko.jepaxos.epaxos.InstanceSpace;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ExternalMessage.*;
import edu.yuvyurchenko.jepaxos.epaxos.model.Instance;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyData;
import edu.yuvyurchenko.jepaxos.epaxos.messages.ReplyType;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Cluster;
import edu.yuvyurchenko.jepaxos.epaxos.plugins.Network;

public class RequestHandler extends AbstractExternalHandler<Request> {
    
    public RequestHandler(Cluster cluster, Network network, InstanceSpace instanceSpace) {
        super(cluster, network, instanceSpace);
    }

    @Override
    protected Instance registerNewCommandLeaderInstance(Request request) {
        return instanceSpace.registerNewCommandLeaderInstance(request.command(), 
                                                              new ReplyData(ReplyType.REQUEST,
                                                                            request.src(), 
                                                                            request.meta()));
    }

}
