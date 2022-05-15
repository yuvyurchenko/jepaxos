package edu.yuvyurchenko.jepaxos.maelstrom;

public enum MessageType {
    init, 
    init_ok, 
    error, 
    read,
    read_ok,
    write,
    write_ok, 
    cas,
    cas_ok, 
    accept, 
    accept_reply, 
    commit, 
    preaccept,
    preaccept_ok, 
    preaccept_reply,
    prepare,
    prepare_reply,
    trypreaccept,
    trypreaccept_reply;
}
