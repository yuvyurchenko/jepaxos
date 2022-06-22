# JEPAXOS

## Overview
Java 17+ implementation of EPaxos protocol based on the paper: _There Is More Consensus in Egalitarian Parliaments by Iulian Moraru, David G. Andersen, Michael Kaminsky_ (https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf)

Initial implementation in Go has been used as a reference: https://github.com/efficient/epaxos 

<b>DISCLAIMER</b>: Not for production usage (=^ ◡ ^=)

## Project structure

JEPaxos has two modules:
- jepaxos-epaxos: epaxos library with a set of plug-points for building linearizable key-value storage
- jepaxos-maelstrom: a simple demo application using jepaxos-epaxos library for running on [Jepsen: Maelstrom](https://github.com/jepsen-io/maelstrom) workbench  

### jepaxos-epaxos

This library implements core functionality including optimized EPaxos recovery from the paper.

The current implementation lacks the following parts:
- durable instance sequences
- executed instances garbage collection
- cluster topology change

JEPaxos has a set of plug-points to use for integration with the library:
- [Cluster](jepaxos-epaxos/src/main/java/edu/yuvyurchenko/jepaxos/epaxos/plugins/Cluster.java)
- [Network](jepaxos-epaxos/src/main/java/edu/yuvyurchenko/jepaxos/epaxos/plugins/Network.java)
- [Storage](jepaxos-epaxos/src/main/java/edu/yuvyurchenko/jepaxos/epaxos/plugins/Storage.java)
- [CommandOperation](jepaxos-epaxos/src/main/java/edu/yuvyurchenko/jepaxos/epaxos/plugins/CommandOperation.java)
- [ExecutingDriver](jepaxos-epaxos/src/main/java/edu/yuvyurchenko/jepaxos/epaxos/plugins/ExecutingDriver.java)

[ReplicaBuilder](jepaxos-epaxos/src/main/java/edu/yuvyurchenko/jepaxos/epaxos/ReplicaBuilder.java) is used to construct a [Replica](jepaxos-epaxos/src/main/java/edu/yuvyurchenko/jepaxos/epaxos/Replica.java) instance providing plugin implementations.

### jepaxos-maelstrom

This module works both as an example how to build a simple application using JEPaxos library and a way to test it since it builds an executable compatible with [lin-kv](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-lin-kv) workload from [Jepsen: Maelstrom](https://github.com/jepsen-io/maelstrom) tool.

#### How to run it?

Prerequisites:
- installed Java 17+ JDK
- installed [Gradle](https://gradle.org/install/)
- installed [Leiningen](https://leiningen.org/#install) 

Steps:
1. git clone https://github.com/yuvyurchenko/jepaxos
2. git clone https://github.com/jepsen-io/maelstrom
3. cd jepaxos
4. gradle clean installShadowDist
5. cp -r jepaxos-maelstrom/build/install/jepaxos-maelstrom-shadow/ ../maelstrom/jepaxos-maelstrom-shadow/
6. cd ../maelstrom
7. chmod +x jepaxos-maelstrom-shadow/bin/jepaxos-maelstrom
8. lein run test -w lin-kv --bin ./jepaxos-maelstrom-shadow/bin/jepaxos-maelstrom --time-limit 240 --node-count 3 --concurrency 2n --rate 400
9. check detailed reports in store/latest