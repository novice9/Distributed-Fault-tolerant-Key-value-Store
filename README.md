# Distributed-Fault-tolerant-Key-value-Store

This repository implements a distributed fault-tolerate key-value store in 2 parts:

1. Gossip Membership Procotol (class MP1Node)

	each node maintains a membership list to be aware of the cluster condition (detect join, leave and fail)

2. Distributed Key-Value Store (class MP2Node)
	
	load balance via a consistent hashing ring to hash both servers IP and keys, fault tolerate up to 2 via replication
	
	it is a peer-to-peer system, thus both Client and Server side CRUD are implemented (server can be coordinate) with Quorum consistency
	
	Read repair and failure recovery are also implemented to improve stability


Application is a simulator to simulate the 3-layer framework (application layer, peer-to-peer layer and network layer)

Our membership and key-value store sit in the peer-to-peer layer, network layer is implemented in class EmulNet to mimic internet behavior


The testcase has 4 scenorias (each one will build a cluster of 10 nodes):

1. create - will create 100 key-value over the cluster

2. delete - do create first, then delete 50 of the key-value

3. read - do create first, then test read in 6 situation: normal read, read with 1 replica fails, read with 2 replica fails, read after recovery, read with 1 non-replica fail, read non-existent key

4. update - do create first, then test update in 6 situation: normal update, update with 1 replica fails, update with 2 replica fails, update after recovery, update with 1 non-replica fail, update non-existent key  


Need gcc version 4.7 or higher

To compile:(need gcc version 4.7 or g++ 11)
% make 

To run test:
%./Application testcase/create.conf

