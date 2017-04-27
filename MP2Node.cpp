/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode and update ring
	sort(curMemList.begin(), curMemList.end());
	ring = curMemList;

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	stabilizationProtocol();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	// find the 3 nodes on the virtual ring and send the CREATE message
	vector<Node> nodes = findNodes(key);
	dispatchMessages(&(nodes[0].nodeAddress), Message(g_transID, memberNode->addr, CREATE, key, value, PRIMARY));
	dispatchMessages(&(nodes[1].nodeAddress), Message(g_transID, memberNode->addr, CREATE, key, value, SECONDARY));
	dispatchMessages(&(nodes[2].nodeAddress), Message(g_transID, memberNode->addr, CREATE, key, value, TERTIARY));
	// log coordinate request
	timeoutBook[g_transID] = vector<long>({memberNode->heartbeat, 3, 0});
	Message rqstMsg(g_transID, memberNode->addr, CREATE, key, value);
	replyBook[g_transID].push_back(rqstMsg.toString());
	++g_transID;
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	// find the 3 nodes on the virtual ring and send the READ message
	vector<Node> nodes = findNodes(key);
	dispatchMessages(&(nodes[0].nodeAddress), Message(g_transID, memberNode->addr, READ, key));
	dispatchMessages(&(nodes[1].nodeAddress), Message(g_transID, memberNode->addr, READ, key));
	dispatchMessages(&(nodes[2].nodeAddress), Message(g_transID, memberNode->addr, READ, key));
	// log coordinate request
	timeoutBook[g_transID] = vector<long>({memberNode->heartbeat, 3, 0});
	Message rqstMsg(g_transID, memberNode->addr, READ, key);
	replyBook[g_transID].push_back(rqstMsg.toString());
	++g_transID;
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	// find the 3 nodes on the virtual ring and send the UPDATE message
	vector<Node> nodes = findNodes(key);
	dispatchMessages(&(nodes[0].nodeAddress), Message(g_transID, memberNode->addr, UPDATE, key, value, PRIMARY));
	dispatchMessages(&(nodes[1].nodeAddress), Message(g_transID, memberNode->addr, UPDATE, key, value, SECONDARY));
	dispatchMessages(&(nodes[2].nodeAddress), Message(g_transID, memberNode->addr, UPDATE, key, value, TERTIARY));
	// log coordinate request
	timeoutBook[g_transID] = vector<long>({memberNode->heartbeat, 3, 0});
	Message rqstMsg(g_transID, memberNode->addr, UPDATE, key, value);
	replyBook[g_transID].push_back(rqstMsg.toString());
	++g_transID;
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	// find the 3 nodes on the virtual ring and send the DELETE message
	vector<Node> nodes = findNodes(key);
	dispatchMessages(&(nodes[0].nodeAddress), Message(g_transID, memberNode->addr, DELETE, key));
	dispatchMessages(&(nodes[1].nodeAddress), Message(g_transID, memberNode->addr, DELETE, key));
	dispatchMessages(&(nodes[2].nodeAddress), Message(g_transID, memberNode->addr, DELETE, key));
	// log coordinate request
	timeoutBook[g_transID] = vector<long>({memberNode->heartbeat, 3, 0});
	Message rqstMsg(g_transID, memberNode->addr, DELETE, key);
	replyBook[g_transID].push_back(rqstMsg. toString());
	++g_transID;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	// Insert key, value, replicaType into the hash table
	if (ht->count(key)) {
		return false;
	} else {
		Entry entry(value, memberNode->heartbeat, replica);
		ht->create(key, entry.convertToString());
		return true;
	}
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	// Read key from local hash table and return value
	if (!ht->count(key)) {
		return "";
	}
	Entry entry(ht->read(key));
	return entry.value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	// Update key in local hash table and return true or false
	if (ht->read(key).empty()) {
		return false;
	} else {
		Entry entry(ht->read(key));
		entry.value = value;
		entry.timestamp = memberNode->heartbeat;
		if (replica != RESERVED) {
			entry.replica = replica;
		}
		ht->update(key, entry.convertToString());
		return true;
	}
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deleteKey(string key) {
	// Delete the key from the local hash table
	if (ht->read(key).empty()) {
		return false;
	} else {
		ht->deleteKey(key);
		return true;
	}
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	char * data;
	int size;
	bool status;
	// dequeue all messages and handle them
	while (!memberNode->mp2q.empty()) {
		// Pop a message from the queue
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();
		string message(data, data + size);
		// Handle the message types here
		// Also ensure all CRUD operation get QUORUM replies
		Message msg(message);
		switch (msg.type) {
			case CREATE: {
				status = createKeyValue(msg.key, msg.value, msg.replica);
				if (status) {
					log->logCreateSuccess(&(memberNode->addr), false, msg.transID, msg.key, msg.value);
					dispatchMessages(&(msg.fromAddr), Message(msg.transID, memberNode->addr, REPLY, true));
				} else {
					log->logCreateFail(&(memberNode->addr), false, msg.transID, msg.key, msg.value);
					dispatchMessages(&(msg.fromAddr), Message(msg.transID, memberNode->addr, REPLY, false));
				}
				break;
			}
			case READ: {
				string result = readKey(msg.key);
				if (result.size()) {
					log->logReadSuccess(&(memberNode->addr), false, msg.transID, msg.key, result);
				} else {
					log->logReadFail(&(memberNode->addr), false, msg.transID, msg.key);
				}
				dispatchMessages(&(msg.fromAddr), Message(msg.transID, memberNode->addr, result));
				break;
			}
			case UPDATE: {
				status = updateKeyValue(msg.key, msg.value, msg.replica);
				if (status) {
					log->logUpdateSuccess(&(memberNode->addr), false, msg.transID, msg.key, msg.value);
					dispatchMessages(&(msg.fromAddr), Message(msg.transID, memberNode->addr, REPLY, true));
				} else {
					log->logUpdateFail(&(memberNode->addr), false, msg.transID, msg.key, msg.value);
					dispatchMessages(&(msg.fromAddr), Message(msg.transID, memberNode->addr, REPLY, false));
				}
				break;
			}
			case DELETE: {
				status = deleteKey(msg.key);
				if (status) {
					log->logDeleteSuccess(&memberNode->addr, false, msg.transID, msg.key);
					dispatchMessages(&(msg.fromAddr), Message(msg.transID, memberNode->addr, REPLY, true));
				} else {
					log->logDeleteFail(&memberNode->addr, false, msg.transID, msg.key);
					dispatchMessages(&(msg.fromAddr), Message(msg.transID, memberNode->addr, REPLY, false));
				}
				break;
			}
			case REPLY:
			case READREPLY:
				if (timeoutBook.count(msg.transID)) {
					replyBook[msg.transID].push_back(msg.toString());
					checkStatus(msg.transID);
				}
				break;
		}
	}
	// Go over transaction book to report timeout transactions
	vector<int> timeouts;
	for (pair<int, vector<long>> trans : timeoutBook) {
		if (trans.second[0] + TIMEOUT <= memberNode->heartbeat) {
			Message rqst(replyBook[trans.first][0]);
			switch (rqst.type) {
				case CREATE:
					log->logCreateFail(&(memberNode->addr), true, rqst.transID, rqst.key, rqst.value);
					break;
				case READ:
					cout << "fail for timeout" << endl;
					log->logReadFail(&(memberNode->addr), true, rqst.transID, rqst.key);
					break;
				case UPDATE:
					log->logUpdateFail(&(memberNode->addr), true, rqst.transID, rqst.key, rqst.value);
					break;
				case DELETE:
					log->logDeleteFail(&(memberNode->addr), true, rqst.transID, rqst.key);
					break;
				default:
					break;
			}
			timeouts.push_back(trans.first);
		}
	}
	for (int timeout : timeouts) {
		timeoutBook.erase(timeout);
		replyBook.erase(timeout);
	}
}

/**
 * FUNCTION NAME: checkStatus
 *
 * DESCRIPTION: Manage responses of transations 
 * 				1) Recovery Transition expects all response
 *				2) Client Transition expects Quorum response
 *				3) Read Transition has read repair
 */
void MP2Node::checkStatus(int transID) {
	int count = timeoutBook[transID][1];
	int size = replyBook[transID].size();
	// Not receive quorum reply yet
	if (size - 1 < ((count + 1) / 2)) {
		return;
	}
	Message rqst(replyBook[transID][0]);
	vector<Message> msgs;
	if (rqst.type == READ) {
		// Read always expect 3 replies as maximum
		msgs.push_back(Message(replyBook[transID][1]));
		msgs.push_back(Message(replyBook[transID][2]));
		// Only 2 replies received
		if (size == count) {
			if (msgs[0].value == "" && msgs[1].value == "") {
				// Quorum failure received, log failure and remove entry
				cout << "fail for 2 failures" << endl;
				log->logReadFail(&(memberNode->addr), true, transID, rqst.key);
				replyBook.erase(transID);
				timeoutBook.erase(transID);
			} else if (msgs[0].value == msgs[1].value && msgs[0].value != "") {
				cout << "success for 2 consistent success" << endl;
				// Quorum success received, and they are consistent. log succ and wait for 3rd reply to repair read
				log->logReadSuccess(&(memberNode->addr), true, transID, rqst.key, msgs[0].value);
				timeoutBook[transID][2] = 1;
			} else {
				// Neeed the 3rd reply to make the decision, do nothing
			}
		} else {
			msgs.push_back(Message(replyBook[transID][3]));
			int blank(0);
			map<string ,int> candidates;
			for (Message &msg : msgs) {
				if (msg.value == "") {
					++blank;
				} else {
					++candidates[msg.value];
				}
			}
			if (blank > 1) {
				// Quorum failure received, log failure and remove entry
				cout << "fail for quorum failures" << endl;
				log->logReadFail(&(memberNode->addr), true, transID, rqst.key);
			} else if (blank + candidates.size() == 3) {
				// Quorum success received but not consistent
				cout << "fail for inconsistent success" << endl;
				log->logReadFail(&(memberNode->addr), true, transID, rqst.key);
			} else {
				// Quorum success received and consistent
				cout << "success for quorum consistent success" << endl;
				string value("");
				for (pair<string, int> candidate : candidates) {
					if (candidate.second == 1) {
						continue;
					}
					value = candidate.first;
				}
				// The case 3rd reply is required for the log success
				if (timeoutBook[transID][2] == 0) {
					log->logReadSuccess(&(memberNode->addr), true, transID, rqst.key, value);
					timeoutBook[transID][2] = 1;
				}
				// Read repair message
				for (Message &msg : msgs) {
					if (msg.value == value) {
						continue;
					}
					Message repair(g_transID, memberNode->addr, UPDATE, rqst.key, value, RESERVED);
					dispatchMessages(&(msg.fromAddr), repair);
					timeoutBook[g_transID] = vector<long>({memberNode->heartbeat, 1, 0});
					replyBook[g_transID].push_back(repair.toString());
					++g_transID;
				}	
			}
			replyBook.erase(transID);
			timeoutBook.erase(transID);
		}
	} else {
		// Handle CREATE, UPDATE and DELETE requests
		int fail(0), succ(0);
		for (int i = 1; i < size; ++i) {
			msgs.push_back(Message(replyBook[transID][i]));
		}
		for (Message &msg : msgs) {
			if (msg.success) {
				++succ;
			} else {
				++fail;
			}
		}
		// Not receive enough reply to make the decision
		if (succ < ((count + 1)) / 2 && fail < ((count + 1) / 2)) {
			return;
		}
		// Log success and fail based on quorum
		switch (rqst.type) {
			case CREATE: 
				if (succ >= ((count + 1) / 2)) {
					log->logCreateSuccess(&(memberNode->addr), true, transID, rqst.key, rqst.value);
				} else {
					log->logCreateFail(&(memberNode->addr), true, transID, rqst.key, rqst.value);
				}
				break;
			case UPDATE:
				if (succ >= (count + 1) / 2) {
					log->logUpdateSuccess(&(memberNode->addr), true, transID, rqst.key, rqst.value);
				} else {
					log->logUpdateFail(&(memberNode->addr), true, transID, rqst.key, rqst.value);
				}
				break;
			case DELETE:
				if (succ >= (count + 1) / 2) {
					log->logDeleteSuccess(&(memberNode->addr), true, transID, rqst.key);
				} else {
					log->logDeleteFail(&(memberNode->addr), true, transID, rqst.key);
				}
				break;
			default:
				break;
		}
		timeoutBook.erase(transID);
		replyBook.erase(transID);
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	int size = ring.size();
	if (size >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(size-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i < size; i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%size));
					addr_vec.emplace_back(ring.at((i+2)%size));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
	// TODO: Go over entire hashtable, check key-value one be one
	// 1. use hasMyReplica and hasReplicaOf to figure out who store the key-value previously
	// 2. use findNodes method to figure out who should store the key-value currently
	// 3. if something changes, the highest non-faulty node should send CREATE message to node doesn't has the key-value
	// 	primary > sencondary > tertiary, expect Quorum success response, otherwise report a failure
	// 4. also update the replica type in itself
	// 5. update hasMyReplica and hasReplicaOf at the end
	int size = ring.size();	
	vector<Node> masters, slaves;
	set<size_t> nodeBook;
	Node myself(memberNode->addr);
	for (int i = 0; i < size; ++i) {
		nodeBook.insert(ring[i].nodeHashCode);
		if (ring[i] == myself) {
			int start = (i - 2 + size) % size;
			masters.emplace_back(ring[start]);
			masters.emplace_back(ring[(start + 1) % size]);
			slaves.emplace_back(ring[(i + 1) % size]);
			slaves.emplace_back(ring[(i + 2) % size]);
		}
	}
	// cluster doesn't change, safe to return
	if (haveReplicasOf == masters && hasMyReplicas == slaves) {
		return;
	}
	// cluster changes (node join, leave or fail)
	vector<string> overReplicas;
	if (size >= 3) {
		for (map<string, string>::iterator iter = ht->hashTable.begin(); iter != ht->hashTable.end(); ++iter) {
			string key = iter->first;
			Entry entry(iter->second);
			string value = entry.value;
			ReplicaType replica = entry.replica;
			vector<Node> expects = findNodes(key);
			int count(0);
			if (replica == PRIMARY) {
				if (expects[0] != myself && expects[0] != hasMyReplicas[0] && expects[0] != hasMyReplicas[1]) {
					dispatchMessages(&(expects[0].nodeAddress), Message(g_transID, myself.nodeAddress, CREATE, key, value, PRIMARY));
					++count;
				}
				if (expects[1] != myself && expects[1] != hasMyReplicas[0] && expects[1] != hasMyReplicas[1]) {
					dispatchMessages(&(expects[1].nodeAddress), Message(g_transID, myself.nodeAddress, CREATE, key, value, SECONDARY));
					++count;
				}
				if (expects[2] != myself && expects[2] != hasMyReplicas[0] && expects[2] != hasMyReplicas[1]) {
					dispatchMessages(&(expects[2].nodeAddress), Message(g_transID, myself.nodeAddress, CREATE, key, value, TERTIARY));
					++count;
				}
			} else if (replica == SECONDARY) {
				// The primary replica is still alive, let it handle the stability
				if (nodeBook.count(haveReplicasOf[1].nodeHashCode)) {
					continue;
				}
				if (expects[0] != haveReplicasOf[1] && expects[0] != myself && expects[0] != hasMyReplicas[0]) {
					dispatchMessages(&(expects[0].nodeAddress), Message(g_transID, myself.nodeAddress, CREATE, key, value, PRIMARY));
					++count;
				}
				if (expects[1] != haveReplicasOf[1] && expects[1] != myself && expects[1] != hasMyReplicas[0]) {
					dispatchMessages(&(expects[1].nodeAddress), Message(g_transID, myself.nodeAddress, CREATE, key, value, SECONDARY));
					++count;
				}
				if (expects[2] != haveReplicasOf[1] && expects[2] != myself && expects[2] != hasMyReplicas[0]) {
					dispatchMessages(&(expects[2].nodeAddress), Message(g_transID, myself.nodeAddress, CREATE, key, value, TERTIARY));
					++count;
				}
			} else {
				// THe primary replica or the secondary replica is alive, let them to handle the stability
				if (nodeBook.count(haveReplicasOf[0].nodeHashCode || nodeBook.count(haveReplicasOf[1].nodeHashCode))) {
					continue;
				}
				if (expects[0] != haveReplicasOf[0] && expects[0] != haveReplicasOf[1] && expects[0] != myself) {
					dispatchMessages(&(expects[0].nodeAddress), Message(g_transID, myself.nodeAddress, CREATE, key, value, PRIMARY));
					++count;
				}
				if (expects[1] != haveReplicasOf[0] && expects[1] != haveReplicasOf[1] && expects[1] != myself) {
					dispatchMessages(&(expects[1].nodeAddress), Message(g_transID, myself.nodeAddress, CREATE, key, value, SECONDARY));
					++count;
				}
				if (expects[2] != haveReplicasOf[0] && expects[2] != haveReplicasOf[1] && expects[2] != myself) {
					dispatchMessages(&(expects[2].nodeAddress), Message(g_transID, myself.nodeAddress, CREATE, key, value, TERTIARY));
					++count;
				}

			}
			if (count) {
					timeoutBook[g_transID] = vector<long>({memberNode->heartbeat, count, 0});
					replyBook[g_transID].push_back(Message(g_transID, myself.nodeAddress, CREATE, key, value, PRIMARY).toString());
					++g_transID;
			}
			// Update self replica type
			if (expects[0] == myself) {
				iter->second = Entry(value, memberNode->heartbeat, PRIMARY).convertToString();
			} else if (expects[1] == myself) {
				iter->second = Entry(value, memberNode->heartbeat, SECONDARY).convertToString();
			} else if (expects[2] == myself) {
				iter->second = Entry(value, memberNode->heartbeat, TERTIARY).convertToString();
			} else {
				overReplicas.push_back(key);
			}
		}
	}
	for (string key : overReplicas) {
		ht->deleteKey(key);
	}
	haveReplicasOf = masters;
	hasMyReplicas = slaves;
}

/*
 * FUNCTION NAME: dispatchMessages
 *
 * DESCRIPTION: dispatches messages to corresponding nodes
 */
void MP2Node::dispatchMessages(Address *destAddr, Message message) {
	emulNet->ENsend(&memberNode->addr, destAddr, message.toString());
}
