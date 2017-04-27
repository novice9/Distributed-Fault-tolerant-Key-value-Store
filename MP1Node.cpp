/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */
static bool compMemberEntry(const MemberListEntry &lhs, const MemberListEntry &rhs) {
	if (lhs.id == rhs.id) {
		return lhs.port < rhs.port;
	} else {
		return lhs.id < rhs.id;
	}
}

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
	if (memberNode->bFailed) {
		return false;
	} else {
		return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
	}
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
	Address joinaddr;
	joinaddr = getJoinAddress();
	// Self booting routines
	if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
		exit(1);
	}
	if( !introduceSelfToGroup(&joinaddr) ) {
		finishUpThisNode();
#ifdef DEBUGLOG
log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
		exit(1);
	}

	return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
	// node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = 0;
	memberNode->timeOutCounter = 0;
	initMemberListTable(memberNode);
	// push itself into the membership list
	int myID = *(int*)(&memberNode->addr.addr);
	int myPort = *(short*)(&memberNode->addr.addr[4]);
	MemberListEntry myself(myID, myPort, memberNode->heartbeat, memberNode->heartbeat);
	memberNode->memberList.push_back(myself);
	++(memberNode->nnb);
	log->logNodeAdd(&memberNode->addr, &memberNode->addr);
	return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
	if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
		// I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
log->LOG(&memberNode->addr, "Starting up group...");
#endif
		memberNode->inGroup = true;
	} else {
		size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
		msg = (MessageHdr *) malloc(msgsize * sizeof(char));
		// create JOINREQ message: format of data is {struct Address myaddr}
		msg->msgType = JOINREQ;
		memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
		memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
#ifdef DEBUGLOG
log->LOG(&memberNode->addr, "Trying to join...");
#endif
		// send JOINREQ message to introducer member
		emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
		free(msg);
	}

	return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
	memberNode->bFailed = true;
	return 1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
	if (memberNode->bFailed) {
		return;
	}
	// Check my messages
	checkMessages();
	// Wait until you're in the group...
	if( !memberNode->inGroup ) {
		return;
	}
	// ...then jump in and share your responsibilites!
	nodeLoopOps();
	return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
	void *ptr;
	int size;
	// Pop waiting messages from memberNode's mp1q
	while ( !memberNode->mp1q.empty() ) {
		ptr = memberNode->mp1q.front().elt;
		size = memberNode->mp1q.front().size;
		memberNode->mp1q.pop();
		recvCallBack((void *)memberNode, (char *)ptr, size);
	}
	return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	MessageHdr *recvMsg = (MessageHdr *)(data);
	if (recvMsg->msgType == JOINREQ) {
		// fetch joint member information from the message
		Address recvAddr;
		memcpy(&recvAddr.addr, data + sizeof(MessageHdr), sizeof(recvAddr.addr));
		// send back local membership list as JOINREP
		set<int> nofail;
		sendMemberList(JOINREP, &recvAddr, nofail);
		// add the new process into membership list
		long recvHb = *(long *)(data + sizeof(MessageHdr) + sizeof(recvAddr.addr) + 1);
		int recvID = *(int *)&recvAddr.addr;
		short recvPort = *(short *)&recvAddr.addr[4];
		MemberListEntry recvEntry(recvID, recvPort, recvHb, memberNode->heartbeat);
		memberNode->memberList.push_back(recvEntry);
		++(memberNode->nnb);
		log->logNodeAdd(&memberNode->addr, &recvAddr);
	} else if (recvMsg->msgType == JOINREP) {
		if (memberNode->inGroup == false) {
			memberNode->inGroup = true;
			log->LOG(&memberNode->addr, "Joining the group...");
		}
		// fetch membership list information from the message
		Address entryAddr;
		memcpy(&entryAddr.addr, data + sizeof(MessageHdr), sizeof(entryAddr.addr));
		long entryHb = *(long *)(data + sizeof(MessageHdr) + sizeof(entryAddr.addr) + 1);
		// add received member entry into membership list
		updateMemberList(entryAddr, entryHb);	
	} else {
		// fetch heartbeat update information from the message
		Address updateAddr;
		memcpy(&updateAddr.addr, data + sizeof(MessageHdr), sizeof(updateAddr.addr));
		long updateHb = *(long *)(data + sizeof(MessageHdr) + sizeof(updateAddr.addr) + 1);
		// update membership list with gossip heartbeat
		updateMemberList(updateAddr, updateHb);	
	}
	free(recvMsg);
	return true;
}

/**
 * FUNCTION NAME: sendMemberList
 *
 * DESCRIPTION: Send member list entries not listed in the blacklist to the destination
 */
void MP1Node::sendMemberList(const MsgTypes type, Address *destAddr, set<int> invalid) {
	for (vector<MemberListEntry>::iterator iter = memberNode->memberList.begin();
		iter != memberNode->memberList.end(); ++iter) {
		if (invalid.count(iter - memberNode->memberList.begin())) {
			continue;
		}
		MessageHdr *sendMsg;
		size_t sendMsgSize = sizeof(MessageHdr) + sizeof(Address) + sizeof(long) + 1;
		sendMsg = (MessageHdr *)malloc(sendMsgSize * sizeof(char));
		sendMsg->msgType = type;
		Address entryAddr;
		memset(&entryAddr, 0, sizeof(Address));
		*(int *)(&entryAddr.addr) = iter->id;
		*(short *)(&entryAddr.addr[4]) = iter->port;
		memcpy((char *)(sendMsg+1), &entryAddr.addr, sizeof(entryAddr.addr));
		memcpy((char *)(sendMsg+1) + 1 + sizeof(entryAddr.addr), &iter->heartbeat, sizeof(long));
		emulNet->ENsend(&memberNode->addr, destAddr, (char *)sendMsg, sendMsgSize);
		free(sendMsg);
	}
}

/**
 * FUNCTION NAME: updateMemberList
 *
 * DESCRIPTION: update or create a member list entry
 */
void MP1Node::updateMemberList(Address updAddr, long updHb) {
	int updateID = *(int *)&updAddr.addr;
	short updatePort = *(short *)&updAddr.addr[4];
	bool exist(false);
	// update current entry if member already exist
	for (vector<MemberListEntry>::iterator iter = memberNode->memberList.begin();
		iter != memberNode->memberList.end(); ++iter) {
		if (updateID == iter->id && updatePort == iter->port) {
			exist = true;
			if (updHb > iter->heartbeat) {
				iter->heartbeat = updHb;
				iter->timestamp = memberNode->heartbeat;
			}
			break;
		}
	}
	// insert the entry as a new member if not found
	if (exist == false) {
		MemberListEntry updateEntry(updateID, updatePort, updHb, memberNode->heartbeat);
		memberNode->memberList.push_back(updateEntry);
		++(memberNode->nnb);
		log->logNodeAdd(&memberNode->addr, &updAddr);
	}
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
	++(memberNode->heartbeat);
	if (memberNode->inGroup == false) {
		return;
	}
	// Sort entire membership list and update self pointer
	if (memberNode->memberList.size()) {
		// Sort the membership list based on process address
		sort(memberNode->memberList.begin(), memberNode->memberList.end(), compMemberEntry);
		// update self-pointer to map the right entry in membership list
		int myID = *(int *)&memberNode->addr.addr;
		int myPort = *(short *)&memberNode->addr.addr[4];
		for (vector<MemberListEntry>::iterator iter = memberNode->memberList.begin();
						iter != memberNode->memberList.end(); ++iter) {
			if (myID == iter->id && myPort == iter->port) {
				memberNode->myPos = iter;
			}
		}
	}
	// check all membership and remove if anyone timeout for 2 * TFAIL, mark if anyone timeout for TFAIL
	int last(memberNode->nnb - 1), index(0);
	set<int> fails;
	while (index <= last) {
		if (memberNode->memberList[index].timestamp >= (memberNode->heartbeat - TFAIL)) {
			// member's heartbeat period is in range
			++index;
		} else if (memberNode->memberList[index].timestamp >= (memberNode->heartbeat - TREMOVE)) {
			// member's heartbeat already timeout for TFAIL but not reach TREMOVE 
			fails.insert(index);
			++index;
		} else {
			// member's heartbeat timeout for TREMOVE 
			MemberListEntry tmp = memberNode->memberList[last];
			memberNode->memberList[last] = memberNode->memberList[index];
			memberNode->memberList[index] = tmp;
			--last;
		}
	}
	// remove failures
	int rmCnt(memberNode->nnb - 1 - last); 
	for (int i = 0; i < rmCnt; ++i) {
		Address removeAddr;
		memset(&removeAddr, 0, sizeof(Address));
		*(int *)(&removeAddr.addr) = memberNode->memberList.back().id;
		*(short *)(&removeAddr.addr[4]) = memberNode->memberList.back().port;
		memberNode->memberList.pop_back();
		--(memberNode->nnb);
		log->logNodeRemove(&memberNode->addr, &removeAddr);
	}
	// gossip membership list entries to 3 random nodes, skip member if it timeout for TFAIL
	set<int> destIndex;
	int sdCnt(0), myIndex;
	myIndex = memberNode->myPos - memberNode->memberList.begin();
	// randomly find 3 valid member to send membershiplist
	while (sdCnt < min(3, memberNode->nnb - 1 - (int)fails.size())) {
		index = rand() % memberNode->nnb;
		if (destIndex.count(index) || fails.count(index) || index == myIndex) {
			continue;
		}
		destIndex.insert(index);
		++sdCnt;
	}
	// update self-entry in the membership list before Gossip
	memberNode->myPos->heartbeat = memberNode->myPos->timestamp = memberNode->heartbeat;
	for (set<int>::iterator iter = destIndex.begin(); iter != destIndex.end(); ++iter) {
		Address destAddr;
		memset(&destAddr, 0, sizeof(Address));
		*(int *)(&destAddr.addr) = memberNode->memberList[*iter].id;
		*(short *)(&destAddr.addr[4]) = memberNode->memberList[*iter].port;
		sendMemberList(GOSSIPHB, &destAddr, fails);
	}
	return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
	Address joinaddr;
	memset(&joinaddr, 0, sizeof(Address));
	*(int *)(&joinaddr.addr) = 1;
	*(short *)(&joinaddr.addr[4]) = 0;
	return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) {
	printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2], addr->addr[3], *(short*)&addr->addr[4]);
}
