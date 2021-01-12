/**********************************
* FILE NAME: MP1Node.cpp
*
* DESCRIPTION: Membership protocol run by this Node.
*     Definition of MP1Node class functions.
**********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */
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
 *     This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
	if ( memberNode->bFailed ) {
		return false;
	}
	else {
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
 *     All initializations routines for a member.
 *     Called by the application layer.
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
	/*
	 * This function is partially implemented and may require changes
	 */

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
	// node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
	initMemberListTable();

	return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 * JOINREQ | memberNode->addr | memberNode->heartbeat
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
	static char s[1024];
#endif

	if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
		// I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "Starting up group...");
#endif
		memberNode->inGroup = true;
	}
	else {
		size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
		msg = (MessageHdr *) malloc(msgsize * sizeof(char));

		// create JOINREQ message: format of data is {struct Address myaddr}
		msg->msgType = JOINREQ;
		memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
		memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
		sprintf(s, "Trying to join...");
		log->LOG(&memberNode->addr, s);
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
void MP1Node::finishUpThisNode(){
	/*
	 * Your code goes here
	 */
	this->memberNode->bFailed = true;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 *     Check your messages in queue and perform membership protocol duties
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
 void MP1Node::initMemberListTable() {
     this->memberNode->memberList.clear();

     MemberListEntry node(getSelfId(), getSelfPort(), this->memberNode->heartbeat, this->memberNode->timeOutCounter);
     this->memberNode->memberList.push_back(node);

     this->log->logNodeAdd(&(this->memberNode->addr), &(this->memberNode->addr));
 }

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
	printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
	       addr->addr[3], *(short*)&addr->addr[4]);
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
	bool result = true;
	MessageHdr receivedMessageHeader;
	memcpy(&receivedMessageHeader, data, sizeof(MessageHdr));

	switch (receivedMessageHeader.msgType) {
	case JOINREQ:
		this->log->LOG(&this->memberNode->addr, "received JOINREQ");

		joinReqHandler(data + sizeof(MessageHdr));
		break;
	case JOINREP:
		this->log->LOG(&this->memberNode->addr, "received JOINREP");

		this->memberNode->inGroup = true;
		joinRepHandler(data + sizeof(MessageHdr));
		break;
	case PING:
		this->log->LOG(&this->memberNode->addr, "received PING");

		pingHandler(data + sizeof(MessageHdr));
		break;
	case ACK:
	case ACKPRINGREQ:
		this->log->LOG(&this->memberNode->addr, "received ACK/ACKPRINGREQ");

		ackHandler(data + sizeof(MessageHdr));
		break;
	case PINGREQ:
		this->log->LOG(&this->memberNode->addr, "received PINGREQ");

		pingReqHandler(data + sizeof(MessageHdr));
		break;
	case ACKDELEGATEDPING:
		this->log->LOG(&this->memberNode->addr, "received ACKDELEGATEDPING");

		ackDelegatedPingHandler(data + sizeof(MessageHdr));
		break;
	case DELEGATEDPING:
		this->log->LOG(&this->memberNode->addr, "received DELEGATEDPING");

		delegatedPingHandler(data + sizeof(MessageHdr));
		break;
	case HEARTBEAT:
		this->log->LOG(&this->memberNode->addr, "received HEARTBEAT");

		heartbeatHandler(data + sizeof(MessageHdr));
		break;
	default:
		result = false;
		break;
	}

	return result;
}


/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete the nodes
 *                 Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    if (this->memberNode->pingCounter > 0) {
        this->memberNode->pingCounter--;
    } else {
        this->memberNode->pingCounter = TPING;
        this->memberNode->heartbeat++;
		switch (protocol) {
			case ATA:
				pingAll();
				break;
			case SWIM:
				pingRandom();
		}
    }

    dealWithTimeout();
    
    // update self info
    this->memberNode->timeOutCounter++;

    MemberListEntry *self = getMember(getSelfId());
    self->setheartbeat(this->memberNode->heartbeat);
    self->settimestamp(this->memberNode->timeOutCounter);
}

// handling time-out nodes

void MP1Node::dealWithTimeout() {
    switch (protocol) {
        case ATA:
            // simply remove timeout members
            for(auto it = this->memberNode->memberList.begin(); it != this->memberNode->memberList.end();) {
                if(this->memberNode->timeOutCounter - it->gettimestamp() > TREMOVE) {
                    it = this->memberNode->memberList.erase(it);

                    cout<<this->memberNode->timeOutCounter<<" timing out "<<it->getid()<<":"<<it->getport()<< "with heart beat "<<it->gettimestamp()<<" and "<<it->getheartbeat()<<endl;
                    Address memberAddress = getAddress(it->getid(), it->getport());
                    this->log->logNodeRemove(&(this->memberNode->addr), &memberAddress);
                }
                else {
                    ++it;
                }
            }
            break;
        case SWIM:
            // case 1: not responding after TREMOVE, remove from member list
            // case 2: not responding after TFAIL, mark as a failed node + try to ping indirectly from other nodes
            for(auto it = this->memberNode->memberList.begin(); it != this->memberNode->memberList.end();) {
                auto mapIt = this->pingedList.find(it->getid());
                if (mapIt == this->pingedList.end()) {
                    it++;
                    continue;
                }

                // remove
                if(this->memberNode->timeOutCounter - mapIt->second.count > TREMOVE) {
                    addMemberToInfoList(this->failedInfo, *it);
                    failedIds.insert(it->id);
                    this->pingedList.erase(mapIt);
                    it = this->memberNode->memberList.erase(it);

                    Address memberAddress = getAddress(it->getid(), it->getport());
                    this->log->logNodeRemove(&(this->memberNode->addr), &memberAddress);
                } else if (this->memberNode->timeOutCounter - mapIt->second.count > TFAIL) {
                    // try to ping from other nodes
                    std::unordered_set<int> indices;
                    int maxRandIndices = std::min(DELEGATIONSIZE, static_cast<int>(this->memberNode->memberList.size()) - static_cast<int>(this->pingedList.size() - 1));
                    while (static_cast<int>(indices.size()) < maxRandIndices) {
                        int idx = rand() % this->memberNode->memberList.size();
                        MemberListEntry randomNode = this->memberNode->memberList[idx];
                        if (this->memberNode->memberList[idx].getid() != getSelfId() && this->pingedList.find(randomNode.getid()) == this->pingedList.end()) {
                            indices.insert(idx);
                        }
                    }

                    size_t msgsize = sizeof(MessageHdr) + 2 * sizeof(int) + 2 * sizeof(short) + 2 * sizeof(size_t) + (this->updatedInfo.size() + this->failedInfo.size()) * sizeof(MemberListEntry);
                    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
                    msg->msgType = PINGREQ;
                    size_t outOffset = 0;

                    // my info
                    int selfId = getSelfId();
                    memcpy((char*)(msg+1) + outOffset, &selfId, sizeof(int));
                    outOffset +=sizeof(int);

                    short selfPort = getSelfPort();
                    memcpy((char*)(msg+1) + outOffset, &selfPort, sizeof(short));
                    outOffset +=sizeof(short);

                    // original sender of the ping
                    int id = it->getid();
                    memcpy((char*)(msg+1) + outOffset, &id, sizeof(int));
                    outOffset +=sizeof(int);

                    short port = it->getport();
                    memcpy((char*)(msg+1) + outOffset, &port, sizeof(short));
                    outOffset +=sizeof(short);

                    addMemberInfoToMessage(msg, outOffset);

                    for (int idx : indices) {
                        MemberListEntry member = this->memberNode->memberList[idx];

                        Address memberAddress = getAddress(member.getid(), member.getport());
                        this->emulNet->ENsend(&(this->memberNode->addr), &memberAddress, (char *)msg, msgsize);
                    }

                    free(msg);
                    it++;
                }
                else {
                    it++;
                }
            }
            incrementCounts();
            removeStaleInfo();
            break;
        default:
            break;
	}
}

/*
 * Handlers
 */

 /*
  * generates the following message in response to JOINREQ
  * note: only include non-faulty members in the list
  * JOINREP | numMemberListEntry | list of MemberListEntry info
  *                                   id | port | heartbeat
  */
void MP1Node::joinReqHandler(char *data) {
    int id;
    short port;
    long heartbeat;
    size_t offset = 0;

    memcpy(&id, data + offset, sizeof(int));
    offset += sizeof(int);

    memcpy(&port, data + offset, sizeof(short));
    offset += sizeof(short);

    memcpy(&heartbeat, data + offset, sizeof(long));

    // add the new neighbour
    MemberListEntry entry(id, port, heartbeat, this->memberNode->timeOutCounter);
    this->memberNode->memberList.push_back(entry);
    addMemberToInfoList(this->updatedInfo, entry);

    Address address = getAddress(entry.getid(), entry.getport());
    this->log->logNodeAdd(&(this->memberNode->addr), &address);

    // reply with JOINREP
    MessageHdr *msg = nullptr;
    size_t numMembers = this->memberNode->memberList.size() - failedIds.size();
    size_t msgsize = sizeof(MessageHdr) + sizeof(size_t) + numMembers * sizeof(MemberListEntry);

    msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = JOINREP;

    memcpy((char *)(msg+1), &numMembers, sizeof(size_t));
    offset = sizeof(size_t);

    for (auto &entry: this->memberNode->memberList) {
        if (failedIds.count(entry.getid())!=0) {
            continue;
        }

        memcpy((char *)(msg+1) + offset, &entry, sizeof(MemberListEntry));
        offset += sizeof(MemberListEntry);
    }
    this->emulNet->ENsend(&(this->memberNode->addr), &address, (char *)msg, msgsize);
    free(msg);
}

/*
 * create membership list entries based on received JOINREP message
 */
void MP1Node::joinRepHandler(char *data) {
    size_t numMembers;
    memcpy(&numMembers, data, sizeof(size_t));

    updateMemberFromMessage(numMembers, data, sizeof(size_t));
}

/*
 * create membership list entry based on received HEARTBEAT message
 */
void MP1Node::heartbeatHandler(char *data) {
    updateMemberFromMessage(1, data, 0);
}

/*
 * generates the following message in response to PING
 * ACK | id | numUpdate | numFailure | list of MemberListEntry for update | list of MemberListEntry for failure
 */
void MP1Node::pingHandler(char *data) {
    int id;
    size_t offset = 0;

    memcpy(&id, data + offset, sizeof(int));
    offset += sizeof(int);

    short port;
    memcpy(&port, data + offset, sizeof(short));
    offset += sizeof(short);

    // record the new info received
    recordMemberIfMissing(id, port);
    readMemberInfoFromMessage(data, offset);

    // reply with updated local info
    size_t msgsize = sizeof(MessageHdr) + sizeof(int) + 2 * sizeof(size_t) + (this->updatedInfo.size() + this->failedInfo.size()) * sizeof(MemberListEntry);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = ACK;

    int selfId = getSelfId();
    memcpy((char*)(msg+1), &selfId, sizeof(int));

    addMemberInfoToMessage(msg, sizeof(int));

    Address address = getAddress(id, port);
    this->emulNet->ENsend(&(this->memberNode->addr), &address, (char *)msg, msgsize);

    free(msg);
}


/*
 * remove sender from the ping waitlist, update local info based on ACK
 */
void MP1Node::ackHandler(char *data) {
    int id;
    memcpy(&id, data, sizeof(int));

    readMemberInfoFromMessage(data, sizeof(int));

    auto it = this->pingedList.find(id);
    if (it != this->pingedList.end()) {
        this->pingedList.erase(it);
    }
}

/*
 * Put info of self and forward the received PINGREQ message to the desired node
 * DELEGATEDPING | delegate id | delegate port | sender id | sender port | list of info from PINGREQ
 */
void MP1Node::pingReqHandler(char *data) {
    int fromID;
    memcpy(&fromID, data, sizeof(int));
    size_t inOffset = sizeof(int);

    short fromPort;
    memcpy(&fromPort, data + inOffset, sizeof(short));
    inOffset += sizeof(short);

    int toID;
    memcpy(&toID, data + inOffset, sizeof(int));
    inOffset += sizeof(int);

    short toPort;
    memcpy(&toPort, data + sizeof(int), sizeof(short));
    inOffset += sizeof(short);

    Address toAddress = getAddress(toID, toPort);

    recordMemberIfMissing(fromID, fromPort);

    size_t updateSize;
    memcpy(&updateSize, data + inOffset, sizeof(size_t));
    inOffset += sizeof(size_t) + updateSize * sizeof(MemberListEntry);

    size_t failureSize;
    memcpy(&failureSize, data + inOffset, sizeof(size_t));


    size_t msgsize = sizeof(MessageHdr) + 2 * sizeof(int) + 2 * sizeof(short)
        + 2 * sizeof(size_t) + (updateSize + failureSize) * sizeof(MemberListEntry);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = DELEGATEDPING;
    size_t outOffset = 0;
    int selfId = getSelfId();
    memcpy((char*)(msg+1) + outOffset, &selfId, sizeof(int));
    outOffset +=sizeof(int);

    short selfPort = getSelfPort();
    memcpy((char*)(msg+1) + outOffset, &selfPort, sizeof(short));
    outOffset +=sizeof(short);

    memcpy((char*)(msg+1) + outOffset, &fromID, sizeof(int));
    outOffset +=sizeof(int);

    memcpy((char*)(msg+1) + outOffset, &fromPort, sizeof(short));
    outOffset +=sizeof(short);

    memcpy((char*)(msg+1) + outOffset, data + outOffset, 2 * sizeof(size_t) + (updateSize + failureSize) * sizeof(MemberListEntry));

    this->emulNet->ENsend(&(this->memberNode->addr), &toAddress, (char *)msg, msgsize);

    free(msg);

}

/*
 * Forward the received ACKDELEGATEDPING message to the desired node
 * ACKPRINGREQ | sender id | list of info from ACKDELEGATEDPING
 */
void MP1Node::ackDelegatedPingHandler(char *data) {
    int toID;
    size_t offset = 0;
    memcpy(&toID, data + offset, sizeof(int));
    offset += sizeof(int);

    short toPort;
    memcpy(&toPort, data + offset, sizeof(short));
    offset += sizeof(short);

    int fromID;
    memcpy(&fromID, data + offset, sizeof(int));
    offset += sizeof(int);

    short fromPort;
    memcpy(&fromPort, data + sizeof(int), sizeof(short));
    offset += sizeof(short);

    Address toAddress = getAddress(toID, toPort);

    recordMemberIfMissing(fromID, fromPort);

    size_t updateSize;
    memcpy(&updateSize, data + offset, sizeof(size_t));
    offset += sizeof(size_t) + updateSize * sizeof(MemberListEntry);

    size_t failureSize;
    memcpy(&failureSize, data + offset, sizeof(size_t));

    size_t msgsize = sizeof(MessageHdr) + sizeof(int) + 2 * sizeof(size_t)
        + (updateSize + failureSize) * sizeof(MemberListEntry);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = ACKPRINGREQ;
    memcpy((char*)(msg+1), &fromID, sizeof(int));

    memcpy((char*)(msg+1) + sizeof(int), data + 2 * sizeof(int) + 2 * sizeof(short),
        2 * sizeof(size_t) + (updateSize + failureSize) * sizeof(MemberListEntry));

    this->emulNet->ENsend(&(this->memberNode->addr), &toAddress, (char *)msg, msgsize);

    free(msg);

}

/*
 * generate the following meeage in response to DELEGATEDPING
 * ACKDELEGATEDPING | delegate id | delegate port | sender id | sender port | list of member info
 */
void MP1Node::delegatedPingHandler(char *data) {
    int delegateID;
    size_t offset = 0;

    memcpy(&delegateID, data + offset, sizeof(int));
    offset += sizeof(int);

    short delegatePort;
    memcpy(&delegatePort, data + offset, sizeof(short));
    offset += sizeof(short);

    int senderID;
    memcpy(&senderID, data + offset, sizeof(int));
    offset += sizeof(int);

    short senderPort;
    memcpy(&senderPort, data + sizeof(int), sizeof(short));
    offset += sizeof(short);


    readMemberInfoFromMessage(data, offset);
    recordMemberIfMissing(delegateID, delegatePort);

    size_t msgsize = sizeof(MessageHdr) + 2 * sizeof(this->memberNode->addr.addr) + 2 * sizeof(size_t) + (this->updatedInfo.size() + this->failedInfo.size()) * sizeof(MemberListEntry);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = ACKDELEGATEDPING;
    size_t outOffset = 0;

    // original sender of the ping
    memcpy((char*)(msg+1) + outOffset, &senderID, sizeof(int));
    outOffset +=sizeof(int);

    memcpy((char*)(msg+1) + outOffset, &senderPort, sizeof(short));
    outOffset +=sizeof(short);

    // my info
    int selfId = getSelfId();
    memcpy((char*)(msg+1) + outOffset, &selfId, sizeof(int));
    outOffset +=sizeof(int);

    short selfPort = getSelfPort();
    memcpy((char*)(msg+1) + outOffset, &selfPort, sizeof(short));
    outOffset +=sizeof(short);

    addMemberInfoToMessage(msg, outOffset);

    Address delegateAddress = getAddress(delegateID, delegatePort);
    this->emulNet->ENsend(&(this->memberNode->addr), &delegateAddress, (char *)msg, msgsize);

    free(msg);

}

/*
 * generate HEARTBEAT message to all nodes
 * HEARTBEAT | id | port | heartbeat | counter
 */
void MP1Node::pingAll() {
	MessageHdr *msg = nullptr;
    size_t msgsize = sizeof(MessageHdr) + sizeof(MemberListEntry);

    msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = HEARTBEAT;

	MemberListEntry entry(getSelfId(), getSelfPort(), this->memberNode->heartbeat, this->memberNode->timeOutCounter);
	memcpy((char *)(msg+1), &entry, sizeof(MemberListEntry));

	for (auto &memberListEntry : this->memberNode->memberList) {
        Address address = getAddress(memberListEntry.getid(), memberListEntry.getport());
        this->emulNet->ENsend(&(this->memberNode->addr), &address, (char *)msg, msgsize);
    }
    free(msg);
}

/*
 * generate PING message to random nodes
 * PING | id | port | numUpdate | numFailure | list of MemberListEntry for update | list of MemberListEntry for failure
 */
void MP1Node::pingRandom() {
    // don't ping if is the only node
    if (this->memberNode->memberList.size() == 1 || this->memberNode->memberList.size() - this->pingedList.size() == 1) {
        return;
    }

    int idx;
    MemberListEntry entry;
    while (true) {
        idx = rand() % this->memberNode->memberList.size();
        entry = this->memberNode->memberList[idx];
        if (entry.getid() != getSelfId() && this->pingedList.find(entry.getid()) == this->pingedList.end()) {
            break;
        }
    }

    size_t msgsize = sizeof(MessageHdr) + sizeof(int) + sizeof(short) + 2 * sizeof(size_t)
                    + (this->updatedInfo.size() + this->failedInfo.size()) * sizeof(MemberListEntry);
    MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));
    msg->msgType = PING;

    size_t offset = 0;

    int id = getSelfId();
    memcpy((char*)(msg+1) + offset, &id, sizeof(int));
    offset += sizeof(int);

    short port = getSelfPort();
    memcpy((char*)(msg+1) + offset, &port, sizeof(short));
    offset += sizeof(short);

    addMemberInfoToMessage(msg, offset);

    Address address = getAddress(entry.getid(), entry.getport());
    this->emulNet->ENsend(&(this->memberNode->addr), &address, (char *)msg, msgsize);

    MemberChanges info;
    info.entry = entry;
    info.count = this->memberNode->timeOutCounter;
    this->pingedList[entry.getid()] = info;

    free(msg);
}

/*
 * Helper functions
 */
void MP1Node::incrementCounts() {
    for (auto& update : this->updatedInfo) {
        update.count++;
    }
    for (auto& update : this->failedInfo) {
        update.count++;
    }
}

void MP1Node::removeStaleInfo() {
    for (auto it = this->updatedInfo.begin(); it != this->updatedInfo.end();) {
        if (it->count >= TSTALE) {
            it = this->updatedInfo.erase(it);
        } else {
            it++;
        }
    }
    for (auto it = this->failedInfo.begin(); it != this->failedInfo.end();) {
        if (it->count >= TSTALE) {
            it = this->failedInfo.erase(it);
            failedIds.erase(it->entry.getid());
        } else {
            it++;
        }
    }
}

void MP1Node::addMemberInfoToMessage(MessageHdr *msg, size_t offset) {
    size_t updateSize = this->updatedInfo.size();
    memcpy((char*)(msg+1) + offset, &updateSize, sizeof(size_t));
    offset += sizeof(size_t);

    size_t failureSize = this->failedInfo.size();
    memcpy((char*)(msg+1) + offset, &failureSize, sizeof(size_t));
    offset += sizeof(size_t);

    for (auto& update : this->updatedInfo) {
        memcpy((char*)(msg+1) + offset, &update.entry, sizeof(MemberListEntry));
        offset += sizeof(MemberListEntry);
    }

    for (auto& update : this->failedInfo) {
        memcpy((char*)(msg+1) + offset, &update.entry, sizeof(MemberListEntry));
        offset += sizeof(MemberListEntry);
    }
}

int MP1Node::updateMemberFromMessage(size_t updateSize, char *data, size_t offset) {
    for (size_t i = 0; i != updateSize; i++) {
        MemberListEntry node;
        memcpy(&node, data + offset, sizeof(MemberListEntry));
        offset += sizeof(MemberListEntry);

        UpdateMember(node.getid(), node.getport(), node.getheartbeat());
    }
    return offset;
}

int MP1Node::failMemberFromMessage(size_t failureSize, char *data, size_t offset) {
    for (size_t i = 0; i != failureSize; i++) {
        MemberListEntry node;
        memcpy(&node, data + offset, sizeof(MemberListEntry));
        offset += sizeof(MemberListEntry);

        for (auto it = this->memberNode->memberList.begin(); it != this->memberNode->memberList.end();) {
            if (it->id == node.getid() && node.getheartbeat() > it->heartbeat) {
                addMemberToInfoList(this->failedInfo, *it);
                failedIds.insert(it->id);
                it = this->memberNode->memberList.erase(it);

                Address memberAddress = getAddress(it->getid(), it->getport());
                this->log->logNodeRemove(&(this->memberNode->addr), &memberAddress);
            } else {
                it++;
            }
        }
    }
    return offset;
}

void MP1Node::readMemberInfoFromMessage(char *data, size_t offset) {
    size_t updateSize;
    memcpy(&updateSize, data + offset, sizeof(size_t));
    offset += sizeof(size_t);

    size_t failureSize;
    memcpy(&failureSize, data + offset, sizeof(size_t));
    offset += sizeof(size_t);

    offset = updateMemberFromMessage(updateSize, data, offset);
    offset = failMemberFromMessage(failureSize, data, offset);
}

void MP1Node::addMemberToInfoList(std::vector<MemberChanges>& list, MemberListEntry& entry) {
    std::vector<MemberChanges>::iterator it;
    for (it = list.begin(); it != list.end(); it++) {
        if (it->entry.getid() == entry.getid()) {
            break;
        }
    }
    MemberChanges update;
    update.entry = entry;
    update.count = 0;

    if (it != list.end()) {
        *it = update;
    } else {
        list.push_back(update);
    }
}

MemberListEntry* MP1Node::recordMember(int id, short port) {
    this->memberNode->memberList.push_back(MemberListEntry(id, port, this->memberNode->timeOutCounter, this->memberNode->timeOutCounter));
    Address address = getAddress(id, port);

    this->log->logNodeAdd(&(this->memberNode->addr), &address);

    return getMember(id);
}


void MP1Node::recordMemberIfMissing(int id, short port) {
    MemberListEntry *member = getMember(id);
    if (!member) {
        recordMember(id, port);
    }
}


void MP1Node::UpdateMember(int id, short port, long heartbeat) {
    MemberListEntry *member = getMember(id);
    if (!member) {
        member = recordMember(id, port);
        addMemberToInfoList(this->updatedInfo, *member);
    } else if (heartbeat > member->heartbeat) {
        member->setheartbeat(heartbeat);
        member->settimestamp(this->memberNode->timeOutCounter);
        addMemberToInfoList(this->updatedInfo, *member);
    }
}

MemberListEntry* MP1Node::getMember(int id) {
    MemberListEntry *result = nullptr;

    for (auto &entry : this->memberNode->memberList) {
        if (id == entry.getid()) {
            result = &entry;
            break;
        }
    }

    return result;
}

Address MP1Node::getAddress(int id, short port) {
    Address address;

    memset(&address, 0, sizeof(Address));
    *(int *)(&address.addr) = id;
    *(short *)(&address.addr[4]) = port;

    return address;
}
