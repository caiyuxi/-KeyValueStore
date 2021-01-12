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

void MP2Node::sendMessage(Message* message, Address* toAddress) {
	string msgStr = message->toString();
	char* charArray = (char*)msgStr.c_str();
    this->emulNet->ENsend(&(this->memberNode->addr), toAddress, charArray, strlen(charArray));  
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
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if (ring.size() == 0 || ring.size() != curMemList.size()) {
		ring = curMemList;
		stabilizationProtocol();
	}
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
	/*
	 * Implement this
	 */
	this->log->LOG(&this->memberNode->addr, "here");
	 Message* msg = new Message(g_transID, memberNode->addr, CREATE, key, value);
	 quorum[msg->transID]=new QuorumInfo(msg);
	 g_transID++;
	 vector<Node> replicas = findNodes(key);
	 string msgStr = msg->toString();
	 
	 // send a request to each replica
	 for(auto replica : replicas) {
		sendMessage(msg, replica.getAddress());
	 }
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
	/*
	 * Implement this
	 */
	 Message* msg = new Message(g_transID, memberNode->addr, READ, key);
	 quorum[msg->transID]=new QuorumInfo(msg);
	 g_transID++;

	 vector<Node> replicas = findNodes(key);
	 // send a request to each replica
	 for(auto replica : replicas) {
		 sendMessage(msg, replica.getAddress());
	 }
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
	/*
	 * Implement this
	 */
	 Message* msg = new Message(g_transID, memberNode->addr, UPDATE, key, value);
	 quorum[msg->transID]=new QuorumInfo(msg);
	 g_transID++;

	 vector<Node> replicas = findNodes(key);
	 // send a request to each replica
	 for(auto replica : replicas) {
		 sendMessage(msg, replica.getAddress());
	 }
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
	/*
	 * Implement this
	 */
	 Message* msg = new Message(g_transID, memberNode->addr, DELETE, key);
	 quorum[msg->transID]=new QuorumInfo(msg);
	 g_transID++;

	 vector<Node> replicas = findNodes(key);
	 // send a request to each replica
	 for(auto replica : replicas) {
		 sendMessage(msg, replica.getAddress());
	 }
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica=PRIMARY) { // don't differentiate
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	if (key.empty()) {
		return false;
	}
	return ht->create(key, value);

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
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	if (key.empty()) {
		return "";
	}
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica=PRIMARY) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	if (key.empty()) {
		return false;
	}
	return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	return ht->deleteKey(key);
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
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
		recvCallBack(message);

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP2Node::recvCallBack(string msgStr) {
	/*
	 * Your code goes here
	 */
	bool result = false;
	this->log->LOG(&this->memberNode->addr, &msgStr[0]);
	Message message(msgStr);
	Message* response;
	string value;

	switch (message.type) {
	case CREATE:
#ifdef DEBUGLOG
		this->log->LOG(&this->memberNode->addr, "received CREATE");
#endif
		result = createKeyValue(message.key, message.value);
		if (result) {
			log->logCreateSuccess(&memberNode->addr, false, message.transID, message.key, message.value);
		}
		response = new Message(message.transID, memberNode->addr, REPLY, result);
		//emulNet->ENsend(&memberNode->addr,&message.fromAddr ,(char *)response,sizeof(Message));
		sendMessage(response, &message.fromAddr);

		break;
		case TRANSFER:
#ifdef DEBUGLOG
		this->log->LOG(&this->memberNode->addr, "received TRANSFER");
#endif
		result = createKeyValue(message.key, message.value);
		if (result) {
			log->logCreateSuccess(&memberNode->addr, false, message.transID, message.key, message.value);
		}
		break;
	case READ:
#ifdef DEBUGLOG
		this->log->LOG(&this->memberNode->addr, "received READ");
#endif
		value = readKey(message.key);
		response = new Message(message.transID, memberNode->addr, READREPLY, message.key, value);
		if (!value.empty()) {
			log->logReadSuccess(&memberNode->addr, false, message.transID, message.key, value);
		} else {
			log->logReadFail(&memberNode->addr, true, message.transID, message.key);
		}
		sendMessage(response, &message.fromAddr);
		break;
	case UPDATE:
#ifdef DEBUGLOG
		this->log->LOG(&this->memberNode->addr, "received UPDATE");
#endif
		result = updateKeyValue(message.key, message.value);
		if (result) {
			log->logUpdateSuccess(&memberNode->addr, false, message.transID, message.key, message.value);
		} else {
			log->logUpdateFail(&memberNode->addr, false, message.transID, message.key, message.value);
		}
		response = new Message(message.transID, memberNode->addr, REPLY, result);
		sendMessage(response, &message.fromAddr);
		break;
	case DELETE:
#ifdef DEBUGLOG
		this->log->LOG(&this->memberNode->addr, "received DELETE");
#endif
		result = deletekey(message.key);
		if (result) {
			log->logDeleteSuccess(&memberNode->addr, false, message.transID, message.key);
		} else {
			log->logDeleteFail(&memberNode->addr, false, message.transID, message.key);
		}
		response = new Message(message.transID, memberNode->addr, REPLY, result);
		sendMessage(response, &message.fromAddr);
		break;
	case REPLY:
#ifdef DEBUGLOG
		this->log->LOG(&this->memberNode->addr, "received REPLY");
#endif
		if (message.success) {
			quorum[message.transID]->size++;
			if(quorum[message.transID]->size==2) {
				MessageType type = quorum[message.transID]->msg->type;
				string key = quorum[message.transID]->msg->key;
				string value = quorum[message.transID]->msg->value;
				switch(type) {
					case CREATE:
						log->logCreateSuccess(&memberNode->addr, true, message.transID, key, value);
						break;
					case UPDATE:
						log->logUpdateSuccess(&memberNode->addr, true, message.transID, key, value);
						break;
					case DELETE:
						log->logDeleteSuccess(&memberNode->addr, true, message.transID, key);
						break;
					default:
						break;
				}
			}
		} else {
			quorum[message.transID]->size--;
			if(quorum[message.transID]->size==-2) {
				MessageType type = quorum[message.transID]->msg->type;
				string key = quorum[message.transID]->msg->key;
				string value = quorum[message.transID]->msg->value;
				switch(type) {
					case CREATE:
						log->logCreateFail(&memberNode->addr, true, message.transID, key, value);
						break;
					case UPDATE:
						log->logUpdateFail(&memberNode->addr, true, message.transID, key, value);
						break;
					case DELETE:
						log->logDeleteFail(&memberNode->addr, true, message.transID, key);
						break;
					default:
						break;
				}
			}
		}

		break;
	case READREPLY:
#ifdef DEBUGLOG
		this->log->LOG(&this->memberNode->addr, "received READREPLY");
#endif
		quorum[message.transID]->size++;

		if(quorum[message.transID]->size==2)
		{
			string key = quorum[message.transID]->msg->key;
			if (!message.value.empty()) {
				log->logReadSuccess(&memberNode->addr, true, message.transID, key, message.value);
			} else {
				log->logReadFail(&memberNode->addr, true, message.transID, key);
			}
		}
		break;
	default:
		result = false;
		break;
	}

	return result;
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
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
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
	 for (auto kv = ht->hashTable.begin(); kv != ht->hashTable.end(); kv++){
		 if (kv->first.empty()) {
			 continue;
		 }
        vector<Node> replicas = findNodes(kv->first);
		Message* msg = new Message(g_transID, memberNode->addr, TRANSFER, kv->first, kv->second);
        for(auto replica : replicas) {
			if (replica.getAddress()->getAddress() != this->memberNode->addr.getAddress()) {
				sendMessage(msg, replica.getAddress());
			}
		}
    }

	for(int i = checkpoint; i < g_transID; i++) {
		if(quorum.count(i)>0 && (quorum [i]->size<2 && quorum[i]->size>-2)) {
			static char temp[100];
			sprintf(temp, "failing quorum %d with count %d ", i, quorum [i]->size);
			this->log->LOG(&(this->memberNode->addr), temp);
				MessageType type = quorum[i]->msg->type;
				string key = quorum[i]->msg->key;
				string value = quorum[i]->msg->value;
				switch(type) {
					case CREATE:
						log->logCreateFail(&memberNode->addr, true, i, key, value);
						break;
					case UPDATE:
						log->logUpdateFail(&memberNode->addr, true, i, key, value);
						break;
					case DELETE:
						log->logDeleteFail(&memberNode->addr, true, i, key);
						break;
					case READ:
						log->logReadFail(&memberNode->addr, true, i, key);
						break;
					default:
						break;
				}
			}
	}
	checkpoint = g_transID;

}
