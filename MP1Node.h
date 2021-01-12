/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 *                 Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"
#include <unordered_set>
#include <unordered_map>

/**
 * Macros
 */
#define TREMOVE 30
#define TFAIL 5
#define TPING 3
#define DELEGATIONSIZE 2
#define TSTALE 20


/*
* Note: You can change/add any functions in MP1Node.{h,cpp}
*/

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    HEARTBEAT,
    PING,
    ACK,
    PINGREQ,
    DELEGATEDPING,
    ACKDELEGATEDPING,
    ACKPRINGREQ
};

/* protocols */
enum Protocol {
    SWIM,
    ATA,
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
    enum MsgTypes msgType;
}MessageHdr;

struct MemberChanges {
    MemberListEntry entry;
    size_t count;
};

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode; // info about self
	char NULLADDR[6];
    map<int,bool> status;
    Protocol protocol = ATA;

    // node ids to timeout counters
    std::unordered_map<int, MemberChanges> pingedList;

    // updated and failed node
    std::vector<MemberChanges> updatedInfo;
    std::vector<MemberChanges> failedInfo;
    std::unordered_set<int> failedIds;

    // todo: check if necessary
    inline int getSelfId() {
        return *(int *)(&memberNode->addr.addr);
    }

    inline short getSelfPort() {
        return *(short *)(&memberNode->addr.addr[4]);
    }
public:
    MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
    int introduceSelfToGroup(Address *joinAddress);
	void finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable();
	void printAddress(Address *addr);
	virtual ~MP1Node();

    // handlers
    void joinReqHandler(char *data);
    void joinRepHandler(char *data);
    void pingHandler(char *data);
    void ackHandler(char *data);
    void pingReqHandler(char *data);
    void delegatedPingHandler(char *data);
    void ackDelegatedPingHandler(char *data);
    void heartbeatHandler(char *data);

    // ping
    void pingAll();
    void pingRandom();

    // helpers
    void dealWithTimeout();
    void incrementCounts();
    void removeStaleInfo();
    void addMemberInfoToMessage(MessageHdr *msg, size_t offset);
    void readMemberInfoFromMessage(char *data, size_t offset);
    void addMemberToInfoList(std::vector<MemberChanges>& list, MemberListEntry& node);
    MemberListEntry* recordMember(int id, short port);
    void recordMemberIfMissing(int id, short port);
    int updateMemberFromMessage(size_t updateSize, char *data, size_t offset);
    int failMemberFromMessage(size_t updateSize, char *data, size_t offset);
    void UpdateMember(int nodeId, short port, long heartbeat);
    MemberListEntry* getMember(int id);
    Address getAddress(int id, short port);
};

#endif /* _MP1NODE_H_ */
