#ifndef REPLICATIONTRANSPORT_H
#define REPLICATIONTRANSPORT_H

class ReplicationMessage; // forward

/*
===============================================================================

 ReplicationTransport

===============================================================================
*/

class ReplicationTransport
{
public:

	void					SendMessage(unsigned nodeID, const ReplicationMessage& msg);
	void					SendPriorityMessage(unsigned nodeID, const ReplicationMessage& msg);

	void					BroadcastMessage(const ReplicationMessage& msg);
	void					BroadcastPriorityMessage(const ReplicationMessage& msg);
};

#endif
