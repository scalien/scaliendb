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

	ReplicationMessage*		GetMessage() const;
	void					SendMessage(unsigned nodeID, const ReplicationMessage& msg);
	void					BroadcastMessage(const ReplicationMessage& msg);
};

#endif
