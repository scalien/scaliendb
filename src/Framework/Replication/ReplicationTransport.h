#ifndef REPLICATIONTRANSPORT_H
#define REPLICATIONTRANSPORT_H

/*
===============================================================================

 ReplicationTransport

===============================================================================
*/

class ReplicationTransport
{
public:

	void		SendMessage(unsigned nodeID, const ReplicationMessage* msg);
	void		BroadcastMessage(const ReplicationMessage* msg);
};

#endif
