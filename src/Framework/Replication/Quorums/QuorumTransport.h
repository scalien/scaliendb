#ifndef QUORUMTRANSPORT_H
#define QUORUMTRANSPORT_H

#include "ReplicationMessage.h"

/*
===============================================================================

 QuorumTransport

===============================================================================
*/

class QuorumTransport
{
public:
	virtual ~QuorumTransport() {}
	
	virtual ReplicationMessage*	GetMessage() const											= 0;
	virtual void				SendMessage(unsigned nodeID, const ReplicationMessage* msg)	= 0;
	virtual void				BroadcastMessage(const ReplicationMessage* msg)				= 0;
};

#endif
