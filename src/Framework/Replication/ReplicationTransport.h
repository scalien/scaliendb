#ifndef REPLICATIONTRANSPORT_H
#define REPLICATIONTRANSPORT_H

#include "System/Containers/List.h"
#include "Framework/Clustering/ClusterTransport.h"

/*
===============================================================================

 ReplicationTransport

===============================================================================
*/

class ReplicationTransport : public ClusterTransport
{
public:
	void			RegisterConnectionEvents(uint64_t contextID);
	
private:
	virtual void	OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
	virtual void	OnMessage(ReadBuffer msg);

	List<uint64_t>	registered;
};

#endif
