#ifndef CLUSTERTRANSPORT_H
#define CLUSTERTRANSPORT_H

#include "Framework/Messaging/Message.h"
#include "ClusterServer.h"

class ClusterTransport
{
public:
	virtual ~ClusterTransport() {}
	
	void						Init(uint64_t nodeID, Endpoint& endpoint);
	
	uint64_t					GetNodeID();
	Endpoint&					GetEndpoint();
	void						AddEndpoint(uint64_t nodeID, Endpoint endpoint);
	
	void						SendMessage(uint64_t nodeID, Buffer& prefix, Message& msg);
	void						SendPriorityMessage(uint64_t nodeID, Buffer& prefix, Message& msg);
	
	virtual void				OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)	= 0;
	virtual void				OnMessage(ReadBuffer msg)										= 0;

private:
	// for ClusterConnection:
	void						AddConnection(ClusterConnection* conn);
	ClusterConnection*			GetConnection(uint64_t nodeID);
	void						DeleteConnection(ClusterConnection* conn);

	uint64_t					nodeID;
	Endpoint					endpoint;
	Buffer						msgBuffer;
	ClusterServer				server;
	InList<ClusterConnection>	conns;

	friend class ClusterConnection;
};

#endif
