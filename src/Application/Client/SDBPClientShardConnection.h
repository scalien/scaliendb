#ifndef SDBPCLIENTSHARDCONNECTION_H
#define SDBPCLIENTSHARDCONNECTION_H

#include "System/IO/Endpoint.h"
#include "Framework/Messaging/MessageConnection.h"
#include "Application/Common/ClientRequest.h"

class SDBPClient;	// forward

/*
===============================================================================================================

 SDBPClientShardConnection

===============================================================================
*/

class SDBPClientShardConnection : public MessageConnection
{
public:
	void			Init(SDBPClient* client, uint64_t nodeID, Endpoint& endpoint);
	void			Connect();
	void			Send(ClientRequest* request);

	uint64_t		GetNodeID();
	Endpoint&		GetEndpoint();

private:
	uint64_t		nodeID;
	Endpoint		endpoint;
	uint64_t		quorumID;
};

#endif
