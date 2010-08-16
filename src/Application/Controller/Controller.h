#ifndef CONTROLLER_H
#define CONTROLLER_H

#include "ControlConfigContext.h"
#include "ControlChunkContext.h"
#include "System/Events/Countdown.h"
#include "Application/HTTP/HTTPServer.h"
#include "ClusterMessage.h"
#include "ConfigMessage.h"

class Controller : public HTTPHandler
{
public:
	void					Init(Table* table);
	void					ReadChunkQuorum(uint64_t /*chunkID*/);
	void					WriteChunkQuorum(ConfigMessage& msg);
	// HTTPHandler interface
	virtual bool			HandleRequest(HTTPConnection* conn, HTTPRequest& request);
	
	void					OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
	void					OnConfigMessage(ConfigMessage& msg);
	void					OnSendClusterInfo();

private:	
	uint64_t				numNodes;
	uint64_t				nodeIDs[7];
	Endpoint				endpoints[7];
	bool					chunkCreated;
	Table*					table;
	Countdown				clusterInfoTimeout;
};

#endif
