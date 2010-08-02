#ifndef CONTROLLER_H
#define CONTROLLER_H

#include "ControlConfigContext.h"
#include "ControlChunkContext.h"
#include "Application/HTTP/HttpServer.h"
#include "ClusterMessage.h"
#include "ConfigMessage.h"

class Controller : public HttpHandler
{
public:
	void					Init(Table* table);
	bool					ReadChunkQuorum(uint64_t /*chunkID*/);
	void					WriteChunkQuorum(const ConfigMessage& msg);
	// HttpHandler interface
	virtual bool			HandleRequest(HttpConn* conn, const HttpRequest& request);
	
	void					OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
	void					OnConfigMessage(const ConfigMessage& msg);

	void					OnPrimaryTimeout();

private:	
	uint64_t				numNodes;
	uint64_t				nodeIDs[7];
	Endpoint				endpoints[7];
	bool					chunkCreated;
	Table*					table;
	Countdown				primaryTimeout;
};

#endif
