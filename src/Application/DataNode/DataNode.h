#ifndef DATANODE_H
#define DATANODE_H

#include "DataChunkContext.h"
#include "Application/HTTP/HttpServer.h"
#include "ClusterMessage.h"

class DataNode : public HttpHandler
{
public:
	void					Init(Table* table);

	// HttpHandler interface
	virtual bool			HandleRequest(HttpConn* conn, const HttpRequest& request);

private:
	Table*					table;
	uint64_t				nodeID;
};

#endif
