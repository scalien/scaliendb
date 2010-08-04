#ifndef DATANODE_H
#define DATANODE_H

#include "DataChunkContext.h"
#include "Application/HTTP/HttpServer.h"
#include "Application/HTTP/UrlParam.h"
#include "Application/Controller/ClusterMessage.h"
#include "DataMessage.h"
#include "DataService.h"

class DataNode : public HttpHandler, public DataService
{
public:
	void					Init(Table* table);

	// HttpHandler interface
	virtual bool			HandleRequest(HttpConn* conn, const HttpRequest& request);
	
	// DataService interface
	virtual void			OnComplete(DataMessage* msg, bool status);

private:
	bool					ProcessCommand(HttpConn* conn, const char* cmd, 
							 unsigned cmdlen, const UrlParam& params);
	bool					MatchString(const char* s1, unsigned len1, 
							 const char* s2, unsigned len2);

	void					PrintHello(HttpConn* conn);

	Table*					table;
	uint64_t				nodeID;
};

#endif
