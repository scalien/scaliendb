#ifndef DATANODE_H
#define DATANODE_H

#include "DataChunkContext.h"
#include "Application/HTTP/HTTPServer.h"
#include "Application/HTTP/UrlParam.h"
#include "Application/Controller/ClusterMessage.h"
#include "DataMessage.h"
#include "DataService.h"

class DataNode : public HTTPHandler, public DataService
{
public:
	void					Init(Table* table);

	// HTTPHandler interface
	virtual bool			HandleRequest(HTTPConnection* conn, HTTPRequest& request);
	
	// DataService interface
	virtual void			OnComplete(DataMessage* msg, bool status);

private:
	bool					ProcessCommand(HTTPConnection* conn, const char* cmd, 
							 unsigned cmdlen, UrlParam& params);
	bool					MatchString(const char* s1, unsigned len1, 
							 const char* s2, unsigned len2);

	void					PrintHello(HTTPConnection* conn);

	Table*					table;
	uint64_t				nodeID;
};

#endif
