#ifndef DATAHTTPHANDLER_H
#define DATAHTTPHANDLER_H

#include "Application/HTTP/HTTPServer.h"
#include "DataMessage.h"
#include "DataService.h"

class DataNode;
class UrlParam;

/*
===============================================================================

 DataHTTPHandler

===============================================================================
*/

class DataHTTPHandler : public HTTPHandler, public DataService
{
public:
	void				Init(DataNode* dataNode);
	
	// HTTPHandler interface
	virtual bool		HandleRequest(HTTPConnection* conn, HTTPRequest& request);

	// DataService interface
	virtual void		OnComplete(DataMessage* msg, bool status);

private:
	bool				ProcessCommand(HTTPConnection* conn, const char* cmd, 
						 unsigned cmdlen, UrlParam& params);
	bool				MatchString(const char* s1, unsigned len1, 
						 const char* s2, unsigned len2);

	void				PrintHello(HTTPConnection* conn);

	DataNode*			dataNode;
};

#endif
