#ifndef HTTP_FILE_HANDLER_H
#define HTTP_FILE_HANDLER_H

#include "HttpServer.h"

/*
===============================================================================

 HttpFileHandler serves static files over HTTP.

===============================================================================
*/

class HttpFileHandler : public HttpHandler
{
public:
	HttpFileHandler(const char* docroot, const char* prefix);
	
	virtual bool	HandleRequest(HttpConn* conn, HttpRequest& request);
	
private:
	const char*		documentRoot;
	const char*		prefix;
};

#endif
