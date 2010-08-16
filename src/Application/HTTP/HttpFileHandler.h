#ifndef HTTP_FILE_HANDLER_H
#define HTTP_FILE_HANDLER_H

#include "HTTPServer.h"

/*
===============================================================================

 HTTPFileHandler serves static files over HTTP.

===============================================================================
*/

class HTTPFileHandler : public HTTPHandler
{
public:
	HTTPFileHandler(const char* docroot, const char* prefix);
	
	virtual bool	HandleRequest(HTTPConnection* conn, HTTPRequest& request);
	
private:
	const char*		documentRoot;
	const char*		prefix;
};

#endif
