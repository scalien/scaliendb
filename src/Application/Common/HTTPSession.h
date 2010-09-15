#ifndef HTTPSESSION_H
#define HTTPSESSION_H

#include "Application/HTTP/JSONSession.h"

#define HTTP_MATCH_COMMAND(cmd, csl) \
	((sizeof(csl) == 1 && cmd.GetLength() == 0) || \
	(memcmp(cmd.GetBuffer(), csl, MIN(cmd.GetLength(), sizeof(csl) - 1)) == 0))

#define HTTP_GET_OPT_PARAM(params, name, var) \
	params.GetNamed(name, sizeof("" name) - 1, var)

#define HTTP_GET_PARAM(params, name, var) \
	if (!params.GetNamed(name, sizeof("" name) - 1, var)) { return NULL; }

#define HTTP_GET_U64_PARAM(params, name, var) \
	{ \
		ReadBuffer	tmp; \
		unsigned	nread; \
		if (!params.GetNamed(name, sizeof("" name) - 1, tmp)) \
			return NULL; \
		var = BufferToUInt64(tmp.GetBuffer(), tmp.GetLength(), &nread); \
		if (nread != tmp.GetLength()) \
			return NULL; \
	}

class UrlParam;			// forward
class HTTPConnection;	// forward
class HTTPRequest;		// forward

/*
===============================================================================

 HTTPSession
 
===============================================================================
*/

class HTTPSession
{
public:
	enum Type
	{
		PLAIN,
		HTML,
		JSON
	};

	void				SetConnection(HTTPConnection* conn);
	bool				ParseRequest(HTTPRequest& request, ReadBuffer& cmd, UrlParam& params);
	void				ParseType(ReadBuffer& cmd);

	void				ResponseFail();

	HTTPConnection*		conn;
	Type				type;
	JSONSession			json;
}; 

#endif
