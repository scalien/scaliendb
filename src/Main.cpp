#include <stdio.h>

#include "Application/HTTP/HttpServer.h"
#include "Application/HTTP/HttpConsts.h"
#include "System/Events/EventLoop.h"
#include "System/Containers/HashTable.h"

size_t Hash(const char* str)
{
	size_t h;
	unsigned char *p;
	
	h = 0;
	for (p = (unsigned char *)str; *p != '\0'; p++)
		h = 37 * h + *p;
		
	return h;
}

class SimpleHandler : public HttpHandler
{
public:
	virtual bool	HandleRequest(HttpConn* conn, const HttpRequest& request);
};

bool SimpleHandler::HandleRequest(HttpConn* conn, const HttpRequest& /*request*/)
{
	conn->Response(HTTP_STATUS_CODE_OK, "hello", 5);
	return true;
}

int main(void)
{
//	configFile.Init("");
	
	HttpServer		httpServer;
	SimpleHandler	handler;
	HashTable<const char*, const char*> testHash;
	const char**	pvalue;
		
	Log_SetTarget(LOG_TARGET_STDOUT);
	Log_SetTrace(true);
	Log_SetTimestamping(true);
	
	testHash.Set("hello", "world");
	testHash.Set("hol", "peru");
	pvalue = testHash.Get("hol");
	if (pvalue)
		Log_Trace("hol = %s", *pvalue);
	
	IOProcessor::Init(1024);
	
	httpServer.Init(8080);
	httpServer.RegisterHandler(&handler);

	EventLoop::Init();
	EventLoop::Run();
	EventLoop::Shutdown();
}
