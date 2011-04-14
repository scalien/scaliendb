#ifndef CLIENTREQUESTCACHE_H
#define CLIENTREQUESTCACHE_H

#include "System/Platform.h"
#include "ClientRequest.h"

#define REQUEST_CACHE (ClientRequestCache::Get())

class ClientRequestCache
{
public:
    static ClientRequestCache*  Get();
    
    void                        Init(uint64_t size);
    void                        Shutdown();

    ClientRequest*              CreateRequest();
    void                        DeleteRequest(ClientRequest* request);

    unsigned                    GetNumFreeRequests();

private:
    ClientRequestCache();

    unsigned                    maxSize;
    InList<ClientRequest>       freeRequests;
};

#endif
