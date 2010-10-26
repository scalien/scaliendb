#include "ClientRequestCache.h"

static ClientRequestCache* clientRequestCache = NULL;

ClientRequestCache::ClientRequestCache()
{
    maxSize = 0;
}

ClientRequestCache* ClientRequestCache::Get()
{
    if (clientRequestCache == NULL)
        clientRequestCache = new ClientRequestCache;
    
    return clientRequestCache;
}
    
void ClientRequestCache::Init(uint64_t size)
{
    assert(maxSize == 0);

    maxSize = size;
}

void ClientRequestCache::Shutdown()
{
    assert(maxSize != 0);

    // TODO: memleak
//    freeRequests.DeleteList();

    delete clientRequestCache;
    clientRequestCache = NULL;
}

ClientRequest* ClientRequestCache::CreateRequest()
{
    ClientRequest*  request;
    
    if (freeRequests.GetLength() > 0)
    {
        request = freeRequests.First();
        freeRequests.Remove(request);
        request->Init();
        return request;
    }
    
    return new ClientRequest;
}

void ClientRequestCache::DeleteRequest(ClientRequest* request)
{
    if (freeRequests.GetLength() < maxSize)
    {
        freeRequests.Append(request);
        return;
    }
    
    delete request;
}

