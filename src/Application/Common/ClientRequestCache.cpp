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
    ASSERT(maxSize == 0);

    maxSize = size;
}

void ClientRequestCache::Shutdown()
{
    ASSERT(maxSize != 0);

    freeRequests.DeleteList();

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
        return request;
    }
    
    return new ClientRequest;
}

void ClientRequestCache::DeleteRequest(ClientRequest* request)
{
    if (freeRequests.GetLength() < maxSize)
    {
        request->Clear();
        freeRequests.Append(request);
        return;
    }
    
    delete request;
}

unsigned ClientRequestCache::GetNumFreeRequests()
{
    return freeRequests.GetLength();
}

unsigned ClientRequestCache::GetMaxFreeRequests()
{
    return maxSize;
}

uint64_t ClientRequestCache::GetMemorySize()
{
    return GetNumFreeRequests() * sizeof(ClientRequest);
}
