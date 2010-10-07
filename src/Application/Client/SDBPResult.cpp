#include "SDBPResult.h"
#include "SDBPClientConsts.h"

using namespace SDBPClient;

static uint64_t Key(Request* req)
{
    return req->commandID;
}

static int KeyCmp(uint64_t a, uint64_t b)
{
    if (a < b)
        return -1;
    if (a > b)
        return 1;
    return 0;
}

void Result::Close()
{
    transportStatus = SDBP_FAILURE;
    requests.Clear();
    numCompleted = 0;
}

void Result::Begin()
{
    requestCursor = requests.First();
}

void Result::Next()
{
    requestCursor = requests.Next(requestCursor);
}

bool Result::IsEnd()
{
    return requestCursor == NULL;
}

void Result::AppendRequest(Request* req)
{
    req->numTry = 0;
    requests.Insert(req);
    if (numCompleted > 0)
        transportStatus = SDBP_PARTIAL;
}

bool Result::AppendRequestResponse(ClientResponse* resp)
{
    Request*    req;
    
    req = requests.Get<uint64_t>(resp->commandID);
    if (!req)
        return false;

    if (resp->type == CLIENTRESPONSE_OK)
        req->status = SDBP_SUCCESS;
    else
        req->status = SDBP_FAILED;

    req->responses.Append(resp);
    numCompleted++;
    
    if (numCompleted == requests.GetCount())
        transportStatus = SDBP_SUCCESS;
    else
        transportStatus = SDBP_PARTIAL;
    
    return true;
}

int Result::CommandStatus()
{
    if (!requestCursor)
        return SDBP_API_ERROR;
        
    return requestCursor->status;
}

int Result::TransportStatus()
{
    return transportStatus;
}

unsigned Result::GetRequestCount()
{
    return requests.GetCount();
}
