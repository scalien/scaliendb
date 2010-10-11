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

Result::~Result()
{
    Close();
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
    req->status = SDBP_NOSERVICE;
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

    if (resp->type == CLIENTRESPONSE_FAILED)
        req->status = SDBP_FAILED;
    else
        req->status = SDBP_SUCCESS;

    //req->responses.Append(resp);
    req->response = *resp;
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

int Result::GetKey(ReadBuffer& key)
{
    Request*    request;
    
    if (requestCursor == NULL)
        return SDBP_API_ERROR;
    
    request = requestCursor;
    key.Wrap(request->key);
    
    return request->status;
}

int Result::GetValue(ReadBuffer& value)
{
    Request*    request;
    
    if (requestCursor == NULL)
        return SDBP_API_ERROR;
    
    request = requestCursor;
    // TODO: copy the ReadBuffer!
    value.Wrap(request->response.value.GetBuffer(), request->response.value.GetLength());
    
    return request->status;
}

int Result::GetDatabaseID(uint64_t& databaseID)
{
    if (requestCursor == NULL)
        return SDBP_API_ERROR;
    
    databaseID = requestCursor->databaseID;
    return requestCursor->status;
}

int Result::GetTableID(uint64_t& tableID)
{
    if (requestCursor == NULL)
        return SDBP_API_ERROR;
    
    tableID = requestCursor->tableID;
    return requestCursor->status;
}

unsigned Result::GetRequestCount()
{
    return requests.GetCount();
}
