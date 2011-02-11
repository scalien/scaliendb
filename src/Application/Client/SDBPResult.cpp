#include "SDBPResult.h"
#include "SDBPClientConsts.h"
#include "System/Time.h"
#include "System/Events/EventLoop.h"

using namespace SDBPClient;

static inline uint64_t Key(const Request* req)
{
    return req->commandID;
}

static inline int KeyCmp(uint64_t a, uint64_t b)
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
    Log_Trace();
    transportStatus = SDBP_FAILURE;
    requests.DeleteTree();
    numCompleted = 0;
    requestCursor = NULL;
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
    
    req = requests.Get(resp->commandID);
    if (!req)
        return false;

    if (resp->type == CLIENTRESPONSE_FAILED)
        req->status = SDBP_FAILED;
    else
        req->status = SDBP_SUCCESS;

    if (resp->type == CLIENTRESPONSE_VALUE)
        resp->CopyValue();

    //req->responses.Append(resp);
    req->responseTime = EventLoop::Now/*Clock*/();
    //req->response = *resp;
    resp->Transfer(req->response);
    numCompleted++;
    
//    Log_Message("commandID: %u", (unsigned) req->commandID);
    
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
    value.Wrap(*request->response.valueBuffer);
    
    return request->status;
}

int Result::GetNumber(uint64_t& number)
{
    Request*    request;
    
    if (requestCursor == NULL)
        return SDBP_API_ERROR;
    
    request = requestCursor;
    number = request->response.number;
    
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

Request* Result::GetRequestCursor()
{
    return requestCursor;
}
