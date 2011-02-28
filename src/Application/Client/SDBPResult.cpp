#include "SDBPResult.h"
#include "SDBPClientConsts.h"
#include "System/Time.h"
#include "System/Events/EventLoop.h"

using namespace SDBPClient;

#define BUFFER_LENGTH(buflen)        (buflen > ARRAY_SIZE ? buflen : 0)
#define REQUEST_SIZE(req)                                                           \
    BUFFER_LENGTH(req->name.GetLength()) + BUFFER_LENGTH(req->key.GetLength()) +    \
    BUFFER_LENGTH(req->value.GetLength()) + BUFFER_LENGTH(req->test.GetLength()) +  \
    sizeof(Request)

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
    batchLimit = 100*MB;
    batchSize = 0;
}

void Result::Begin()
{
    requestCursor = requests.First();
    responseCursor = requestCursor ? requestCursor->responses.First() : NULL;
    responsePos = 0;
}

void Result::Next()
{
    if (requestCursor == NULL)
        return;
        
    if (requestCursor->IsList())
    {
        responsePos++;

        // skip fragments that contain zero keys
        while (responsePos >= (*responseCursor)->numKeys)
        {
            responseCursor = requestCursor->responses.Next(responseCursor);
            responsePos = 0;
            if (responseCursor == NULL)
            {
                // reached the end
                requestCursor = NULL;
                break;
            }
        }

        return;
    }

    requestCursor = requests.Next(requestCursor);
}

bool Result::IsEnd()
{
    if (requestCursor == NULL)
        return true;

    if (requestCursor->IsList())
    {
        if (responsePos == (*responseCursor)->numKeys && 
         requestCursor->responses.Next(responseCursor) == NULL)
            return true;
    }
    
    return false;
}

void Result::SetBatchLimit(uint64_t batchLimit_)
{
    batchLimit = batchLimit_;
}

bool Result::AppendRequest(Request* req)
{
    batchSize += REQUEST_SIZE(req);
    
    if (batchSize > batchLimit)
        return false;

    req->numTry = 0;
    req->status = SDBP_NOSERVICE;
    req->response.NoResponse();
    requests.Insert(req);
    if (numCompleted > 0)
        transportStatus = SDBP_PARTIAL;
    
    return true;
}

bool Result::AppendRequestResponse(ClientResponse* resp)
{
    Request*        req;
    ClientResponse* respCopy;
    
    req = requests.Get(resp->commandID);
    if (!req)
        return false;

    req->responseTime = EventLoop::Now();
    if (resp->type == CLIENTRESPONSE_FAILED)
        req->status = SDBP_FAILED;
    else
        req->status = SDBP_SUCCESS;

    if (req->IsList() && req->type != CLIENTREQUEST_COUNT)
    {
        // copy data from connection buffer
        if (resp->type == CLIENTRESPONSE_LIST_KEYS)
            resp->CopyKeys();
        if (resp->type == CLIENTRESPONSE_LIST_KEYVALUES)
            resp->CopyKeyValues();

        // each LIST request has an extra response meaning the end of transmission
        if (resp->type != CLIENTRESPONSE_LIST_KEYS && resp->type != CLIENTRESPONSE_LIST_KEYVALUES)
            numCompleted++;

        // make a copy of the response as MessageConnection reuses the response object
        respCopy = new ClientResponse;
        resp->Transfer(*respCopy);
        req->responses.Append(respCopy);
    }
    else
    {
        if (resp->type == CLIENTRESPONSE_VALUE)
            resp->CopyValue();
        resp->Transfer(req->response);
        numCompleted++;
    }
    
//    Log_Message("commandID: %u", (unsigned) req->commandID);
    
    if (numCompleted == requests.GetCount())
        transportStatus = SDBP_SUCCESS;
    else
        transportStatus = SDBP_PARTIAL;

    return true;
}

void Result::RemoveRequest(Request* req)
{
    if (req->treeNode.IsInTree())
        requests.Remove(req);
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
    if (request->IsList())
    {
        if (responsePos >= (*responseCursor)->numKeys)
            return SDBP_API_ERROR;
        key = (*responseCursor)->keys[responsePos];
    }
    else
        key.Wrap(request->key);
    
    return request->status;
}

int Result::GetValue(ReadBuffer& value)
{
    Request*    request;
    
    if (requestCursor == NULL)
        return SDBP_API_ERROR;
    
    request = requestCursor;
    if (request->IsList())
    {
        if (responsePos >= (*responseCursor)->numKeys)
            return SDBP_API_ERROR;
        value = (*responseCursor)->values[responsePos];
    }
    else
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
