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

Result::Result()
{
    Close();
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

bool Result::IsFinished()
{
    char                type;
    Request*            request;
    ClientResponse**    response;

    request = requests.First();
    if (request == NULL)
        return false;

    if (request->IsList())
    {
        response = request->responses.Last();
        if (response == NULL)
            return false;

        type = (*response)->type;
        if (type == CLIENTRESPONSE_OK || type == CLIENTRESPONSE_FAILED || type == CLIENTRESPONSE_NOSERVICE)
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
    
    req = requests.Get(resp->commandID);
    if (!req)
        return false;

    Log_Trace("%c %U", resp->type, resp->commandID);    

    req->responseTime = EventLoop::Now();

    if (resp->type == CLIENTRESPONSE_FAILED)
        req->status = SDBP_FAILED;
    else if (req->status != SDBP_FAILED)
        req->status = SDBP_SUCCESS;

    HandleRequestResponse(req, resp);

    if (numCompleted >= requests.GetCount())
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

int Result::GetCommandStatus()
{
    if (!requestCursor)
        return SDBP_API_ERROR;
        
    return requestCursor->status;
}

int Result::GetTransportStatus()
{
    return transportStatus;
}

void Result::SetTransportStatus(int status)
{
    transportStatus = status;
}

void Result::SetConnectivityStatus(int status)
{
    connectivityStatus = status;
}

void Result::SetTimeoutStatus(int status)
{
    timeoutStatus = status;
}

unsigned Result::GetNumNodes()
{
    if (requestCursor == NULL)
        return 0;
    
    return requestCursor->shardConns.GetLength();
}

uint64_t Result::GetNodeID(unsigned n)
{
    if (requestCursor == NULL)
        return 0;
    
    if (n > requestCursor->shardConns.GetLength())
        return 0;
    
    return requestCursor->shardConns.Get(n);
}

unsigned Result::GetElapsedTime()
{
    if (requestCursor == NULL)
        return 0;

    return (unsigned)(requestCursor->responseTime - requestCursor->requestTime);
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
    {
        if (request->response.valueBuffer == NULL)
            return SDBP_API_ERROR;
        value.Wrap(*request->response.valueBuffer);
    }
    
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

int Result::IsConditionalSuccess(bool& isConditionalSuccess)
{
    if (requestCursor == NULL)
        return SDBP_API_ERROR;

    isConditionalSuccess = requestCursor->response.isConditionalSuccess;
    return requestCursor->status;
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

void Result::HandleRequestResponse(Request* req, ClientResponse* resp)
{
    ClientResponse* respCopy;

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
        
        // HACK for enabling Filter to work
        if (req->async)
            numCompleted = requests.GetCount();

        Log_Trace("async: %b, numCompleted: %u, requests: %u", req->async, numCompleted, requests.GetCount());

        // make a copy of the response as MessageConnection reuses the response object
        respCopy = new ClientResponse;
        resp->Transfer(*respCopy);
        req->responses.Append(respCopy);
    }
    else
    {
        // copy data from connection buffer
        if (resp->type == CLIENTRESPONSE_VALUE)
            resp->CopyValue();

        if (req->isBulk)
        {
            req->numBulkResponses++;
            if (req->numBulkResponses == req->shardConns.GetLength())
                numCompleted++;
        }
        else
        {
            // COUNT is a special case, it needs an extra response
            if (req->type == CLIENTREQUEST_COUNT)
            {
                if (resp->type == CLIENTRESPONSE_NUMBER)
                    req->response.Number(req->response.number + resp->number);
                else
                    numCompleted++;
            }
            else
            {
                if (req->response.type == CLIENTRESPONSE_NORESPONSE)
                    numCompleted++;
            
                resp->Transfer(req->response);
            }
        }
    }

    // 'resp' here is already transferred to 'req->response'
    if (req->parent)
        HandleMultiRequestResponse(req);
}

void Result::HandleMultiRequestResponse(Request* req)
{
    if (req->IsList() && req->type != CLIENTREQUEST_COUNT)
    {
        // TODO: sort & merge results
    }
    else
    {
        // type is CLIENTREQUEST_COUNT
        
        // update the aggregate value or set the response status
        if (req->response.type == CLIENTRESPONSE_NUMBER)
            req->parent->response.number += req->response.number;
        else
            req->parent->response.type = req->response.type;
    }
}
