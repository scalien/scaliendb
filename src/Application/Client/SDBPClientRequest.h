#ifndef SDBPCLIENTREQUEST_H
#define SDBPCLIENTREQUEST_H

#include "System/Containers/List.h"
#include "System/Containers/InTreeMap.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/ClientResponse.h"

namespace SDBPClient
{

#define BUFFER_LENGTH(buflen)        (buflen > ARRAY_SIZE ? buflen : 0)
#define REQUEST_SIZE(req)                                                           \
    BUFFER_LENGTH(req->name.GetLength()) + BUFFER_LENGTH(req->key.GetLength()) +    \
    BUFFER_LENGTH(req->value.GetLength()) + BUFFER_LENGTH(req->test.GetLength()) +  \
    sizeof(Request)

/*
===============================================================================================

 SDBPClient::Request

===============================================================================================
*/

class Request : public ClientRequest
{
public:
    typedef InTreeNode<Request>     TreeNode;
    typedef List<ClientResponse*>   ResponseList;
    typedef ArrayList<uint64_t, 9>  ShardConnList;

    Request();
    ~Request();

    unsigned        GetNumResponses();

    TreeNode        treeNode;
    int             status;
    uint64_t        requestTime;
    uint64_t        responseTime;
    unsigned        numTry;
    unsigned        numShardServers;
    unsigned        numBulkResponses;
    ResponseList    responses;
    bool            async;
    bool            multi;              // multirequests are those that sent to multiple servers
    ShardConnList   shardConns;
    Request*        parent;
};

};  // namespace

#endif
