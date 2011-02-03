#ifndef SDBPCLIENTREQUEST_H
#define SDBPCLIENTREQUEST_H

#include "System/Containers/List.h"
#include "System/Containers/InTreeMap.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/ClientResponse.h"

namespace SDBPClient
{

/*
===============================================================================================

 SDBPClient::Request

===============================================================================================
*/

class Request : public ClientRequest
{
public:
    typedef List<ClientResponse*> ResponseList;
    typedef InTreeNode<Request> TreeNode;

    Request();

    ResponseList    responses;
    TreeNode        treeNode;
    int             status;
    unsigned        numTry;
    uint64_t        requestTime;
    uint64_t        responseTime;
};

};  // namespace

#endif
