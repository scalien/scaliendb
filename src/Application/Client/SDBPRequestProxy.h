#ifndef SDBPREQUESTPROXY_H
#define SDBPREQUESTPROXY_H

#include "System/Containers/List.h"
#include "System/Containers/InTreeMap.h"
#include "SDBPClientRequest.h"

namespace SDBPClient
{

/*
===============================================================================================

 SDBPClient::RequestProxy

===============================================================================================
*/

class RequestProxy
{
    typedef InList<Request>     RequestList;
    typedef InTreeMap<Request>  RequestMap;

public:
    void        Init();
    Request*    RemoveAndAdd(Request* request); // returns old removed
    Request*    Find(Request* query);
    void        Remove(Request* request);
    void        Clear();
    unsigned    GetCount();
    unsigned    GetSize();

    // pop oldest Request
    Request*    Pop();

    // tableID/key sorted iteration
    Request*    First();
    Request*    Last();    
    Request*    Next(Request* t);
    Request*    Prev(Request* t);

private:
    int64_t     size;
    RequestList list;
    RequestMap  map;
};

}

#endif
