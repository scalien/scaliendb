#ifndef CONFIGSHARDSERVER_H
#define CONFIGSHARDSERVER_H

#include "System/Common.h"
#include "System/Containers/List.h"
#include "System/IO/Endpoint.h"

/*
===============================================================================================

 ConfigShardServer

===============================================================================================
*/

class ConfigShardServer
{
public:
    ConfigShardServer();
    ConfigShardServer(const ConfigShardServer& other);
    
    ConfigShardServer&  operator=(const ConfigShardServer& other);

    uint64_t            nodeID;
    Endpoint            endpoint;
    
    ConfigShardServer*  prev;
    ConfigShardServer*  next;
};

inline ConfigShardServer::ConfigShardServer()
{
    prev = next = this;
}

inline ConfigShardServer::ConfigShardServer(const ConfigShardServer& other)
{
    *this = other;
}

inline ConfigShardServer& ConfigShardServer::operator=(const ConfigShardServer& other)
{
    nodeID = other.nodeID;
    endpoint = other.endpoint;
    
    prev = next = this;
    
    return *this;
}

#endif
