#ifndef CONFIGCONTROLLER_H
#define CONFIGCONTROLLER_H

#include "System/IO/Endpoint.h"

class ConfigController
{
public:
    ConfigController();
    ConfigController(const ConfigController& other);
    
    ConfigController&       operator=(const ConfigController& other);

    uint64_t                nodeID;
    Endpoint                endpoint;
    
    bool                    isConnected;
    
    ConfigController*       prev;
    ConfigController*       next;
};

#endif
