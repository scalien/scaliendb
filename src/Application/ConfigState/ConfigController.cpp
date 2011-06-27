#include "ConfigController.h"

ConfigController::ConfigController()
{
    prev = next = this;
    nodeID = 0;
    isConnected = false;    
}

ConfigController::ConfigController(const ConfigController& other)
{
    *this = other;
}

ConfigController& ConfigController::operator=(const ConfigController& other)
{
    nodeID = other.nodeID;
    endpoint = other.endpoint;
    isConnected = other.isConnected;
    
    prev = next = this;
    
    return *this;
}
