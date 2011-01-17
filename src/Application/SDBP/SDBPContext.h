#ifndef SDBPCONTEXT_H
#define SDBPCONTEXT_H

#include "Application/Common/ClientRequest.h"

class SDBPConnection; // forward

/*
===============================================================================================

 SDBPContext

===============================================================================================
*/

class SDBPContext
{
public:
    virtual ~SDBPContext() {}

    // ========================================================================================
    // SDBPContext interface:
    //
    virtual bool    IsValidClientRequest(ClientRequest* request)                            = 0;
    virtual void    OnClientRequest(ClientRequest* request)                                 = 0;
    virtual void    OnClientClose(ClientSession* session)                                   = 0;
    // ========================================================================================
};

#endif
