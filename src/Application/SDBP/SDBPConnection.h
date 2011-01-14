#ifndef SDBPCONNECTION_H
#define SDBPCONNECTION_H

#include "Framework/Messaging/MessageConnection.h"
#include "Application/Common/ClientSession.h"
#include "Application/Common/ClientResponse.h"

class SDBPContext;
class SDBPServer;
class ClientRequest;

/*
===============================================================================================

 SDBPConnection

===============================================================================================
*/

class SDBPConnection : public MessageConnection, public ClientSession
{
public:
    SDBPConnection();
    
    void                Init(SDBPServer* server);
    void                SetContext(SDBPContext* context);

    // ========================================================================================
    // MessageConnection interface:
    //
    // OnMessage() returns whether the connection was closed and deleted
    bool                OnMessage(ReadBuffer& msg);
    // Must overrise OnWrite(), to handle closeAfterSend()
    void                OnWrite();
    // Must override OnClose() to prevent the default behaviour, which is to call Close(),
    // in case numPendingOps > 0
    void                OnClose();
    // ========================================================================================

    // ========================================================================================
    // ClientSession interface
    //
    virtual void        OnComplete(ClientRequest* request, bool last);
    virtual bool        IsActive();
    // ========================================================================================
    
private:
    SDBPServer*         server;
    SDBPContext*        context;
    unsigned            numPending;
    unsigned            numCompleted;
    uint64_t            connectTimestamp;
};

#endif
