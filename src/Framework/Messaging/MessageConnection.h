#ifndef MESSAGECONNECTION_H
#define MESSAGECONNECTION_H

#include "System/Buffers/Buffer.h"
#include "System/Events/Timer.h"
#include "Framework/TCP/TCPConnection.h"
#include "Message.h"

#define MESSAGING_CONNECT_TIMEOUT       (2*1000)
#define MESSAGING_BUFFER_THRESHOLD      (10*1360)              // tuned to work well with Ethernet
#define MESSAGING_MAX_SIZE              (128*MB)

/*
===============================================================================================

MessageConnection

===============================================================================================
*/

class MessageConnection : public TCPConnection
{
public:
    MessageConnection();
    ~MessageConnection();

    void                InitConnected(bool startRead = true);

    void                Connect(Endpoint& endpoint);
    virtual void        Close();

    void                Write(Buffer& msg);
    void                Write(Message& msg);
    void                Write(Buffer& prefix, Buffer& msg);
    void                Write(Buffer& prefix, Message& msg);

    // Must implement OnMessage() in derived classes
    // OnMessage() returns whether the connection was closed and deleted
    virtual bool        OnMessage(ReadBuffer& msg)                              = 0;
    virtual void        OnConnect();
    virtual void        OnClose();
    
protected:
    void                OnRead();
    void                OnResumeRead();
    void                OnConnectTimeout();
    void                OnFlushWrites();
    void                Flush();

    Endpoint            endpoint;
    YieldTimer          resumeRead;
    YieldTimer          flushWrites;

    bool                autoFlush;
    bool                readActive;
};

#endif
