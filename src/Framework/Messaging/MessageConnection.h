#ifndef MESSAGECONNECTION_H
#define MESSAGECONNECTION_H

#include "System/Stopwatch.h"
#include "System/Events/EventLoop.h"
#include "System/Buffers/Buffer.h"
#include "Framework/TCP/TCPConnection.h"
#include "Message.h"

#define MESSAGING_CONNECT_TIMEOUT       2000
#define MESSAGING_BUFFER_THRESHOLD      (100*1360)              // tuned to work well with Ethernet
#define MESSAGING_MAX_SIZE              128*MB

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

    void                PauseReads();
    void                ResumeReads();

    void                Write(Buffer& msg);
    void                Write(Message& msg);
    void                Write(Buffer& prefix, Buffer& msg);
    void                Write(Buffer& prefix, Message& msg);

    // Must implement OnMessage() in derived classes
    // OnMessage() returns whether the connection was closed and deleted
    virtual bool        OnMessage(ReadBuffer& msg)                              = 0;
    virtual void        OnConnect();
    virtual void        OnClose();
    virtual void        OnWrite();
    
protected:
    void                OnRead();
    void                OnResumeRead();
    void                OnFlushWrites();
    void                OnConnectTimeout();
    Buffer*             AcquireBuffer();
    void                FlushWriteBuffer();

    Endpoint            endpoint;
    YieldTimer          resumeRead;
    YieldTimer          flushWrites;
    Buffer*             writeBuffer;
    bool                autoFlush;
    bool                readActive;
};

#endif
