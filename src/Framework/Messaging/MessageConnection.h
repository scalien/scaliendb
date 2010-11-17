#ifndef MESSAGECONNECTION_H
#define MESSAGECONNECTION_H

#include "System/Stopwatch.h"
#include "System/Events/EventLoop.h"
#include "System/Buffers/Buffer.h"
#include "Framework/TCP/TCPConnection.h"
#include "Message.h"

#define MESSAGING_YIELD_TIME            CLOCK_RESOLUTION    // msec
#define MESSAGING_CONNECT_TIMEOUT       2000
#define MESSAGING_BUFFER_THRESHOLD      1360                // tuned to work well with Ethernet

/*
===============================================================================================

MessageConnection

===============================================================================================
*/

class MessageConnection : public TCPConnection
{
public:
    MessageConnection();

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
    virtual void        OnWrite();
    
protected:
    void                OnRead();
    void                OnResumeRead();
    void                OnConnectTimeout();
    Buffer*             AcquireBuffer();
    void                FlushWriteBuffer();

    Endpoint            endpoint;
    Countdown           resumeRead;
    Buffer*             writeBuffer;
    bool                autoFlush;
};

#endif
