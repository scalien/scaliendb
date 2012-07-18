#ifndef TCPCONNECTION_H
#define TCPCONNECTION_H

#include "System/Events/Callable.h"
#include "System/Events/Countdown.h"
#include "System/Buffers/Buffer.h"
#include "System/Containers/InQueue.h"
#include "System/IO/Socket.h"
#include "System/IO/IOOperation.h"

/*
===============================================================================================

 TCPConnection

===============================================================================================
*/

class TCPConnection
{
public:
    enum State          { DISCONNECTED, CONNECTED, CONNECTING };

    TCPConnection();
    virtual ~TCPConnection();
    
    void                InitConnected(bool startRead = true);
    void                Connect(Endpoint &endpoint, unsigned timeout);
    void                Close();

    void                UseKeepAlive(unsigned timeout);
    void                SetPriority(bool priority);
    Socket&             GetSocket() { return socket; }
    State               GetState() { return state; }
    Buffer&             GetWriteBuffer();
    virtual uint64_t    GetMemoryUsage();
    
    void                AsyncRead(bool start = true);
    
    TCPConnection*      next;
    TCPConnection*      prev;

    virtual void        OnWriteReadyness() {} // called when the write queue is empty

protected:
    void                TryFlush();
    void                Init(bool startRead = true);
    virtual void        OnRead();
    virtual void        OnWrite();
    virtual void        OnClose() = 0;
    virtual void        OnConnect();
    virtual void        OnConnectTimeout();
    void                OnKeepAliveTimeout();

    State               state;
    Socket              socket;
    TCPRead             tcpread;
    TCPWrite            tcpwrite;
    Buffer              readBuffer;
    Buffer              writeBuffers[2];
    unsigned            writeIndex;
    bool                useKeepAlive;
    uint64_t            lastTrafficEvent;
    uint64_t            connectTime;
    Countdown           connectTimeout;
    Countdown           keepAliveTimeout;
};

#endif
