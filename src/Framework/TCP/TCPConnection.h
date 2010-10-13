#ifndef TCPCONNECTION_H
#define TCPCONNECTION_H

#include "System/Events/Callable.h"
#include "System/Events/Countdown.h"
#include "System/Buffers/Buffer.h"
#include "System/Containers/InQueue.h"
#include "System/IO/Socket.h"
#include "System/IO/IOOperation.h"
#include "TCPWriter.h"

/*
===============================================================================

 TCPConnection

===============================================================================
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

    Socket&             GetSocket() { return socket; }
    const State         GetState() { return state; }
    
    void                AsyncRead(bool start = true);

    TCPWriter*          GetWriter();
    void                OnWritePending(); // for TCPWriter
    
    TCPConnection*      next;
    TCPConnection*      prev;
    
    virtual void        OnWriteReadyness() {} // called when the write queue is empty

protected:
    void                Init(bool startRead = true);
    virtual void        OnRead() = 0;
    virtual void        OnWrite();
    virtual void        OnClose() = 0;
    virtual void        OnConnect();
    virtual void        OnConnectTimeout();

    State               state;
    Socket              socket;
    TCPRead             tcpread;
    TCPWrite            tcpwrite;
    TCPWriter*          writer;
    Buffer              readBuffer;
    Countdown           connectTimeout;
};

#endif
