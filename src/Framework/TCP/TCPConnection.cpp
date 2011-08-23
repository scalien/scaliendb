#include "TCPConnection.h"
#include "System/Events/EventLoop.h"

TCPConnection::TCPConnection()
{
    state = DISCONNECTED;
    connectTimeout.SetCallable(MFUNC(TCPConnection, OnConnectTimeout));
    writeIndex = 0;
    prev = next = this;
}

TCPConnection::~TCPConnection()
{   
    Close();
}

void TCPConnection::InitConnected(bool startRead)
{
    Init(startRead);
    state = CONNECTED;
}

void TCPConnection::Connect(Endpoint &endpoint, unsigned timeout)
{
    Log_Trace("endpoint = %s", endpoint.ToString());

    bool ret;

    if (state != DISCONNECTED)
        return;

    Init(false);
    state = CONNECTING;

    socket.Create(Socket::TCP);
    socket.SetNonblocking();
    ret = socket.Connect(endpoint);

    tcpwrite.SetFD(socket.fd);
    tcpwrite.SetOnComplete(MFUNC(TCPConnection, OnConnect));
    tcpwrite.SetOnClose(MFUNC(TCPConnection, OnClose));
    tcpwrite.AsyncConnect();
    IOProcessor::Add(&tcpwrite);

    if (timeout > 0)
    {
        Log_Trace("starting timeout with %d", timeout);

        connectTimeout.SetDelay(timeout);
        EventLoop::Reset(&connectTimeout);
    }
}

void TCPConnection::Close()
{
    Log_Trace();

    EventLoop::Remove(&connectTimeout);

    IOProcessor::Remove(&tcpread);
    IOProcessor::Remove(&tcpwrite);

    writeBuffers[0].Reset();
    writeBuffers[1].Reset();

    socket.Close();
    state = DISCONNECTED;
}


Buffer& TCPConnection::GetWriteBuffer()
{
    return writeBuffers[writeIndex];
}

uint64_t TCPConnection::GetMemoryUsage()
{
    return sizeof(*this) + readBuffer.GetSize() + writeBuffers[0].GetSize() + writeBuffers[1].GetSize();
}

void TCPConnection::AsyncRead(bool start)
{
    Log_Trace();
    
    tcpread.SetFD(socket.fd);
    tcpread.SetOnComplete(MFUNC(TCPConnection, OnRead));
    tcpread.SetOnClose(MFUNC(TCPConnection, OnClose));
    tcpread.buffer = &readBuffer;
    if (start)
        IOProcessor::Add(&tcpread);
    else
        Log_Trace("not posting read");
}

void TCPConnection::TryFlush()
{
    if (state == DISCONNECTED || tcpwrite.active || writeBuffers[writeIndex].GetLength() == 0)
        return;
    
    writeIndex = 1 - writeIndex;
    
    writeBuffers[writeIndex].SetLength(0);

    tcpwrite.SetBuffer(&writeBuffers[1 - writeIndex]);
    tcpwrite.transferred = 0;
    IOProcessor::Add(&tcpwrite);
}

void TCPConnection::Init(bool startRead)
{
    Log_Trace();
    
    ASSERT(tcpread.active == false);
    ASSERT(tcpwrite.active == false);

    readBuffer.Clear();
    
    tcpwrite.SetFD(socket.fd);
    tcpwrite.SetOnComplete(MFUNC(TCPConnection, OnWrite));
    tcpwrite.SetOnClose(MFUNC(TCPConnection, OnClose));

    AsyncRead(startRead);
}

void TCPConnection::OnWrite()
{
    Log_Trace("Written %d bytes on fd %d, bytes: %B",
     tcpwrite.buffer->GetLength(), (int)socket.fd, tcpwrite.buffer);

    TryFlush();
    
    if (state != DISCONNECTED || !tcpwrite.active)
        OnWriteReadyness();
}

void TCPConnection::OnConnect()
{
    Log_Trace();

    socket.SetNodelay();
    
    state = CONNECTED;
    tcpwrite.onComplete = MFUNC(TCPConnection, OnWrite);
    
    EventLoop::Remove(&connectTimeout);

    TryFlush();
    
    if (state != DISCONNECTED && !tcpwrite.active)
        OnWriteReadyness();
}

void TCPConnection::OnConnectTimeout()
{
    Log_Trace();
}
