#include "TCPConnection.h"
#include "System/Events/EventLoop.h"
#include "System/IO/IOProcessor.h"

TCPConnection::TCPConnection()
{
    state = DISCONNECTED;
    connectTimeout.SetCallable(MFUNC(TCPConnection, OnConnectTimeout));
    connectTime = 0;
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

    connectTime = EventLoop::Now();
    Init(false);
    state = CONNECTING;
    connectTimeout.SetDelay(timeout);

    ret = socket.Create();
    if (ret == false)
    {
        Log_Debug("Socket creation failed");
        // HACK: make sure connectTimeout is called eventually
        EventLoop::Reset(&connectTimeout);
        return;
    }

    socket.SetNonblocking();

    // tcpwrite needs to be added to IOProcessor before Socket::Connect, because
    // in multithreaded mode it might happen, that the async connect is finished
    // on the iothread, but the IOOperation is not yet added
    tcpwrite.SetFD(socket.fd);
    tcpwrite.SetOnComplete(MFUNC(TCPConnection, OnConnect));
    tcpwrite.SetOnClose(MFUNC(TCPConnection, OnClose));
    tcpwrite.AsyncConnect();

    // TODO: FIXME: make Posix behavior default by storing endpoint in 
    // IODesc in IOProcessor_Windows, and start ConnectEx only when adding
    // tcpwrite _after_ Socket::Connect
#ifdef PLATFORM_WINDOWS
    IOProcessor::Add(&tcpwrite);
#endif    

    ret = socket.Connect(endpoint);
    if (ret == false)
    {
        Log_Debug("Connect failed");
        // HACK: make sure connectTimeout is called eventually
        EventLoop::Reset(&connectTimeout);
        return;
    }

#ifndef PLATFORM_WINDOWS
    IOProcessor::Add(&tcpwrite);
#endif

    if (timeout > 0)
    {
        Log_Trace("starting timeout with %d", timeout);
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

void TCPConnection::SetPriority(bool priority)
{
    tcpread.priority = priority;
    tcpwrite.priority = priority;
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
    {
        Log_Trace("Not flushing, fd = %d", (int) tcpwrite.fd);
        return;
    }

    writeIndex = 1 - writeIndex;
    
    writeBuffers[writeIndex].SetLength(0);

    tcpwrite.SetBuffer(&writeBuffers[1 - writeIndex]);
    tcpwrite.transferred = 0;
    IOProcessor::Add(&tcpwrite);
    Log_Trace("Flushing, added tcpwrite, fd = %d", (int) tcpwrite.fd);
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
