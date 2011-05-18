#include "TCPConnection.h"
#include "System/Events/EventLoop.h"

TCPConnection::TCPConnection()
{
    state = DISCONNECTED;
    connectTimeout.SetCallable(MFUNC(TCPConnection, OnConnectTimeout));
    writer = new TCPWriter(this);
    prev = next = this;
}

TCPConnection::~TCPConnection()
{   
    Close();
    delete writer;
}

void TCPConnection::InitConnected(bool startRead)
{
    Init(startRead);
    state = CONNECTED;
}

void TCPConnection::Connect(Endpoint &endpoint, unsigned timeout)
{
    Log_Trace("endpoint_ = %s", endpoint.ToString());

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

    socket.Close();
    state = DISCONNECTED;
    
    if (writer)
        writer->OnClose();
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

TCPWriter* TCPConnection::GetWriter()
{
    return writer;
}

void TCPConnection::OnWritePending()
{
    Log_Trace("fd = %d", (int)socket.fd);

    Buffer* buffer;

    if (state == DISCONNECTED || tcpwrite.active)
        return;

    ASSERT(writer != NULL);
    buffer = writer->GetNext();
    
    if (buffer == NULL)
        return;

//    Log_Message("OnWritePending: %u", buffer->GetLength());
    tcpwrite.SetBuffer(buffer);
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
    Log_Trace("Written %d bytes on %d", tcpwrite.buffer->GetLength(), (int)socket.fd);
    Log_Trace("Written on %d: %B", (int)socket.fd, tcpwrite.buffer);

    ASSERT(writer != NULL);
    writer->OnNextWritten();
}

void TCPConnection::OnConnect()
{
    Log_Trace();

    socket.SetNodelay();
    
    state = CONNECTED;
    tcpwrite.onComplete = MFUNC(TCPConnection, OnWrite);
    
    EventLoop::Remove(&connectTimeout);
    OnWritePending();
    
    OnWriteReadyness();
}

void TCPConnection::OnConnectTimeout()
{
    Log_Trace();
}

