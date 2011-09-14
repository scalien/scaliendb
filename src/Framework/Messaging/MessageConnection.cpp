#include "MessageConnection.h"

MessageConnection::MessageConnection()
{
    resumeRead.SetCallable(MFUNC(MessageConnection, OnResumeRead));
    flushWrites.SetCallable(MFUNC(MessageConnection, OnFlushWrites));
    writeBuffers[0].Allocate(MESSAGING_BUFFER_THRESHOLD);
    writeBuffers[1].Allocate(MESSAGING_BUFFER_THRESHOLD);
    autoFlush = true;
    readActive = true;
}

MessageConnection::~MessageConnection()
{
    Close();
}

void MessageConnection::InitConnected(bool startRead)
{
    readBuffer.Allocate(MESSAGING_BUFFER_THRESHOLD * 2);    
    TCPConnection::InitConnected(startRead);    
    Log_Trace();
}

void MessageConnection::Connect(Endpoint& endpoint_)
{
    Log_Trace();    
    endpoint = endpoint_;
    readBuffer.Allocate(MESSAGING_BUFFER_THRESHOLD * 2);    
    TCPConnection::Connect(endpoint, MESSAGING_CONNECT_TIMEOUT);
}

void MessageConnection::Close()
{
    EventLoop::Remove(&resumeRead);
    EventLoop::Remove(&flushWrites);
    TCPConnection::Close(); 
}

void MessageConnection::Write(Buffer& msg)
{
    GetWriteBuffer().Appendf("%#B", &msg);

    if (autoFlush)
        Flush();
}

void MessageConnection::Write(Message& msg)
{
    // TODO: optimize
    
    Buffer tmpBuffer;

    msg.Write(tmpBuffer);

    GetWriteBuffer().Appendf("%#B", &tmpBuffer);

    if (autoFlush)
        Flush();
}

void MessageConnection::Write(Buffer& prefix, Buffer& msg)
{
    unsigned length;

    length = prefix.GetLength() + 1 + msg.GetLength();

    GetWriteBuffer().Appendf("%u:%B:%B", length, &prefix, &msg);

    if (autoFlush)
        Flush();
}

void MessageConnection::Write(Buffer& prefix, Message& msg)
{
    // TODO: optimize

    unsigned    length;
    Buffer      tmpBuffer;

    msg.Write(tmpBuffer);
    length = prefix.GetLength() + 1 + tmpBuffer.GetLength();

    GetWriteBuffer().Appendf("%u:%B:%B", length, &prefix, &tmpBuffer);

    if (autoFlush)
        Flush();
}

void MessageConnection::OnConnect()
{
    TCPConnection::OnConnect();
    
    AsyncRead();        
}

void MessageConnection::OnClose()
{
    Log_Trace();
    
    if (connectTimeout.IsActive())
        return;
    
    Close();    
}

void MessageConnection::OnRead()
{
    bool            yield, closed;
    unsigned        pos, msglength, nread, msgbegin, msgend, required;
    uint64_t        start;
    Stopwatch       sw;
    ReadBuffer      msg;

    if (!readActive)
    {
        if (resumeRead.IsActive())
            STOP_FAIL(1, "Program bug: resumeRead.IsActive() should be false.");
        EventLoop::Add(&resumeRead);
        return;
    }

    sw.Start();
    
    Log_Trace("Read buffer: %B", tcpread.buffer);
    
    tcpread.requested = IO_READ_ANY;
    pos = 0;
    start = NowClock();
    yield = false;

    while(true)
    {
        msglength = BufferToUInt64(tcpread.buffer->GetBuffer() + pos,
         tcpread.buffer->GetLength() - pos, &nread);
        
        if (msglength > tcpread.buffer->GetSize() - NumDigits(msglength) - 1)
        {
            Log_Trace();
            required = msglength + NumDigits(msglength) + 1;
            if (required > MESSAGING_MAX_SIZE)
            {
                Log_Trace();
                OnClose();
                return;
            }
            tcpread.buffer->Allocate(required);
            break;
        }
        
        if (nread == 0 || (unsigned) tcpread.buffer->GetLength() - pos <= nread)
            break;
            
        if (tcpread.buffer->GetCharAt(pos + nread) != ':')
        {
            Log_Trace("Message protocol error");
            OnClose();
            return;
        }
    
        msgbegin = pos + nread + 1;
        msgend = pos + nread + 1 + msglength;
        
        if ((unsigned) tcpread.buffer->GetLength() < msgend)
        {
            // read more
            //tcpread.requested = msgend - pos;
            break;
        }

        msg.SetBuffer(tcpread.buffer->GetBuffer() + msgbegin);
        msg.SetLength(msglength);
        closed = OnMessage(msg);
        if (closed)
            return;

        pos = msgend;
        
        // if the user called Close() in OnMessageRead()
        if (state != CONNECTED)
            return;
        
        if (tcpread.buffer->GetLength() == msgend)
            break;

        if (NowClock() - start >= YIELD_TIME || !readActive)
        {
            // let other code run every YIELD_TIME msec
            yield = true;
            if (resumeRead.IsActive())
                STOP_FAIL(1, "Program bug: resumeRead.IsActive() should be false.");
            EventLoop::Add(&resumeRead);
            break;
        }
    }
    
    if (pos > 0)
    {
        memmove(tcpread.buffer->GetBuffer(), tcpread.buffer->GetBuffer() + pos,
         tcpread.buffer->GetLength() - pos);
        tcpread.buffer->Shorten(pos);
    }
    
    if (state == CONNECTED
     && !tcpread.active && !yield)
        IOProcessor::Add(&tcpread);

    sw.Stop();
    //Log_Debug("time spent in OnRead(): %U", sw.Elapsed());
}

void MessageConnection::OnResumeRead()
{
    Log_Trace();

    OnRead();
}

void MessageConnection::OnConnectTimeout()
{
    TCPConnection::OnConnectTimeout();
    
    Log_Trace("endpoint = %s", endpoint.ToString());
    
    Close();
    
    TCPConnection::Connect(endpoint, MESSAGING_CONNECT_TIMEOUT);
}

void MessageConnection::OnFlushWrites()
{
    // flushWrites YieldTimer arrived
    Log_Trace("fd = %d", tcpwrite.fd.index);

    TCPConnection::TryFlush();
}

void MessageConnection::Flush()
{
    // this just adds the flushWrites YieldTimer
    
    if (flushWrites.IsActive())
        return;
    
    EventLoop::Add(&flushWrites);
}
