#include "MessageConnection.h"

MessageConnection::MessageConnection()
{
    resumeRead.SetCallable(MFUNC(MessageConnection, OnResumeRead));
    flushWrites.SetCallable(MFUNC(MessageConnection, OnFlushWrites));
    writeBuffer = NULL;
    autoFlush = true;
    readActive = true;
}

MessageConnection::~MessageConnection()
{
    // TODO: it would be better to make Close virtual in TCPConnection
    Close();
}

void MessageConnection::InitConnected(bool startRead)
{
    writeBuffer = NULL;
    readBuffer.Allocate(MESSAGING_BUFFER_THRESHOLD * 2);    
    TCPConnection::InitConnected(startRead);
    
    Log_Trace();
}

void MessageConnection::Connect(Endpoint& endpoint_)
{
    Log_Trace();
    
    ASSERT(writeBuffer == NULL);
    endpoint = endpoint_;
    readBuffer.Allocate(MESSAGING_BUFFER_THRESHOLD * 2);    
    TCPConnection::Connect(endpoint, MESSAGING_CONNECT_TIMEOUT);
}

void MessageConnection::Close()
{
    if (writeBuffer != NULL)
    {
        writer->ReleaseBuffer(writeBuffer);
        writeBuffer = NULL;
    }
    
    EventLoop::Remove(&resumeRead);
    EventLoop::Remove(&flushWrites);
    TCPConnection::Close(); 
}

void MessageConnection::PauseReads()
{
    readActive = false;
}

void MessageConnection::ResumeReads()
{
    readActive = true;
}

void MessageConnection::Write(Buffer& msg)
{
    AcquireBuffer();
    writeBuffer->Appendf("%#B", &msg);
    if (autoFlush)
        FlushWriteBuffer();
}

void MessageConnection::Write(Message& msg)
{
    Buffer      tmpBuffer;

    msg.Write(tmpBuffer);
    AcquireBuffer();
    writeBuffer->Appendf("%#B", &tmpBuffer);
    if (autoFlush)
        FlushWriteBuffer();
}

void MessageConnection::Write(Buffer& prefix, Buffer& msg)
{
    unsigned    length;

    AcquireBuffer();
    length = prefix.GetLength() + 1 + msg.GetLength();
    writeBuffer->Appendf("%u:%B:%B", length, &prefix, &msg);
    if (autoFlush)
        FlushWriteBuffer();
}

void MessageConnection::Write(Buffer& prefix, Message& msg)
{
    unsigned    length;
    Buffer      tmpBuffer;

    msg.Write(tmpBuffer);
    AcquireBuffer();
    length = prefix.GetLength() + 1 + tmpBuffer.GetLength();
    writeBuffer->Appendf("%u:%B:%B", length, &prefix, &tmpBuffer);
    if (autoFlush)
        FlushWriteBuffer();
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

void MessageConnection::OnWrite()
{
    TCPConnection::OnWrite();

    if (writer->GetQueueLength() == 0 && writeBuffer != NULL)
    {
        writer->WritePooled(writeBuffer);
        writeBuffer = NULL;
    } 
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
    
//    printf("-------- OnMessage: %u, %.*s\n", tcpread.buffer->GetLength(), MIN(tcpread.buffer->GetLength(), 20), tcpread.buffer->GetBuffer());
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
    Log_Trace("time spent in OnRead(): %U", sw.Elapsed());
}

void MessageConnection::OnResumeRead()
{
    Log_Trace();

    OnRead();
}

void MessageConnection::OnFlushWrites()
{
    if (writeBuffer)
    {
        writer->WritePooled(writeBuffer);
        writeBuffer = NULL;
    }
    
    writer->Flush();    
}

void MessageConnection::OnConnectTimeout()
{
    TCPConnection::OnConnectTimeout();
    
    Log_Trace("endpoint = %s", endpoint.ToString());
    
    Close();
    
    TCPConnection::Connect(endpoint, MESSAGING_CONNECT_TIMEOUT);
}

Buffer* MessageConnection::AcquireBuffer()
{
    if (writeBuffer != NULL)
        return writeBuffer;
    
    writeBuffer = writer->AcquireBuffer(MESSAGING_BUFFER_THRESHOLD);
    return writeBuffer;
}

void MessageConnection::FlushWriteBuffer()
{
    if (writeBuffer != NULL && writeBuffer->GetLength() >= MESSAGING_BUFFER_THRESHOLD)
    {
        writer->WritePooled(writeBuffer);
        writeBuffer = NULL;
    }

    if (flushWrites.IsActive())
        return;
    
    EventLoop::Add(&flushWrites);
}
