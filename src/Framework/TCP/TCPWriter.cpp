#include "TCPWriter.h"
#include "TCPConnection.h"

TCPWriter::TCPWriter(TCPConnection* conn_, BufferPool* pool_)
{
    conn = conn_;
    writeBuffer = NULL;
    queuedBytes = 0;
    
    if (pool_ == NULL)
        pool = DEFAULT_BUFFERPOOL;
    else
        pool = pool_;   
}

TCPWriter::~TCPWriter()
{
    OnClose();
}

Buffer* TCPWriter::AcquireBuffer(unsigned size)
{
    return pool->Acquire(size);
}

void TCPWriter::ReleaseBuffer(Buffer* buffer)
{
    return pool->Release(buffer);
}

void TCPWriter::Write(Buffer* buffer)
{
    Write(buffer->GetBuffer(), buffer->GetLength());
}

void TCPWriter::Write(const char* p, unsigned length)
{
    Buffer* buffer;

    if (!p || length == 0)
        return;
    
    buffer = pool->Acquire(length);
    buffer->Write(p, length);
    queue.Enqueue(buffer);
    queuedBytes += buffer->GetLength();
}

void TCPWriter::WritePooled(Buffer* buffer)
{
    if (!buffer || buffer->GetLength() == 0)
    {
        pool->Release(buffer);
        return;
    }

    queue.Enqueue(buffer);
    queuedBytes += buffer->GetLength();
}

void TCPWriter::Flush()
{
    Log_Trace();
    conn->OnWritePending();
}

unsigned TCPWriter::GetQueueLength()
{
    return queue.GetLength();
}

unsigned TCPWriter::GetQueuedBytes()
{
    return queuedBytes;
}

Buffer* TCPWriter::GetNext()
{
    Log_Trace();

    ASSERT(writeBuffer == NULL);

    if (queue.GetLength() == 0)
        return NULL;
    
    writeBuffer = queue.Dequeue();
    queuedBytes -= writeBuffer->GetLength();
    return writeBuffer;
}

void TCPWriter::OnNextWritten()
{
    Log_Trace();
    
    pool->Release(writeBuffer);
    writeBuffer = NULL;
    
    if (queue.GetLength() > 0)
        conn->OnWritePending();
    else
        conn->OnWriteReadyness();
}

void TCPWriter::OnClose()
{
    if (writeBuffer)
        pool->Release(writeBuffer);

    writeBuffer = NULL;
    queuedBytes = 0;
    
    while (queue.GetLength() > 0)
        pool->Release(queue.Dequeue());
}
