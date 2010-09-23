#ifndef TCPWRITER_H
#define TCPWRITER_H

#include "System/Containers/InPriorityQueue.h"
#include "System/Buffers/BufferPool.h"

class TCPConnection; // forward

/*
===============================================================================

 TCPWriter
 
 You must call Flush() after Write() functions to start I/O!

===============================================================================
*/

class TCPWriter
{
public:
    TCPWriter(TCPConnection* conn, BufferPool* pool = NULL);
    virtual ~TCPWriter();
    
    Buffer*                     AcquireBuffer(unsigned size = 0);
    void                        ReleaseBuffer(Buffer* buffer);
    // ReleaseBuffer() if you don't want to write after an AcquireBuffer() after all

    virtual void                Write(Buffer* buffer);
    virtual void                Write(const char* buffer, unsigned length);

    void                        WritePriority(Buffer* buffer);
    void                        WritePriority(const char* buffer, unsigned length);

    void                        WritePooled(Buffer* buffer);
    void                        WritePooledPriority(Buffer* buffer);
    // WritePooled() functions automatically release buffers after data is written
    void                        Flush();

    virtual Buffer*             GetNext();
    virtual void                OnNextWritten();    
    virtual void                OnClose();
    
    unsigned                    BytesQueued();

protected:
    TCPConnection*              conn;
    InPriorityQueue<Buffer>     queue;
    BufferPool*                 pool;
};

#endif
