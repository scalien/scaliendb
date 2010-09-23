#ifndef IOOPERATION_H
#define IOOPERATION_H

#ifdef PLATFORM_DARWIN
#include <sys/types.h>
#endif

#include "System/Events/Callable.h"
#include "System/Buffers/Buffer.h"
#include "Endpoint.h"
#include "FD.h"

/* all the cursor-type members of IOOperations are absolute cursors */

#define IO_READ_ANY -1

/*
===============================================================================

 IOOperation

===============================================================================
*/

struct IOOperation
{

    IOOperation()
    {
        fd = INVALID_FD;
        type = UNKNOWN;
        active = false;
        pending = false;
        offset = 0;
    }
    
    void SetFD(FD fd_)
    {
        fd = fd_;
    }
    
    void SetOnComplete(const Callable& onComplete_)
    {
        onComplete = onComplete_;
    }
    
    void SetOnClose(const Callable& onClose_)
    {
        onClose = onClose_;
    }
    
    void SetBuffer(Buffer* buffer_)
    {
        buffer = buffer_;
    }

    enum Type       { UDP_WRITE, UDP_READ, TCP_WRITE, TCP_READ, UNKNOWN };
    
    Buffer*         buffer;
    
    Type            type;
    FD              fd;
    int             offset;
    
    bool            active;
    bool            pending;

    Callable        onComplete;
    Callable        onClose;
};

struct UDPWrite : public IOOperation
{
    UDPWrite()
    : IOOperation()
    {
        type = UDP_WRITE;
    }

    Endpoint        endpoint;
};

struct UDPRead : public IOOperation
{
public:
    UDPRead()
    : IOOperation()
    {
        type = UDP_READ;
    }

public:
    Endpoint        endpoint;
};

struct TCPWrite : public IOOperation
{
    TCPWrite() : IOOperation()
    {
        type = TCP_WRITE;
        transferred = 0;
    }
    
    void AsyncConnect()
    {
        buffer = NULL;
        // zero indicates for IOProcessor that we are waiting for connect event
    }
    
    unsigned    transferred;        /*  the IO subsystem has given the first
                                        'transferred' bytes to the kernel */
};

struct TCPRead : public IOOperation
{
    TCPRead() : IOOperation()
    {
        type = TCP_READ;
        listening = false;
        requested = IO_READ_ANY;
    }

    void Listen(FD fd_, const Callable& onComplete_)
    {
        fd = fd_;
        onComplete = onComplete_;
        listening = true;
    }

    bool            listening;      /*  it's a listener */
    int             requested;      /*  application is requesting the buffer
                                        to be filled exactly with a total of
                                        'requested' bytes; if the application
                                        has no idea how much to expect, it sets
                                        'requested' to IO_READ_ANY; 'callback'
                                        is called when request bytes have been
                                        read into the buffer; useful for cases
                                        when header includes length */
};

#endif

