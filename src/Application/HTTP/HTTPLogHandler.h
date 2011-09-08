#ifndef HTTPLOGHANDLER_H
#define HTTPLOGHANDLER_H

#include "HTTPHandler.h"
#include "System/Containers/InList.h"
#include "System/Containers/InTreeMap.h"
#include "System/Buffers/Buffer.h"
#include "System/Threading/Mutex.h"

/*
===============================================================================================

 HTTPQueue

===============================================================================================
*/

class HTTPQueue
{
public:
    void            Init(ReadBuffer& name, unsigned size);

    void            Write(ReadBuffer& msg);

    // iterator interface, should Lock while iterating in multithreaded environment
    Buffer*         First();
    Buffer*         Next(Buffer* it);

    void            Lock();
    void            Unlock();

    ReadBuffer      GetName();
    unsigned        GetSize();
    unsigned        GetCount();
    uint64_t        GetVersion();

private:
    Buffer          name;
    unsigned        size;
    unsigned        count;
    InList<Buffer>  buffers;
    Mutex           mutex;
    uint64_t        version;
};

/*
===============================================================================================

 HTTPLogHandler

===============================================================================================
*/

class HTTPLogHandler : public HTTPHandler
{
public:
    void            Init(ReadBuffer& name, unsigned size);

    // HTTPHandler interface
    virtual bool    HandleRequest(HTTPConnection* conn, HTTPRequest& request);

    static void     LoggerFunction(void* pHandler, const char* buf, int size, int flush);

private:
    HTTPQueue       queue;
};

#endif
