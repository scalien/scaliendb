#include "HTTPLogHandler.h"
#include "HTTPRequest.h"
#include "HTTPSession.h"
#include "UrlParam.h"

/*
===============================================================================================

 HTTPQueue

===============================================================================================
*/

void HTTPQueue::Init(ReadBuffer& name_, unsigned size_)
{
    Buffer*     buffer;

    name.Write(name_);
    size = size_;

    for (unsigned i = 0; i < size; i++)
    {
        buffer = new Buffer;
        buffers.Append(buffer);
    }

    count = 0;
    version = 1;
}

void HTTPQueue::Write(ReadBuffer& msg)
{
    Buffer*     buffer;

    Lock();

    buffer = buffers.First();
    buffers.Remove(buffer);

    buffer->Write(msg);
    buffers.Append(buffer);

    version += 1;
    if (count < size)
        count += 1;

    Unlock();
}

// iterator interface, should Lock while iterating in multithreaded environment
Buffer* HTTPQueue::First()
{
    return buffers.First();
}

Buffer* HTTPQueue::Next(Buffer* it)
{
    return buffers.Next(it);
}

void HTTPQueue::Lock()
{
    mutex.Lock();
}

void HTTPQueue::Unlock()
{
    mutex.Unlock();
}

ReadBuffer HTTPQueue::GetName()
{
    return name;
}

unsigned HTTPQueue::GetSize()
{
    return size;
}

unsigned HTTPQueue::GetCount()
{
    return count;
}

uint64_t HTTPQueue::GetVersion()
{
    return version;
}

/*
===============================================================================================

 HTTPLogHandler

===============================================================================================
*/

void HTTPLogHandler::Init(ReadBuffer& name, unsigned size)
{
    queue.Init(name, size);
    Log_SetFunction(&HTTPLogHandler::LoggerFunction, this);
}

bool HTTPLogHandler::HandleRequest(HTTPConnection* conn, HTTPRequest& request)
{
    HTTPSession     session;
    UrlParam        params;
    ReadBuffer      cmd;
    ReadBuffer      line;
    uint64_t        lineNumber;
    uint64_t        startGap;
    Buffer          tmp;
    Buffer*         buffer;

	// sanity checks
	if (request.line.uri.GetLength() == 0)
		return false;

	if (request.line.uri.BeginsWith("."))
		return false;

    session.SetConnection(conn);
    session.ParseRequest(request, cmd, params);

    if (!cmd.BeginsWith(queue.GetName()))
        return false;

    queue.Lock();
    
    startGap = queue.GetSize() - queue.GetCount();
    lineNumber = queue.GetVersion() - queue.GetSize() + startGap + 1;
    FOREACH (buffer, queue)
    {
        if (startGap > 0)
        {
            startGap -= 1;
            continue;
        }
        tmp.Writef("%010U", lineNumber);
        session.PrintPair(tmp, *buffer);
        lineNumber += 1;
    }

    queue.Unlock();

    session.Flush();

    return true;
}

void HTTPLogHandler::LoggerFunction(void* pHandler, const char* buf, int size, int flush)
{
    HTTPLogHandler*     handler;
    ReadBuffer          msg;
    int                 terminatorSize;

    terminatorSize = 3;

    if (size - terminatorSize <= 0)
        return;

    msg.Wrap((char*) buf, size - terminatorSize);

    handler = static_cast<HTTPLogHandler*>(pHandler);
    handler->queue.Write(msg);
}
