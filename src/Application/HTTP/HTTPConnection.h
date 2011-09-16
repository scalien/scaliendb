#ifndef HTTPCONNECTION_H
#define HTTPCONNECTION_H

#include "Framework/TCP/TCPConnection.h"
#include "HTTPRequest.h"

class HTTPServer;   // forward

/*
===============================================================================================

 HTTPConnection

===============================================================================================
*/

class HTTPConnection : public TCPConnection
{
public:
    HTTPConnection();
    
    void                Init(HTTPServer* server_);
    void                SetOnClose(const Callable& callable);
    void                SetContentType(ReadBuffer& contentType);
    void                SetOrigin(ReadBuffer& origin);

    void                Print(const char* s);
    void                Write(const char* s, unsigned length);
    void                WriteHeader(int code, const char* extraHeader = NULL);
    void                Response(int code, const char* buf,
                         int len, bool close = true, const char* header = NULL);
    void                ResponseHeader(int code, bool close = true,
                         const char* header = NULL);
    void                Flush(bool closeAfterSend = false);

    HTTPServer*         GetServer() { return server; }
    Buffer&             GetReadBuffer() { return readBuffer; }

    // TCPConnection interface
    virtual void        OnRead();
    virtual void        OnClose();
    virtual void        OnWrite();

protected:
    Callable            onCloseCallback;
    HTTPServer*         server;
    HTTPRequest         request;
    Endpoint            endpoint;
    bool                closeAfterSend;
    ReadBuffer          contentType;
    ReadBuffer          origin;

    int                 Parse(char* buf, int len);
    int                 ProcessGetRequest();
    const char*         Status(int code);
    void                WriteHeaderBuffer(Buffer& buffer, int code);
};

#endif
