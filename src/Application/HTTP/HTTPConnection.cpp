#include "HTTPConnection.h"
#include "HTTPServer.h"
#include "HTTPConsts.h"

#define CS_CR               "\015"
#define CS_LF               "\012"
#define CS_CRLF             CS_CR CS_LF
#define MSG_NOT_FOUND       "Not found"

HTTPConnection::HTTPConnection()
{
    server = NULL;
}

void HTTPConnection::Init(HTTPServer* server_)
{
    TCPConnection::InitConnected();
    
    server = server_;
    
    request.Init();
    socket.GetEndpoint(endpoint);
    onCloseCallback.Unset();
    contentType.Reset();
    origin.Reset();
    
    // HACK
    closeAfterSend = false;
}

void HTTPConnection::SetOnClose(const Callable& callable)
{
    onCloseCallback = callable;
}

void HTTPConnection::SetContentType(ReadBuffer& contentType_)
{
    contentType = contentType_;
}

void HTTPConnection::SetOrigin(ReadBuffer& origin_)
{
    origin = origin_;
}

void HTTPConnection::OnRead()
{
    Log_Trace();
    int len;
    
    len = Parse(tcpread.buffer->GetBuffer(), tcpread.buffer->GetLength());

    if (len < 0)
        OnClose();  // parse error
    else if (len == 0)
    {
        if (tcpread.buffer->GetSize() > 1*MB)
        {
            Flush(); // closes the connection
            return;
        }
            
        tcpread.offset = tcpread.buffer->GetLength();
        if (tcpread.buffer->GetSize() < 1*KiB)
            tcpread.buffer->Allocate(1*KiB);
        else
            tcpread.buffer->Allocate(2 * tcpread.buffer->GetLength());
            
        IOProcessor::Add(&tcpread);
    }
}


void HTTPConnection::OnClose()
{
    Log_Trace();
    
    Close();
    request.Free();

    if (onCloseCallback.IsSet())
        Call(onCloseCallback);

    server->DeleteConn(this);
}


void HTTPConnection::OnWrite()
{
    Log_Trace();
    TCPConnection::OnWrite();
    if (closeAfterSend && !tcpwrite.active)
        OnClose();
}

void HTTPConnection::Print(const char* s)
{
    Write(s, (unsigned) strlen(s));
}

void HTTPConnection::Write(const char* s, unsigned length)
{
    GetWriteBuffer().Append(s, length);
}

void HTTPConnection::WriteHeader(int code, const char* extraHeader)
{
    WriteHeaderBuffer(GetWriteBuffer(), code);
    if (extraHeader)
        GetWriteBuffer().Append(extraHeader);
    GetWriteBuffer().Append(CS_CRLF);
}

int HTTPConnection::Parse(char* buf, int len)
{
    int pos;
    
    pos = request.Parse(buf, len);
    if (pos < 0)
        return -1;
    if (pos != len)
        return 0;
    
    if (server->HandleRequest(this, request))
        return 1;
    
    Response(HTTP_STATUS_CODE_NOT_FOUND, MSG_NOT_FOUND, sizeof(MSG_NOT_FOUND) - 1);
    closeAfterSend = true;
        
    return 1;
}


const char* HTTPConnection::Status(int code)
{
    if (code == HTTP_STATUS_CODE_OK)
        return "OK";
    if (code == HTTP_STATUS_CODE_NOT_FOUND)
        return "Not found";
    if (code == HTTP_STATUS_CODE_TEMPORARY_REDIRECT)
        return "Temporary redirect";
    
    return "";
}

void HTTPConnection::Response(int code, const char* data,
 int len, bool close, const char* header)
{   
    Log_Message("[%s] HTTP: %.*s %.*s %d %d", endpoint.ToString(),
                request.line.method.GetLength(), request.line.method.GetBuffer(), 
                request.line.uri.GetLength(), request.line.uri.GetBuffer(),
                code, len);

    WriteHeaderBuffer(GetWriteBuffer(), code);
    GetWriteBuffer().Appendf("Content-Length: %d" CS_CRLF, len);
    if (close)
        GetWriteBuffer().Append("Connection: close" CS_CRLF);
    if (header)
        GetWriteBuffer().Append(header);
    GetWriteBuffer().Append(CS_CRLF);

    GetWriteBuffer().Append(data, len);
    
    Flush(true);
}

void HTTPConnection::ResponseHeader(int code, bool close, const char* header)
{
    if (!request.line.uri.BeginsWith("/json/getconfigstate"))
    {
        Log_Message("[%s] HTTP: %.*s %.*s %d ?",
                    endpoint.ToString(), 
                    request.line.method.GetLength(), request.line.method.GetBuffer(),
                    request.line.uri.GetLength(), request.line.uri.GetBuffer(),
                    code);
    }

    WriteHeaderBuffer(GetWriteBuffer(), code);
    if (close)
        GetWriteBuffer().Append("Connection: close" CS_CRLF);
    if (header)
        GetWriteBuffer().Append(header);
    GetWriteBuffer().Append(CS_CRLF);
}

void HTTPConnection::Flush(bool closeAfterSend_)
{
    // already sent the response
    if (closeAfterSend)
        return;

    closeAfterSend = closeAfterSend_;
    
    TryFlush();
}

void HTTPConnection::WriteHeaderBuffer(Buffer& buffer, int code)
{
    buffer.Append(request.line.version);
    buffer.Appendf(" %d %s" CS_CRLF, code, Status(code));
    buffer.Append("Cache-Control: no-cache" CS_CRLF);
    if (contentType.GetLength() > 0)
        buffer.Appendf("Content-Type: %R" CS_CRLF, &contentType);
    if (origin.GetLength() > 0)
        buffer.Appendf("Access-Control-Allow-Origin: %R" CS_CRLF, &origin);
}
