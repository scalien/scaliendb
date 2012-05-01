#include "HTTPSession.h"
#include "Application/HTTP/UrlParam.h"
#include "Application/HTTP/HTTPRequest.h"
#include "Application/HTTP/HTTPConnection.h"
#include "Application/HTTP/HTTPConsts.h"
#include "Application/HTTP/Mime.h"

#define MSG_FAIL                "Unable to process your request at this time"
#define CONTENT_TYPE_PLAIN      HTTP_HEADER_CONTENT_TYPE ": " MIME_TYPE_TEXT_PLAIN HTTP_CS_CRLF
#define CONTENT_TYPE_HTML       HTTP_HEADER_CONTENT_TYPE ": " MIME_TYPE_TEXT_HTML HTTP_CS_CRLF
#define WINDOWS_NT_USER_AGENT   "Windows NT "

HTTPSession::HTTPSession()
{
    headerSent = false;
    conn = NULL;
    type = PLAIN_TEXT;
}

void HTTPSession::SetConnection(HTTPConnection* conn_)
{
    conn = conn_;
    
    if (conn != NULL)
        json.Init(conn);
}

bool HTTPSession::ParseRequest(HTTPRequest& request, ReadBuffer& cmd, UrlParam& params)
{
    char*       qmark;
    ReadBuffer  rb;
    ReadBuffer  jsonCallback;
    ReadBuffer  mimeType;
    ReadBuffer  origin;
    
    // TODO: when adding keep-alive HTTP sessions, move this to an Init() function
    isFlushed = false;
    
    uri = request.line.uri;
    rb = request.line.uri;
    if (rb.GetCharAt(0) == '/')
        rb.Advance(1);
    
    mimeType.Reset();
    ParseType(rb);
    cmd = rb;

    qmark = NULL;
    if (rb.GetLength() > 0)
        qmark = FindInBuffer(rb.GetBuffer(), rb.GetLength() - 1, '?');

    if (qmark)
    {
        rb.Advance((unsigned) (qmark - rb.GetBuffer() + 1));
        params.Init(rb.GetBuffer(), rb.GetLength(), '&');
        cmd.SetLength((unsigned) (qmark - cmd.GetBuffer()));
        
        if (type == JSON)
        {
            HTTP_GET_OPT_PARAM(params, "callback", jsonCallback);
            json.SetCallbackPrefix(jsonCallback);
        }
        
        // mime type is overridable
        HTTP_GET_OPT_PARAM(params, "mimetype", mimeType);
        if (mimeType.GetLength() != 0)
            conn->SetContentType(mimeType);
            
        // CORS support
        // http://www.w3.org/TR/cors/
        HTTP_GET_OPT_PARAM(params, "origin", origin);
    }
    
    return true;
}

void HTTPSession::ParseType(ReadBuffer& rb)
{
    const char  JSON_PREFIX[] = "json/";
    const char  HTML_PREFIX[] = "html/";
    const char  OCTET_STREAM_PREFIX[] = "raw/";
    
    if (HTTP_MATCH_PREFIX(rb, JSON_PREFIX))
    {
        SetType(JSON);
        rb.Advance(sizeof(JSON_PREFIX) - 1);
    }
    else if (HTTP_MATCH_PREFIX(rb, HTML_PREFIX))
    {
        SetType(HTML);
        rb.Advance(sizeof(HTML_PREFIX) - 1);
    }
    else if (HTTP_MATCH_PREFIX(rb, OCTET_STREAM_PREFIX))
    {
        SetType(OCTET_STREAM);
        rb.Advance(sizeof(OCTET_STREAM_PREFIX) - 1);
    }
    else
        SetType(PLAIN_TEXT);
}

void HTTPSession::SendHeaders()
{
    if (headerSent)
        return;

    if (type == JSON)
        json.Start();
    else
        conn->WriteHeader(HTTP_STATUS_CODE_OK);

    headerSent = true;
}

void HTTPSession::ResponseFail()
{
    ASSERT(headerSent == false);
    if (!conn)
        return;
    
    if (type == JSON)
        json.PrintStatus("error", MSG_FAIL);
    else
        conn->Response(HTTP_STATUS_CODE_OK, MSG_FAIL, sizeof(MSG_FAIL) - 1);
}

void HTTPSession::Redirect(const ReadBuffer& location)
{
    Buffer      extraHeader;

    ASSERT(headerSent == false);
    if (!conn)
        return;
    
    extraHeader.Writef(HTTP_HEADER_LOCATION ": %R" HTTP_CS_CRLF, &location);
    extraHeader.NullTerminate();
    conn->ResponseHeader(HTTP_STATUS_CODE_TEMPORARY_REDIRECT, true, extraHeader.GetBuffer());
}

void HTTPSession::Print(const ReadBuffer& line)
{
    Buffer      header;
    ReadBuffer  tmp;
    
    if (!conn)
        return;

    if (!headerSent)
        SendHeaders();
    
    if (type == JSON)
    {
        tmp = "response";
        json.PrintString(tmp);
        json.PrintColon();
        json.PrintString(line);
    }
    else
    {
        conn->Write(line.GetBuffer(), line.GetLength());
        if (type != OCTET_STREAM)
            conn->Print("\n");
    }
}

void HTTPSession::PrintPair(const ReadBuffer& key, const ReadBuffer& value)
{
    if (!conn)
        return;

    if (!headerSent)
        SendHeaders();

    if (type == JSON)
        json.PrintPair(key.GetBuffer(), key.GetLength(), value.GetBuffer(), value.GetLength());
    else
    {
        conn->Write(key.GetBuffer(), key.GetLength());
        conn->Print(": ");
        conn->Write(value.GetBuffer(), value.GetLength());
        if (type == HTML)
            conn->Print("<br/>");
        else if (type == PLAIN_TEXT)
            conn->Print("\n");
    }
}

void HTTPSession::PrintPair(const char* key, const char* value)
{
    PrintPair(ReadBuffer(key), ReadBuffer(value));
}

void HTTPSession::Flush(bool closeAfterSend)
{
    if (!conn)
        return;

    // TODO: if no data was sent, no headers are sent either

    if (type == JSON)
        json.End();
    
    conn->Flush(closeAfterSend);
    if (closeAfterSend)
        isFlushed = true;
}

void HTTPSession::SetType(Type type_)
{
    const char* mime;
    ReadBuffer  mimeType;
    
    type = type_;

    switch (type)
    {
    case PLAIN_TEXT:
        mime = MIME_TYPE_TEXT_PLAIN;
        break;
    case HTML:
        mime = MIME_TYPE_TEXT_HTML;
        break;
    case JSON:
        mime = MIME_TYPE_APPLICATION_JSON;
        break;
    case OCTET_STREAM:
        mime = MIME_TYPE_APPLICATION_OCTET_STREAM;
        break;
    default:
        mime = MIME_TYPE_TEXT_PLAIN;
        break;
    }

    if (mimeType.GetLength() == 0)
        mimeType.Wrap(mime);
    
    conn->SetContentType(mimeType);
}

bool HTTPSession::IsFlushed()
{
    return isFlushed;
}

bool HTTPSession::RedirectLocalhost(HTTPConnection *conn, HTTPRequest &request)
{
    ReadBuffer  host;

    // fix Windows 7 IPv6 localhost name resolution issue
    host = request.header.GetField(HTTP_HEADER_HOST);
    if (host.BeginsWith("localhost"))
    {
        ReadBuffer  userAgent;
        Buffer      newHost;
        Buffer      ha;
        unsigned    i;
        int         pos;
        
        userAgent = request.header.GetField(HTTP_HEADER_USER_AGENT);
        pos = userAgent.Find(WINDOWS_NT_USER_AGENT);
        if (pos < 0)
            return false;

        newHost.Write("127.0.0.1");
        for (i = 0; i < host.GetLength(); i++)
        {
            if (host.GetCharAt(i) == ':')
            {
                host.Advance(i);
                newHost.Append(host);
                break;
            }
        }
        ha.Writef(HTTP_HEADER_LOCATION ": http://%B%R" HTTP_CS_CRLF, &newHost, &request.line.uri);
	    ha.NullTerminate();
		conn->ResponseHeader(HTTP_STATUS_CODE_TEMPORARY_REDIRECT, true, ha.GetBuffer());
        conn->Flush(true);
		return true;
    }

    return false;
}
