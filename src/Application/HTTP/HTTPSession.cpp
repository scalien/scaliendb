#include "HTTPSession.h"
#include "Application/HTTP/UrlParam.h"
#include "Application/HTTP/HTTPRequest.h"
#include "Application/HTTP/HTTPConnection.h"
#include "Application/HTTP/HTTPConsts.h"
#include "Application/HTTP/Mime.h"

#define MSG_FAIL            "Unable to process your request at this time"
#define CONTENT_TYPE_PLAIN  HTTP_HEADER_CONTENT_TYPE ": " MIME_TYPE_TEXT_PLAIN HTTP_CS_CRLF
#define CONTENT_TYPE_HTML   HTTP_HEADER_CONTENT_TYPE ": " MIME_TYPE_TEXT_HTML HTTP_CS_CRLF

HTTPSession::HTTPSession()
{
    headerSent = false;
    conn = NULL;
    type = PLAIN;
    keepAlive = false;
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
    else
        SetType(PLAIN);
}

void HTTPSession::ResponseFail()
{
    assert(headerSent == false);
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

    assert(headerSent == false);
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
    {
        if (type == JSON)
            json.Start();
        else
            conn->WriteHeader(HTTP_STATUS_CODE_OK);

        headerSent = true;
    }
    
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
        conn->Print("\n");
    }
}

void HTTPSession::PrintPair(const ReadBuffer& key, const ReadBuffer& value)
{
    if (!conn)
        return;

    if (!headerSent)
    {
        if (type == JSON)
            json.Start();
        else
            conn->WriteHeader(HTTP_STATUS_CODE_OK);

        headerSent = true;
    }

    if (type == JSON)
        json.PrintPair(key.GetBuffer(), key.GetLength(), value.GetBuffer(), value.GetLength());
    else
    {
        conn->Write(key.GetBuffer(), key.GetLength());
        conn->Print(": ");
        conn->Write(value.GetBuffer(), value.GetLength());
        conn->Print("\n");
    }
}

void HTTPSession::PrintPair(const char* key, const char* value)
{
    PrintPair(ReadBuffer(key), ReadBuffer(value));
}

void HTTPSession::Flush()
{
    if (!conn)
        return;

    if (type == JSON)
        json.End();
    
    conn->Flush(true);
}

void HTTPSession::SetType(Type type_)
{
    const char* mime;
    ReadBuffer  mimeType;
    
    type = type_;

    switch (type)
    {
    case PLAIN:
        mime = MIME_TYPE_TEXT_PLAIN;
        break;
    case HTML:
        mime = MIME_TYPE_TEXT_HTML;
        break;
    case JSON:
        mime = MIME_TYPE_APPLICATION_JSON;
        break;
    default:
        mime = MIME_TYPE_TEXT_PLAIN;
        break;
    }

    if (mimeType.GetLength() == 0)
        mimeType.Wrap(mime);
    
    conn->SetContentType(mimeType);
}
