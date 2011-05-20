#include "HTTPFileHandler.h"
#include "HTTPConsts.h"
#include "Mime.h"

#include <stdio.h>

typedef Buffer HeaderArray;

static void HttpHeaderAppend(HeaderArray& ha, const char* k, size_t klen, const char* v, size_t vlen)
{
    ha.Append(k, (unsigned) klen);
    ha.Append(": ", 2);
    ha.Append(v, (unsigned) vlen);
    ha.Append(HTTP_CS_CRLF, 2);
}

void HTTPFileHandler::Init(ReadBuffer& docroot, ReadBuffer& prefix_)
{
    documentRoot = docroot;
    prefix = prefix_;
}

void HTTPFileHandler::SetDirectoryIndex(ReadBuffer& index_)
{
    index = index_;
}

bool HTTPFileHandler::HandleRequest(HTTPConnection* conn, HTTPRequest& request)
{
    Buffer          path;
    Buffer          tmp;
    Buffer          ha;
    ReadBuffer      host;
    ReadBuffer      uri;
    char            buf[128 * 1024];
    FILE*           fp;
    size_t          nread;
    size_t          fsize;
    const char*     mimeType;
	unsigned		i;
    
	// sanity checks
	if (request.line.uri.GetLength() == 0)
		return false;

	if (request.line.uri.BeginsWith("."))
		return false;

    if (!request.line.uri.BeginsWith(prefix))
        return false;
    
	// sanitize path
    path.Writef("%R%R", &documentRoot, &request.line.uri);
	for (i = 0; i < path.GetLength(); i++)
	{
		if (path.GetBuffer()[i] == '\\')
			path.GetBuffer()[i] = '/';
	}

    // fix Windows 7 IPv6 localhost name resolution issue
    host = request.header.GetField(HTTP_HEADER_HOST);
    if (host.BeginsWith("localhost"))
    {
        Buffer      newHost;
        unsigned    i;
        
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
        ha.Writef(HTTP_HEADER_LOCATION ": http://%B%R" HTTP_CS_CRLF, &newHost, &prefix);
	    ha.NullTerminate();
		conn->ResponseHeader(HTTP_STATUS_CODE_TEMPORARY_REDIRECT, true, ha.GetBuffer());
        conn->Flush(true);
		return true;
    }

    // fix trailing slash issues
    uri = request.line.uri;
    if (uri.GetCharAt(uri.GetLength() - 1) == '/')
        uri.SetLength(uri.GetLength() - 1);
    
    if (ReadBuffer::Cmp(uri, prefix) == 0)
	{
		if (path.GetCharAt(path.GetLength() - 1) != '/')
		{
			// redirect to directory
            host = request.header.GetField(HTTP_HEADER_HOST);
            ha.Writef(HTTP_HEADER_LOCATION ": http://%R%R/" HTTP_CS_CRLF, &host, &prefix);
		    ha.NullTerminate();
			conn->ResponseHeader(HTTP_STATUS_CODE_TEMPORARY_REDIRECT, true, ha.GetBuffer());
            conn->Flush(true);
			return true;
		}
        path.Appendf("%R", &index);
	}
    path.NullTerminate();
    
    mimeType = MimeTypeFromExtension(strrchr(path.GetBuffer(), '.'));
    
    fp = fopen(path.GetBuffer(), "rb");
    if (!fp)
        return false;
    
    fseek(fp, 0L, SEEK_END);
    fsize = ftell(fp);
    fseek(fp, 0L, SEEK_SET);
    
    HttpHeaderAppend(ha, 
        HTTP_HEADER_CONTENT_TYPE, sizeof(HTTP_HEADER_CONTENT_TYPE) - 1,
        mimeType, strlen(mimeType));
    
    tmp.Writef("%u", fsize);
    HttpHeaderAppend(ha,
        HTTP_HEADER_CONTENT_LENGTH, sizeof(HTTP_HEADER_CONTENT_LENGTH) - 1,
        tmp.GetBuffer(), tmp.GetLength());

    ha.NullTerminate();
    
    conn->ResponseHeader(HTTP_STATUS_CODE_OK, true, ha.GetBuffer());
    
    while (true)
    {
        nread = fread(buf, 1, sizeof(buf), fp);
        if (feof(fp))
        {
            fclose(fp);
            conn->Write(buf, (unsigned) nread);
            break;
        }
        if (ferror(fp))
        {
            fclose(fp);
            break;
        }
        
        conn->Write(buf, (unsigned) nread);
    }

    conn->Flush(true);
    return true;
}

