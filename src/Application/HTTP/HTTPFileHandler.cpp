#include "HTTPFileHandler.h"
#include "HTTPConsts.h"
#include "Mime.h"

#include <stdio.h>

typedef Buffer HeaderArray;

void HttpHeaderAppend(HeaderArray& ha, const char* k, size_t klen, const char* v, size_t vlen)
{
    ha.Append(k, (unsigned) klen);
    ha.Append(": ", 2);
    ha.Append(v, (unsigned) vlen);
    ha.Append(HTTP_CS_CRLF, 2);
}

HTTPFileHandler::HTTPFileHandler(const char* docroot, const char* prefix_)
{
    documentRoot = docroot;
    prefix = prefix_;
}

bool HTTPFileHandler::HandleRequest(HTTPConnection* conn, HTTPRequest& request)
{
    Buffer          path;
    Buffer          tmp;
    Buffer          ha;
    char            buf[128 * 1024];
    FILE*           fp;
    size_t          nread;
    size_t          fsize;
    const char*     mimeType;
    
    if (strncmp(request.line.uri.GetBuffer(), prefix, strlen(prefix)))
        return false;
    
    path.Writef("%s%R", documentRoot, &request.line.uri);
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

