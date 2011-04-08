#include "HTTPRequest.h"
#include "HTTPConsts.h"

#define CR 13
#define LF 10

void HTTPRequest::Init()
{
    state = REQUEST_LINE;
    pos = 0;
    contentLength = -1;
    header.Init();
}

void HTTPRequest::Free()
{
    header.Free();
}

int HTTPRequest::Parse(char* buf, int len)
{
    int         reqPos;
    int         headPos;
    unsigned    nread;
    ReadBuffer  cl;
    
    if (state == REQUEST_LINE)
    {
        reqPos = line.Parse(buf, len, 0);
        if (reqPos <= 0)
            return reqPos;
        
        pos = reqPos;
        state = HEADER;
    }
    
    if (state == HEADER)
    {
        headPos = header.Parse(buf, len, pos);
        if (headPos <= 0)
            return headPos;
        if (headPos == len)
            return 0;
        if (headPos > len)
            return -1;
        
        pos = headPos;
        if (buf[pos] == CR && buf[pos + 1] == LF)
        {
            state = CONTENT;
            pos += 2;
        }
        else
            return pos;
    }
    
    if (state == CONTENT)
    {
        if (contentLength < 0)
        {
            cl = header.GetField(HTTP_HEADER_CONTENT_LENGTH);
            if (cl.GetLength() == 0)
                contentLength = 0;
            else
            {
                contentLength = (unsigned) BufferToUInt64(cl.GetBuffer(), cl.GetLength(), &nread);
                if (nread != cl.GetLength())
                    contentLength = 0;
            }
            
            pos += contentLength;
        }
        
        return pos;
    }
    
    return -1;
}
