#include <stdlib.h>
#include "HTTPRequest.h"

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
    int reqPos;
    int headPos;
    const char* cl;
    
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
        if (headPos >= len)
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
            cl = header.GetField("content-length");
            if (!cl)
                contentLength = 0;
            else
                contentLength = (int) strtol(cl, NULL, 10);
            
            pos += contentLength;
        }
        
        return pos;
    }
    
    return -1;
}
