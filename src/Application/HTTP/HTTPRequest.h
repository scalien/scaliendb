#ifndef HTTPREQUEST_H
#define HTTPREQUEST_H

#include "IMF.h"

/*
===============================================================================================

 HTTPRequest

===============================================================================================
*/

class HTTPRequest
{
public:
    typedef IMFHeader::RequestLine RequestLine;
    enum State
    {
        REQUEST_LINE,
        HEADER,
        CONTENT
    };

    IMFHeader       header;
    RequestLine     line;
    State           state;
    int             pos;
    int             contentLength;
    
    void            Init();
    void            Free();
    int             Parse(char *buf, int len);
};

#endif
