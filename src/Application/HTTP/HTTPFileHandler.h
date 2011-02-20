#ifndef HTTP_FILE_HANDLER_H
#define HTTP_FILE_HANDLER_H

#include "System/Buffers/ReadBuffer.h"
#include "HTTPServer.h"

/*
===============================================================================================

 HTTPFileHandler serves static files over HTTP.

===============================================================================================
*/

class HTTPFileHandler : public HTTPHandler
{
public:
    void            Init(ReadBuffer& docroot, ReadBuffer& prefix);
    void            SetDirectoryIndex(ReadBuffer& index);
    
    virtual bool    HandleRequest(HTTPConnection* conn, HTTPRequest& request);
    
private:
    ReadBuffer      documentRoot;
    ReadBuffer      prefix;
    ReadBuffer      index;
};

#endif
