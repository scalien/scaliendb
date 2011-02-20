#ifndef HTTPSESSION_H
#define HTTPSESSION_H

#include "Application/HTTP/JSONSession.h"

#define HTTP_MATCH_COMMAND(cmd, csl) \
    ((sizeof(csl) == 1 && cmd.GetLength() == 0) || \
    (sizeof(csl) > 1 && cmd.GetLength() > 0 && sizeof(csl)-1 == cmd.GetLength() && \
    memcmp(cmd.GetBuffer(), csl, MIN(cmd.GetLength() + 1, sizeof(csl)) - 1) == 0))

#define HTTP_MATCH_PREFIX(cmd, csl) \
    ((sizeof(csl) == 1 && cmd.GetLength() == 0) || \
    (sizeof(csl) > 1 && cmd.GetLength() > 0 && sizeof(csl)-1 <= cmd.GetLength() && \
    memcmp(cmd.GetBuffer(), csl, MIN(cmd.GetLength() + 1, sizeof(csl)) - 1) == 0))

#define HTTP_GET_OPT_PARAM(params, name, var) \
    params.GetNamed(name, sizeof("" name) - 1, var)

#define HTTP_GET_PARAM(params, name, var) \
    if (!params.GetNamed(name, sizeof("" name) - 1, var)) { return NULL; }

#define HTTP_GET_U64_PARAM(params, name, var) \
    { \
        ReadBuffer  tmp; \
        unsigned    nread; \
        if (!params.GetNamed(name, sizeof("" name) - 1, tmp)) \
            return NULL; \
        var = BufferToUInt64(tmp.GetBuffer(), tmp.GetLength(), &nread); \
        if (nread != tmp.GetLength()) \
            return NULL; \
    }

#define HTTP_GET_OPT_U64_PARAM(params, name, var) \
    { \
        ReadBuffer  tmp; \
        unsigned    nread; \
        if (params.GetNamed(name, sizeof("" name) - 1, tmp)) \
            var = BufferToUInt64(tmp.GetBuffer(), tmp.GetLength(), &nread); \
    }

class UrlParam;         // forward
class HTTPConnection;   // forward
class HTTPRequest;      // forward

/*
===============================================================================================

 HTTPSession
 
===============================================================================================
*/

class HTTPSession
{
public:
    enum Type
    {
        PLAIN,
        HTML,
        JSON
    };
    
    HTTPSession();

    void                SetConnection(HTTPConnection* conn);
    bool                ParseRequest(HTTPRequest& request, ReadBuffer& cmd, UrlParam& params);
    void                ParseType(ReadBuffer& cmd);

    void                ResponseFail();
    void                Redirect(const ReadBuffer& location);
    
    void                Print(const ReadBuffer& line);
    void                PrintPair(const ReadBuffer& key, const ReadBuffer& value);
    void                PrintPair(const char* key, const char* value);
    void                Flush();
    void                SetType(Type type);

    HTTPConnection*     conn;
    Type                type;
    JSONSession         json;
    bool                headerSent;
    bool                keepAlive;
    ReadBuffer          mimeType;
    ReadBuffer          uri;
}; 

#endif
