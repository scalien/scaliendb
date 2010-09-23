#ifndef JSON_SESSION_H
#define JSON_SESSION_H

#include "System/Buffers/ReadBuffer.h"

class HTTPConnection;

/*
===============================================================================================

 JSONSession is able to serialize JSON messages on HTTPConnection.

===============================================================================================
*/

class JSONSession
{
public:
    void            Init(HTTPConnection* conn);
    void            SetCallbackPrefix(const ReadBuffer& jsonCallback);

    void            Start();
    void            End();

    void            PrintStatus(const char* status, const char* type = NULL);

    void            PrintString(const char* s, unsigned len);
    void            PrintNumber(double number);
    void            PrintBool(bool b);
    void            PrintNull();
    
    void            PrintObjectStart();
    void            PrintObjectEnd();

    void            PrintArrayStart();
    void            PrintArrayEnd();

    void            PrintColon();
    void            PrintComma();

    void            PrintPair(const char* s, unsigned slen, const char* v, unsigned vlen);

    bool            IsCommaNeeded();
    void            SetCommaNeeded(bool needed);

private:
    ReadBuffer      jsonCallback;
    HTTPConnection* conn;
    unsigned        depth;
    uint64_t        depthComma;
};

#endif
