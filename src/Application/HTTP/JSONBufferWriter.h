#ifndef JSON_BUFFERWRITER_H
#define JSON_BUFFERWRITER_H

#include "System/Buffers/ReadBuffer.h"
#include "System/Buffers/Buffer.h"

/*
===============================================================================================

 JSONWriter is able to serialize JSON messages.

===============================================================================================
*/

class JSONBufferWriter
{
public:
    void            Init(Buffer* output_);
    void            SetCallbackPrefix(const ReadBuffer& jsonCallback);

    void            Start();
    void            End();

    void            PrintStatus(const char* status, const char* type = NULL);

    void            PrintString(const ReadBuffer& str);
    void            PrintNumber(int64_t number);
    void            PrintFloatNumber(double number);
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
    Buffer*         output;
    unsigned        depth;
    uint64_t        depthComma;
};

#endif
