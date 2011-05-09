#ifndef SHARDDATABASEEXTENSION_H
#define SHARDDATABASEEXTENSION_H

/*
===============================================================================================

 Interface for adding dynamically loadable external commands

===============================================================================================
*/

#ifdef __cplusplus
extern "C" {
#endif

// uint64_t support
#ifndef _WIN32
#include <stdint.h>
#else
typedef unsigned __int64    uint64_t;
#endif

typedef struct ShardExtensionBuffer
{
    char*       buffer;
    unsigned    length;

    // helper functions for integration with server code
#ifdef READBUFFER_H
    ShardExtensionBuffer() { buffer = 0; length = 0; }
    ShardExtensionBuffer(const ReadBuffer& other) { *this = other; }
    operator ReadBuffer& () { return (ReadBuffer&)(*this); }
    ShardExtensionBuffer& operator=(const ReadBuffer& other) { buffer = other.GetBuffer(); length = other.GetLength(); return *this; }
#endif
}
ShardExtensionBuffer;

typedef struct ShardExtensionParam
{
    ShardExtensionBuffer    name;
    unsigned                argc;
    ShardExtensionBuffer*   argv;
    ShardExtensionBuffer    key;
    ShardExtensionBuffer    value;
    ShardExtensionBuffer    out;
    uint64_t                paxosID;
    uint64_t                commandID;
} 
ShardExtensionParam;

typedef struct ShardExtension
{
    void                    (*Init)();
    void                    (*Close)();
    const char*             (*GetName)();
    const char*             (*GetDescription)();
}
ShardExtension;

#define SHARD_EXTENSION_FACTORY "ShardExtensionFactory"

typedef ShardExtension* (*ShardExtensionFactory)();
typedef bool (*ShardExtensionFunction)(ShardExtensionParam*);

void ShardExtensionRegisterFunction(const char* name, ShardExtensionFunction func);
void ShardExtensionUnregisterFunction(const char* name);

#ifdef __cplusplus
}
#endif

#endif
