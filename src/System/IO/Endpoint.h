#ifndef ENDPOINT_H
#define ENDPOINT_H

#include "System/Platform.h"
#include "System/Log.h"
#include "System/Buffers/ReadBuffer.h"

#define ENDPOINT_ANY_ADDRESS    0
#define ENDPOINT_STRING_SIZE    32
#define ENDPOINT_SOCKADDR_SIZE  16

typedef uint32_t Address;

/*
===============================================================================================

 Endpoint

===============================================================================================
*/

class Endpoint
{
public:
    Endpoint();
    
    bool            operator==(const Endpoint &other) const;
    bool            operator!=(const Endpoint &other) const;
    
    bool            Set(const char* ip, int port, bool resolve = false);
    bool            Set(const char* ip_port, bool resolve = false);
    bool            Set(ReadBuffer ip_port, bool resolve = false);

    bool            SetPort(int port);
    int             GetPort();
    
    Address         GetAddress();
    char*           GetSockAddr();
    
    const char*     ToString();
    const char*     ToString(char s[ENDPOINT_STRING_SIZE]);
    ReadBuffer      ToReadBuffer();
    
    bool            IsSet();

    static Address  GetLoopbackAddress();
    static bool     IsValidEndpoint(ReadBuffer ip_port);
    static Mutex&   GetMutex();

private:

    char            buffer[ENDPOINT_STRING_SIZE];
    char            saBuffer[ENDPOINT_SOCKADDR_SIZE];
};

#endif
