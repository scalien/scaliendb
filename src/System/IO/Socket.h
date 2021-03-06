#ifndef SOCKET_H
#define SOCKET_H

#include "System/Platform.h"
#include "Endpoint.h"
#include "FD.h"

/*
===============================================================================================

 Socket

===============================================================================================
*/

class Socket
{
public:
    Socket();
    
    bool            Create();

    bool            SetNonblocking();
    bool            SetNodelay();
    bool            SetReceiveBufferSize(unsigned size);

    bool            Bind(int port);
    bool            Bind(const Endpoint& endpoint);
    bool            Listen(int port, int backlog = 1024);
    bool            Accept(Socket* newSocket);
    bool            Connect(Endpoint &endpoint);

    bool            GetEndpoint(Endpoint &endpoint);
    const char*     ToString(char s[ENDPOINT_STRING_SIZE]);

    bool            SendTo(void* data, int count, const Endpoint &endpoint);
    int             Send(const char* data, int count, int timeout = 0);
    int             Read(char* data, int count, int timeout = 0);
    
    void            Close();

public: 
    FD              fd;
    bool            listening;

};

#endif
