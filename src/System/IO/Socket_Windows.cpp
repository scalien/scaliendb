#ifdef _WIN32
#include "Socket.h"
#include "System/Log.h"
#include "System/Common.h"
#include "System/Platform.h"

#include <winsock2.h>

#ifndef INADDR_NONE
#define INADDR_NONE 0xffffffff
#endif

unsigned long iftonl(const char* interface_);

/*
 * The Socket implementation is tightly coupled with the
 * IOProcessor because Windows asynchronous mechanism and
 * the IO completion uses the same Windows specific OVERLAPPED
 * structures, therefore these functions are imported here.
 */
bool IOProcessorRegisterSocket(FD& fd);
bool IOProcessorUnregisterSocket(FD& fd);
bool IOProcessorAccept(const FD& listeningFd, FD& fd);
bool IOProcessorConnect(FD& fd, Endpoint& endpoint);
extern unsigned SEND_BUFFER_SIZE;

Socket::Socket()
{
    fd = INVALID_FD;
    listening = false;
}

bool Socket::Create()
{
    BOOL    trueval = TRUE;

    if (fd.handle != INVALID_SOCKET)
    {
        Log_Trace("Called Create() on existing socket");
        return false;
    }

    listening = false;

    // create the socket with WSA_FLAG_OVERLAPPED to support async operations
    fd.handle = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (fd.handle == INVALID_SOCKET)
        return false;

    if (setsockopt(fd.handle, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, (char *)&trueval, sizeof(BOOL)))
    {
        Log_Errno();
        Close();
        return false;
    }

    if (setsockopt(fd.handle, SOL_SOCKET, SO_SNDBUF, (char *) &SEND_BUFFER_SIZE, sizeof(SEND_BUFFER_SIZE)))
    {
        Log_Errno();
        Close();
        return false;
    }

    // set FD index too
    IOProcessorRegisterSocket(fd);

    return true;
}

bool Socket::Bind(int port)
{
    int                 ret;
    struct sockaddr_in  sa;

    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    sa.sin_addr.s_addr = htonl(INADDR_ANY);
    
    ret = bind(fd.handle, (struct sockaddr *)&sa, sizeof(struct sockaddr_in));
    if (ret < 0)
    {
        Log_Errno();
        Close();
        return false;
    }
    
    return true;
}

bool Socket::SetNonblocking()
{
    u_long  nonblocking;

    if (fd.handle == INVALID_SOCKET)
    {
        Log_Trace("SetNonblocking on invalid file descriptor");
        return false;
    }
    
    nonblocking = 1;
    if (ioctlsocket(fd.handle, FIONBIO, &nonblocking) == SOCKET_ERROR)
        return false;
        
    return true;
}

bool Socket::SetNodelay()
{
    BOOL    nodelay;
    
    if (fd.handle == INVALID_SOCKET)
    {
        Log_Trace("SetNodelay on invalid file descriptor");
        return false;
    }
    
    // Nagle algorithm is disabled if TCP_NODELAY is enabled.
    nodelay = TRUE;
    if (setsockopt(fd.handle, IPPROTO_TCP, TCP_NODELAY, (char *) &nodelay, sizeof(nodelay)) == SOCKET_ERROR)
    {
        Log_Trace("setsockopt() failed");
        return false;
    }

    return true;
    
}


bool Socket::Listen(int port, int backlog)
{
    int ret;
    
    if (!Bind(port))
        return false;
    
    ret = listen(fd.handle, backlog);
    if (ret < 0)
    {
        Log_Errno();
        Close();
        return false;
    }
    
    listening = true;
    
    return true;
}

bool Socket::Accept(Socket *newSocket)
{
    if (!IOProcessorAccept(fd, newSocket->fd))
    {
        Log_Errno();
        Close();
        return false;
    }

    // register the newly created socket
    if (!IOProcessorRegisterSocket(newSocket->fd))
    {
        Log_Message("Ran out of IO connections, please increase io.maxfd in config file!");
        return false;
    }
    
    return true;
}

bool Socket::Connect(Endpoint &endpoint)
{
    if (!IOProcessorConnect(fd, endpoint))
    {
        Log_Errno();
        return false;
    }

    return true;
}

bool Socket::GetEndpoint(Endpoint &endpoint)
{
    int                 ret;
    int                 len = ENDPOINT_SOCKADDR_SIZE;
    struct sockaddr*    sa = (struct sockaddr*) endpoint.GetSockAddr();
    
    ret = getpeername(fd.handle, sa, &len);
    
    if (ret == SOCKET_ERROR)
    {
        Log_Errno();
        Close();
        return false;
    }
    
    return true;
}

const char* Socket::ToString(char s[ENDPOINT_STRING_SIZE])
{
    Endpoint    endpoint;
    
    if (!GetEndpoint(endpoint))
        return "";
    
    return endpoint.ToString(s);
}

bool Socket::SendTo(void *data, int count, const Endpoint &endpoint)
{
    int                     ret;
    const struct sockaddr*  sa = (const struct sockaddr*) ((Endpoint &) endpoint).GetSockAddr();


    ret = sendto(fd.handle, (const char*) data, count, 0,
                 sa,
                 ENDPOINT_SOCKADDR_SIZE);
    
    if (ret < 0)
    {
        Log_Errno();
        return false;
    }
    
    return true;
}

int Socket::Send(const char* data, int count, int /*timeout*/)
{
    size_t      left;
    int         nwritten;
    
    left = count;
    while (left > 0)
    {
        if ((nwritten = send((SOCKET) fd.handle, (char*) data, count, 0)) == SOCKET_ERROR)
        {
            // TODO: timeout and error handling
            if (WSAGetLastError() == WSAEWOULDBLOCK)
                return 0;
            
            return -1;
        }

        left -= nwritten;
        data += nwritten;
    }
    
    return count;
}

int Socket::Read(char* data, int count, int /*timeout*/)
{
    int     ret;

    ret = recv((SOCKET)fd.handle, (char *)data, count, 0);
    // TODO: timeout and better error handling
    if (ret == SOCKET_ERROR)
    {
        if (WSAGetLastError() == WSAEWOULDBLOCK)
            return 0;
        return -1;
    }
    else if (ret == 0)
    {
        // graceful disconnection
        return -1;
    }
    return ret;
}

void Socket::Close()
{
    int ret;
    
    if (fd.handle != INVALID_SOCKET)
    {
        IOProcessorUnregisterSocket(fd);
        ret = closesocket(fd.handle);

        if (ret < 0)
            Log_Errno();

        fd.handle = INVALID_SOCKET;
        fd.index = -1;
    }
}

unsigned long iftonl(const char* interface_)
{
    int pos;
    int len;
    unsigned long a;
    unsigned long b;
    unsigned long c;
    unsigned long d;
    unsigned long addr;
    unsigned nread;
    
    nread = 0;
    pos = 0;
    len = strlen(interface_);
    
    a = BufferToUInt64(interface_ + pos, len - pos, &nread);
    if (nread == 0 || a > 255)
        return INADDR_NONE;
    
    pos += nread;
    if (interface_[pos++] != '.')
        return INADDR_NONE;
    
    b = BufferToUInt64(interface_ + pos, len - pos, &nread);
    if (nread == 0 || b > 255)
        return INADDR_NONE;
    
    pos += nread;
    if (interface_[pos++] != '.')
        return INADDR_NONE;

    c = BufferToUInt64(interface_ + pos, len - pos, &nread);
    if (nread == 0 || c > 255)
        return INADDR_NONE;
    
    pos += nread;
    if (interface_[pos++] != '.')
        return INADDR_NONE;

    d = BufferToUInt64(interface_ + pos, len - pos, &nread);
    if (nread == 0 || d > 255)
        return INADDR_NONE;
    
    pos += nread;
    if (interface_[pos] != '\0' &&
        interface_[pos] != ':')
        return INADDR_NONE;
    
    addr = (d & 0xff) << 24 | (c & 0xff) << 16 | (b & 0xff) << 8 | (a & 0xff);
    return addr;
}

#endif
