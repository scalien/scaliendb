#ifndef _WIN32

#include "Socket.h"
#include "System/Log.h"
#include "System/Common.h"
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#ifndef INADDR_NONE
#define INADDR_NONE 0xffffffff
#endif

unsigned long iftonl(const char* interface);

Socket::Socket()
{
    fd = -1;
    listening = false;
}

bool Socket::Create()
{
    if (fd >= 0)
    {
        Log_Trace("Called Create() on existing socket");
        return false;
    }
    
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        Log_Errno();
        return false;
    }
        
    proto = proto_;
    listening = false;
    
//    SetReceiveBufferSize(64*KiB);
    
    return true;
}

bool Socket::SetNonblocking()
{
    int ret;
    
    if (fd < 0)
    {
        Log_Trace("SetNonblocking on invalid file descriptor");
        return false;
    }
    
    ret = fcntl(fd, F_GETFL, 0);
    if (ret < 0)
    {
        Log_Errno();
        return false;
    }

    ret = fcntl(fd, F_SETFL, ret | O_NONBLOCK);
    if (ret < 0)
    {
        Log_Errno();
        return false;
    }
    
    return true;
}

bool Socket::SetNodelay()
{
    int     ret;
    int     sockopt;

    if (fd < 0)
    {
        Log_Trace("SetNodelay on invalid file descriptor");
        return false;
    }
    
    sockopt = 1;
    ret = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &sockopt, sizeof(sockopt));
    if (ret < 0)
    {
        Log_Errno();
        return false;
    }
    
    return true;
}

bool Socket::SetReceiveBufferSize(unsigned size)
{
    int     ret;

    if (fd < 0)
    {
        Log_Trace("SetRecieveBufferSize on invalid file descriptor");
        return false;
    }
    
    ret = setsockopt(fd, IPPROTO_TCP, SO_RCVBUF, &size, sizeof(size));
    if (ret < 0)
    {
        Log_Errno();
        return false;
    }
    
    return true;
}

bool Socket::Bind(int port)
{
    int                 ret;
    struct sockaddr_in  sa;
    int                 sockopt;

    sockopt = 1;
    ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sockopt, sizeof(sockopt));
    if (ret < 0)
    {
        Log_Errno();
        Close();
        return false;
    }
    
    memset(&sa, 0, sizeof(sa));
    sa.sin_port = htons((uint16_t)port);
    sa.sin_addr.s_addr = htonl(INADDR_ANY);
    
    ret = bind(fd, (struct sockaddr *)&sa, sizeof(struct sockaddr_in));
    if (ret < 0)
    {
        Log_Errno();
        Close();
        return false;
    }
    
    return true;
}

bool Socket::Listen(int port, int backlog)
{
    int ret;
    
    if (!Bind(port))
        return false;
    
    ret = listen(fd, backlog);
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
    newSocket->fd = accept(fd, NULL, NULL);
    if (newSocket->fd < 0)
    {
        Log_Errno();
        Close();
        return false;
    }
    
    return true;
}

bool Socket::Connect(Endpoint &endpoint)
{
    int ret;
    struct sockaddr* sa = (struct sockaddr*) endpoint.GetSockAddr();
    
    ret = connect(fd, sa, ENDPOINT_SOCKADDR_SIZE);
    
    if (ret < 0)
    {
        if (errno != EINPROGRESS)
        {
            Log_Errno();
            return false;
        }
    }

    return true;
}

bool Socket::GetEndpoint(Endpoint &endpoint)
{
    int ret;
    struct sockaddr* sa = (struct sockaddr*) endpoint.GetSockAddr();
    socklen_t len = ENDPOINT_SOCKADDR_SIZE;
    
    ret = getpeername(fd, sa, &len);
    
    if (ret < 0)
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
    int ret;
    const struct sockaddr* sa =
    (const struct sockaddr*) ((Endpoint&) endpoint).GetSockAddr();
    
    ret = sendto(fd, data, count, 0, sa, ENDPOINT_SOCKADDR_SIZE);
    
    if (ret < 0)
    {
        Log_Errno();
        return false;
    }
    
    return true;
}

int Socket::Send(const char* data, int count, int timeout)
{
    size_t      left;
    ssize_t     nwritten;
    timeval     tv;
    fd_set      fds;
    int         ret;
    
    left = count;
    while (left > 0)
    {
        if ((nwritten = write(fd, data, count)) < 0)
        {
            if (errno == EINTR)
            {
                nwritten = 0;
            }
            else if (errno == EPIPE)
            {
                return -1;
            }
            else if (errno == EAGAIN)
            {
                FD_ZERO(&fds);
                FD_SET(fd, &fds);
                if (timeout)
                {
                    tv.tv_sec = timeout / 1000;
                    tv.tv_usec = (timeout % 1000) * 1000;
                    ret = select(fd + 1, NULL, &fds, NULL, &tv);
                }
                else
                    ret = select(fd + 1, NULL, &fds, NULL, NULL);

                if (ret <= 0)
                    return -1;

                nwritten = 0;
            }
            else
                return -1;
        }

        left -= nwritten;
        data += nwritten;
    }
    
    return count;
}

int Socket::Read(char* data, int count, int timeout)
{
    ssize_t     nread;
    timeval     tv;
    fd_set      fds;
    int         ret;
    
again:
    if ((nread = read(fd, data, count)) < 0)
    {
        Log_Errno();
        if (errno == EINTR)
        {
            nread = 0;
            goto again;
        }
        else if (errno == EAGAIN)
        {
            FD_ZERO(&fds);
            FD_SET(fd, &fds);
            if (timeout)
            {
                tv.tv_sec = timeout / 1000;
                tv.tv_usec = (timeout % 1000) * 1000;
                ret = select(fd + 1, &fds, NULL, &fds, &tv);
            }
            else
                ret = select(fd + 1, &fds, NULL, &fds, NULL);

            if (ret <= 0)
                return -1;

            goto again;
        }
        else
            return -1;
    }

    return nread;
}

void Socket::Close()
{
    int ret;
    
    if (fd != -1)
    {
        ret = close(fd);

        if (ret < 0)
            Log_Errno();

        fd = -1;
    }
}

unsigned long iftonl(const char* interface)
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
    len = strlen(interface);
    
    a = BufferToUInt64(interface + pos, len - pos, &nread);
    if (nread == 0 || a > 255)
        return INADDR_NONE;
    
    pos += nread;
    if (interface[pos++] != '.')
        return INADDR_NONE;
    
    b = BufferToUInt64(interface + pos, len - pos, &nread);
    if (nread == 0 || b > 255)
        return INADDR_NONE;
    
    pos += nread;
    if (interface[pos++] != '.')
        return INADDR_NONE;

    c = BufferToUInt64(interface + pos, len - pos, &nread);
    if (nread == 0 || c > 255)
        return INADDR_NONE;
    
    pos += nread;
    if (interface[pos++] != '.')
        return INADDR_NONE;

    d = BufferToUInt64(interface + pos, len - pos, &nread);
    if (nread == 0 || d > 255)
        return INADDR_NONE;
    
    pos += nread;
    if (interface[pos] != '\0' &&
        interface[pos] != ':')
        return INADDR_NONE;
    
    addr = (d & 0xff) << 24 | (c & 0xff) << 16 | (b & 0xff) << 8 | (a & 0xff);
    return addr;
}

#endif
