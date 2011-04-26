#include "Endpoint.h"
#include "System/Buffers/Buffer.h"
#include "System/Mutex.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#ifdef _WIN32
#include <winsock2.h>
#define s_addr S_un.S_addr
#define in_addr_t unsigned long
#undef SetPort
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#endif

#ifdef _WIN32
int inet_aton(const char *cp, struct in_addr *in)
{
    // TODO: IPv6
    if (strcmp(cp, "255.255.255.255") == 0)
    {
        in->s_addr = (u_long) 0xFFFFFFFF;
        return 1;
    }
    
    in->s_addr = inet_addr(cp);
    if (in->s_addr == INADDR_NONE)
        return 0;

    return 1;
}
#endif

// TODO this is temporary, we need a real DNS resolver
static bool DNS_ResolveIpv4(const char* name, struct in_addr* addr)
{
    // FIXME gethostbyname is not multithread-safe!
    struct hostent* hostent;
    static Mutex    mutex;
    MutexGuard      mutexGuard(mutex);

    hostent = gethostbyname(name);
    if (!hostent)
        return false;

    if (hostent->h_addrtype != AF_INET)
        return false;

    addr->s_addr = *(in_addr_t *) hostent->h_addr_list[0];

    // TODO: getaddrinfo doesn't work with "localhost"
#ifdef DNS_RESOLVE_GETADDRINFO
    struct addrinfo     hints;
    struct addrinfo*    res;
    
    memset((void*) &hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    if (!getaddrinfo(name, NULL, &hints, &res))
    {
        freeaddrinfo(res);
        return false;
    }
    
    addr->s_addr = *(u_long *) res->ai_addr;
    
    freeaddrinfo(res);
#endif

    return true;
}

Endpoint::Endpoint()
{
    struct sockaddr_in *sa;
    
    sa = (struct sockaddr_in *) &saBuffer;

    memset((char *) sa, 0, sizeof(sa));
    sa->sin_family = 0; sa->sin_port = 0; 
    sa->sin_addr.s_addr = 0; sa->sin_zero[0] = 0;
    buffer[0] = 0;
}

bool Endpoint::operator==(const Endpoint &other) const
{
    struct sockaddr_in *sa = (struct sockaddr_in *) &saBuffer;
    struct sockaddr_in *other_sa = (struct sockaddr_in *) &other.saBuffer;

    return  sa->sin_family == other_sa->sin_family &&
            sa->sin_port == other_sa->sin_port &&
            sa->sin_addr.s_addr == other_sa->sin_addr.s_addr;
}

bool Endpoint::operator!=(const Endpoint &other) const
{
    return !operator==(other);
}

bool Endpoint::Set(const char* ip, int port, bool resolv)
{
    struct sockaddr_in *sa;
    
    sa = (struct sockaddr_in *) &saBuffer;

    memset((char *) sa, 0, sizeof(sa));
    sa->sin_family = AF_INET;
    sa->sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &sa->sin_addr) == 0)
    {
        if (resolv)
        {
            if (!DNS_ResolveIpv4(ip, &sa->sin_addr))
            {
                Log_Trace("DNS resolv failed");
                return false;
            }
            else
                return true;
        }
        
        Log_Trace("inet_aton() failed");
        return false;
    }
    return true;
}

bool Endpoint::Set(const char* ip_port, bool resolv)
{
    const char*     p;
    int             port;
    bool            ret;
    Buffer          ipbuf;

    p = ip_port;

    if (!IsValidEndpoint(ReadBuffer(ip_port)))
        return false;
    
    while (*p != '\0' && *p != ':')
        p++;
    
    if (*p == '\0')
    {
        Log_Trace("No ':' in host specification");
        return false;
    }

    ipbuf.Append(ip_port, p - ip_port);
    ipbuf.NullTerminate();
    p++;
    
    port = -1;
    port = atoi(p);
    if (port < 1 || port > 65535)
    {
        Log_Trace("atoi() failed to produce a sensible value");
        return false;
    }

    ret = Set(ipbuf.GetBuffer(), port, resolv);
    
    return ret;
}

bool Endpoint::Set(ReadBuffer ip_port, bool resolv)
{
    const char*     p;
    const char*     start;
    int             port;
    bool            ret;
    Buffer          ipbuf;
    Buffer          portbuf;

    if (!IsValidEndpoint(ReadBuffer(ip_port)))
        return false;
    
    start = p = ip_port.GetBuffer();
    
    p = FindInBuffer(ip_port.GetBuffer(), ip_port.GetLength(), ':');
    if (*p != ':')
    {
        Log_Trace("No ':' in host specification");
        return false;
    }

    ipbuf.Append(start, p - start);
    ipbuf.NullTerminate();
    
    p++;
    portbuf.Append(p, ip_port.GetLength() - (p - start));
    portbuf.NullTerminate();
    
    port = -1;
    port = atoi(portbuf.GetBuffer());
    if (port < 1 || port > 65535)
    {
        Log_Trace("atoi() failed to produce a sensible value");
        return false;
    }

    ret = Set(ipbuf.GetBuffer(), port, resolv);
    
    return ret;
}

bool Endpoint::SetPort(int port)
{
    struct sockaddr_in *sa;

    sa = (struct sockaddr_in *) &saBuffer;
    sa->sin_port = htons((uint16_t)port);

    return true;
}

int Endpoint::GetPort()
{
    struct sockaddr_in *sa;
    
    sa = (struct sockaddr_in *) &saBuffer;

    return ntohs(sa->sin_port);
}

Address Endpoint::GetAddress()
{
    struct sockaddr_in *sa;
    
    sa = (struct sockaddr_in *) &saBuffer;

    return ntohl(sa->sin_addr.s_addr);
}

char* Endpoint::GetSockAddr()
{
    return saBuffer;
}

const char* Endpoint::ToString()
{
    return ToString(buffer);
}

const char* Endpoint::ToString(char s[ENDPOINT_STRING_SIZE])
{
    struct sockaddr_in *sa = (struct sockaddr_in *) &saBuffer;
    unsigned long addr;

    // inet_ntoa is not thread-safe and have a memory-leak issue on Linux
    addr = ntohl(sa->sin_addr.s_addr);
    snprintf(s, ENDPOINT_STRING_SIZE, "%lu.%lu.%lu.%lu:%u",
        (addr & 0xFF000000UL) >> 24,
        (addr & 0x00FF0000UL) >> 16,
        (addr & 0x0000FF00UL) >> 8,
        (addr & 0x000000FFUL),
        ntohs(sa->sin_port));
    
    return s;
}

ReadBuffer Endpoint::ToReadBuffer()
{
    size_t len;
    
    // TODO: optimize out strlen by doing snprintf directly
    ToString();
    len = strlen(buffer);
    
    return ReadBuffer(buffer, len);
}

bool Endpoint::IsSet()
{
    struct sockaddr_in *sa;
    
    sa = (struct sockaddr_in *) &saBuffer;
    if (sa->sin_port == 0)
        return false;
    return true;
}

bool Endpoint::IsValidEndpoint(ReadBuffer ip_port)
{
    // Valid endpoint is either <IP-Address>:<port> or <Domain-Name>:<port>
    // Valid IP-Address is when it is consists only from numbers and three dots between the numbers
    // Valid domain names: http://en.wikipedia.org/wiki/Hostname#Restrictions_on_valid_host_names

    const char  VALID_CHARS[] = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    unsigned    i;
    bool        labelStart;
    bool        isNumeric;
    bool        isPort;
    unsigned    numCommas;
    unsigned    numPortChars;
    char        c;
    char        prev;

    // assuming fully numeric names
    isNumeric = true;
    isPort = false;
    numCommas = 0;
    numPortChars = 0;
    labelStart = true;
    for (i = 0; i < ip_port.GetLength(); i++)
    {
        c = ip_port.GetCharAt(i);

        if (c == '.' || c == ':')
        {
            // must not start with comma or colon
            if (i == 0)
                return false;

            // labels must not end with hyphens
            if (prev == VALID_CHARS[0])
                return false;
            
            if (c == '.')
            {
                labelStart = true;
                numCommas++;
            }
            
            if (c == ':')
            {
                if (isPort)
                    return false;
                isPort = true;
            }
        }
        else
        {        
            // labels must not start with hyphens
            if (labelStart && c == VALID_CHARS[0])
                return false;
            
            if (isPort)
            {
                if (!isdigit(c))
                    return false;
                numPortChars++;
            }
            
            if (isNumeric && !isdigit(c))
                isNumeric = false;
            
            if (strchr(VALID_CHARS, c) == NULL)
                return false;

            labelStart = false;
        }
        
        prev = c;
    }

    if (isNumeric && numCommas != 3)
        return false;
    if (numPortChars == 0)
        return false;
    
    return true;
}
