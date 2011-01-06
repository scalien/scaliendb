#ifdef PLATFORM_LINUX

#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <signal.h>
#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include "System/IO/IOProcessor.h"
#include "System/Common.h"
#include "System/Log.h"
#include "System/Time.h"
#include "System/Events/EventLoop.h"

#define MAX_EVENTS          1024
#define PIPEOP              IOOperation::UNKNOWN


class PipeOp : public IOOperation
{
public:
    PipeOp()
    {
        type = PIPEOP;
        pipe[0] = pipe[1] = -1;
    }
    
    ~PipeOp()
    {
        Close();
    }
    
    void Close()
    {
        if (pipe[0] >= 0)
        {
            close(pipe[0]);
            pipe[0] = -1;
        }
        if (pipe[1] >= 0)
        {
            close(pipe[1]);
            pipe[1] = -1;
        }
    }
    
    int         pipe[2];
    Callable    callback;
};

class EpollOp
{
public:
    EpollOp()
    {
        read = NULL;
        write = NULL;
    }
    
    IOOperation*    read;
    IOOperation*    write;
};

static int              epollfd = 0;
static int              maxfd;
static PipeOp           asyncPipeOp;
static EpollOp*         epollOps;
static volatile bool    terminated = false;
static volatile int     numClient = 0;

static bool             AddEvent(int fd, uint32_t filter, IOOperation* ioop);

static void             ProcessAsyncOp();
static void             ProcessIOOperation(IOOperation* ioop);
static void             ProcessTCPRead(TCPRead* tcpread);
static void             ProcessTCPWrite(TCPWrite* tcpwrite);
static void             ProcessUDPRead(UDPRead* udpread);
static void             ProcessUDPWrite(UDPWrite* udpwrite);


bool /*IOProcessor::*/InitPipe(PipeOp &pipeop, Callable callback)
{
    if (pipe(pipeop.pipe) < 0)
    {
        Log_Errno();
        return false;
    }

    if (pipeop.pipe[0] < 0 || pipeop.pipe[1] < 0)
        return false;
    
    fcntl(pipeop.pipe[0], F_SETFD, FD_CLOEXEC);
    fcntl(pipeop.pipe[0], F_SETFL, O_NONBLOCK);
    //fcntl(pipeop.pipe[1], F_SETFL, O_NONBLOCK);

    if (!AddEvent(pipeop.pipe[0], EPOLLIN, &pipeop))
        return false;
    
    pipeop.callback = callback;

    return true;
}

void SignalHandler(int )
{
    terminated = true;
}

void SetupSignals()
{
    struct sigaction    sa;
    sigset_t            mask;
    
    sigfillset(&mask);
    pthread_sigmask(SIG_SETMASK, &mask, NULL);

    memset(&sa, 0, sizeof(sa));
    sigfillset(&sa.sa_mask);
    sa.sa_handler = SIG_IGN;
    sigaction(SIGHUP, &sa, NULL);
    sigaction(SIGQUIT, &sa, NULL);
    sigaction(SIGPIPE, &sa, NULL);
    
    sa.sa_handler = SignalHandler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGBUS, &sa, NULL);
    sigaction(SIGFPE, &sa, NULL);
    sigaction(SIGILL, &sa, NULL);
    sigaction(SIGSEGV, &sa, NULL);
    sigaction(SIGSYS, &sa, NULL);
    sigaction(SIGXCPU, &sa, NULL);
    sigaction(SIGXFSZ, &sa, NULL);

    sigemptyset(&mask);
    pthread_sigmask(SIG_SETMASK, &mask, NULL);
}
void IOProcessor::BlockSignals(int blockMode)
{
    struct sigaction    sa;
    sigset_t            mask;

    if (blockMode == IOPROCESSOR_BLOCK_INTERACTIVE)
    {
        sigfillset(&mask);
        pthread_sigmask(SIG_SETMASK, &mask, NULL);

        memset(&sa, 0, sizeof(sa));
        sigfillset(&sa.sa_mask);
        sa.sa_handler = SIG_IGN;
        sigaction(SIGHUP, &sa, NULL);
        sigaction(SIGQUIT, &sa, NULL);
        sigaction(SIGPIPE, &sa, NULL);

        sigemptyset(&mask);
        pthread_sigmask(SIG_SETMASK, &mask, NULL);
    }
    else if (blockMode == IOPROCESSOR_BLOCK_ALL)
    {
        sigfillset(&mask);
        pthread_sigmask(SIG_SETMASK, &mask, NULL);

        memset(&sa, 0, sizeof(sa));
        sigfillset(&sa.sa_mask);
        sa.sa_handler = SIG_IGN;
        sigaction(SIGHUP, &sa, NULL);
        sigaction(SIGQUIT, &sa, NULL);
        sigaction(SIGPIPE, &sa, NULL);

        sa.sa_handler = SignalHandler;
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);
        sigaction(SIGBUS, &sa, NULL);
        sigaction(SIGFPE, &sa, NULL);
        sigaction(SIGILL, &sa, NULL);
        sigaction(SIGSEGV, &sa, NULL);
        sigaction(SIGSYS, &sa, NULL);
        sigaction(SIGXCPU, &sa, NULL);
        sigaction(SIGXFSZ, &sa, NULL);

        sigemptyset(&mask);
        pthread_sigmask(SIG_SETMASK, &mask, NULL);
    }
    
    if (blockMode == IOPROCESSOR_NO_BLOCK)
    {
        sigfillset(&mask);
        pthread_sigmask(SIG_SETMASK, &mask, NULL);

        memset(&sa, 0, sizeof(sa));
        sigfillset(&sa.sa_mask);
        sa.sa_handler = SIG_DFL;
        sigaction(SIGHUP, &sa, NULL);
        sigaction(SIGQUIT, &sa, NULL);
        sigaction(SIGPIPE, &sa, NULL);
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);
        sigaction(SIGBUS, &sa, NULL);
        sigaction(SIGFPE, &sa, NULL);
        sigaction(SIGILL, &sa, NULL);
        sigaction(SIGSEGV, &sa, NULL);
        sigaction(SIGSYS, &sa, NULL);
        sigaction(SIGXCPU, &sa, NULL);
        sigaction(SIGXFSZ, &sa, NULL);

        sigemptyset(&mask);
        pthread_sigmask(SIG_SETMASK, &mask, NULL);
    }
}

bool IOProcessor::Init(int maxfd_)
{
    int i;
    rlimit rl;

    terminated = false;
    numClient++;
    
    if (epollfd > 0)
        return true;

    if (maxfd_ < 0)
    {
        Log_Message("Invalid maxfd value!");
        return false;
    }

    maxfd = maxfd_;
    rl.rlim_cur = maxfd;
    rl.rlim_max = maxfd;
    if (setrlimit(RLIMIT_NOFILE, &rl) < 0)
    {
        Log_Errno();
    }
    
    epollfd = epoll_create(maxfd);
    if (epollfd < 0)
    {
        Log_Errno();
        return false;
    }

    epollOps = new EpollOp[maxfd];
    for (i = 0; i < maxfd; i++)
    {
        epollOps[i].read = NULL;
        epollOps[i].write = NULL;
    }
    
    if (!InitPipe(asyncPipeOp, CFunc(ProcessAsyncOp)))
        return false;

    return true;
}

void IOProcessor::Shutdown()
{
    numClient--;
    
    if (epollfd == 0 || numClient > 0)
        return;

    close(epollfd);
    epollfd = 0;
    delete[] epollOps;
    asyncPipeOp.Close();
}

bool IOProcessor::Add(IOOperation* ioop)
{
    uint32_t    filter;
    
    if (ioop->active)
        return true;
    
    if (ioop->pending)
        return true;
    
    filter = EPOLLONESHOT;
    if (ioop->type == IOOperation::TCP_READ || ioop->type == IOOperation::UDP_READ)
        filter |= EPOLLIN;
    else if (ioop->type == IOOperation::TCP_WRITE || ioop->type == IOOperation::UDP_WRITE)
        filter |= EPOLLOUT;
    
    return AddEvent(ioop->fd, filter, ioop);
}

bool AddEvent(int fd, uint32_t event, IOOperation* ioop)
{
    int                 nev;
    struct epoll_event  ev;
    EpollOp             *epollOp;
    bool                hasEvent;
    
    if (epollfd < 0)
    {
        Log_Trace("epollfd < 0");
        return false;
    }

    epollOp = &epollOps[fd];
    hasEvent = epollOp->read || epollOp->write;

    if ((event & EPOLLIN) == EPOLLIN)
        epollOp->read = ioop;
    if ((event & EPOLLOUT) == EPOLLOUT)
        epollOp->write = ioop;

    if (epollOp->read)
        event |= EPOLLIN;
    if (epollOp->write)
        event |= EPOLLOUT;
    
    // this is here to initalize the whole ev structure and suppress warnings
    ev.data.u64 = 0;
    ev.events = event;
    ev.data.ptr = epollOp;
    
    // add our interest in the event
    nev = epoll_ctl(epollfd, hasEvent ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, fd, &ev);

    // If you add the same fd to an epoll_set twice, you
    // probably get EEXIST, but this is a harmless condition.
    if (nev < 0)
    {
        if (errno == EEXIST)
        {
            //Log_Trace("AddEvent: fd = %d exists", fd);
            nev = epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &ev);
        }

        if (nev < 0)
        {
            Log_Errno();
            return false;
        }
    }
    
    if (ioop)
        ioop->active = true;
    
    return true;
}

bool IOProcessor::Remove(IOOperation* ioop)
{
    int                 nev;
    struct epoll_event  ev;
    EpollOp*            epollOp;
    
    if (!ioop->active)
        return true;
        
    if (ioop->pending)
    {
        ioop->active = false;
        return true;
    }
    
    if (epollfd < 0)
    {
        Log_Trace("eventfd < 0");
        return false;
    }
    
    // this is here to initalize the whole ev structure and suppress warnings
    ev.data.u64 = 0;
    ev.events = EPOLLONESHOT;
    ev.data.ptr = ioop;

    epollOp = &epollOps[ioop->fd];
    if (ioop->type == IOOperation::TCP_READ || ioop->type == IOOperation::UDP_READ)
    {
        epollOp->read = NULL;
        if (epollOp->write)
            ev.events |= EPOLLOUT;
    }
    else
    {
        epollOp->write = NULL;
        if (epollOp->read)
            ev.events |= EPOLLIN;
    }

    if (epollOp->read || epollOp->write)
        nev = epoll_ctl(epollfd, EPOLL_CTL_MOD, ioop->fd, &ev);
    else
        nev = epoll_ctl(epollfd, EPOLL_CTL_DEL, ioop->fd, &ev /* ignored */);

    if (nev < 0)
    {
        Log_Errno();
        return false;
    }
    
    ioop->active = false;
    
    return true;
}

bool IOProcessor::Poll(int sleep)
{
    
    int                         i, nevents;
    int                         ret, currentev, newfd = -1;
    static struct epoll_event   events[MAX_EVENTS];
    static struct epoll_event   newev;
    IOOperation*                ioop;
    EpollOp*                    epollOp;
        
    nevents = epoll_wait(epollfd, events, SIZE(events), sleep);
    EventLoop::UpdateTime();
    
    if (nevents < 0 || terminated)
    {
        Log_Errno();
        return false;
    }
    
    for (i = 0; i < nevents; i++)
    {
        currentev = events[i].events;
        if (!(currentev & EPOLLIN) && !(currentev & EPOLLOUT))
            continue;       
        epollOp = (EpollOp*) events[i].data.ptr;
        
        newev.events = 0;
        newfd = -1;
        if (!(currentev & EPOLLIN) && epollOp->read)
        {
            newev.events |= EPOLLIN;
            newev.data.ptr = epollOp;
            newfd = epollOp->read->fd;
        }
        else if (!(currentev & EPOLLOUT) && epollOp->write)
        {
            newev.events |= EPOLLOUT;
            newev.data.ptr = epollOp;
            newfd = epollOp->write->fd;
        }
        
        if (newev.events)
        {
            if ((epollOp->read && epollOp->read->type != PIPEOP) ||
                (epollOp->write && epollOp->write->type != PIPEOP))
                    newev.events |= EPOLLONESHOT;

            ret = epoll_ctl(epollfd, EPOLL_CTL_MOD, newfd, &newev);
            if (ret < 0)
            {
                Log_Errno();
                return false;
            }
        }
    
        if ((currentev & EPOLLIN) && epollOp->read)
            epollOp->read->pending = true;
        if ((currentev & EPOLLOUT) && epollOp->write)
            epollOp->write->pending = true;
    }
    
    for (i = 0; i < nevents; i++)
    {
        currentev = events[i].events;
        if (!(currentev & EPOLLIN) && !(currentev & EPOLLOUT))
            continue;
        epollOp = (EpollOp*) events[i].data.ptr;
    
        if ((currentev & EPOLLIN) && epollOp->read)
        {
            ioop = epollOp->read;
            assert(ioop != NULL);
            ioop->pending = false;
            if (ioop->active && ioop->type == PIPEOP)
            {
                // we never set pipeOps' read to NULL, they're special
                PipeOp* pipeop = (PipeOp*) ioop;
                Call(pipeop->callback);
            }
            else if (ioop->active)
            {
                epollOp->read = NULL;
                ProcessIOOperation(ioop);
            }
        }

        if ((currentev & EPOLLOUT) && epollOp->write)
        {
            ioop = epollOp->write;
            epollOp->write = NULL;
            assert(ioop != NULL);
            ioop->pending = false;
            // we don't care about write notifications for pipeOps
            if (ioop->active)
                ProcessIOOperation(ioop);
        }       
    }
    
    return true;
}

bool IOProcessor::Complete(Callable* callable)
{
    Log_Trace();
    
    int nwrite;
    
    nwrite = write(asyncPipeOp.pipe[1], &callable, sizeof(Callable*));
    if (nwrite < 0)
    {
        Log_Errno();
        return false;
    }
    
    return true;
}

void ProcessIOOperation(IOOperation* ioop)
{
    ioop->active = false;
    
    if (!ioop)
        return;
    
    switch (ioop->type)
    {
    case IOOperation::TCP_READ:
        ProcessTCPRead((TCPRead*) ioop);
        break;
    case IOOperation::TCP_WRITE:
        ProcessTCPWrite((TCPWrite*) ioop);
        break;
    case IOOperation::UDP_READ:
        ProcessUDPRead((UDPRead*) ioop);
        break;
    case IOOperation::UDP_WRITE:
        ProcessUDPWrite((UDPWrite*) ioop);
        break;
    default:
        /* do nothing */
        break;
    }
}

#define MAX_CALLABLE 256    
void ProcessAsyncOp()
{
    Callable*   callables[MAX_CALLABLE];
    int         nread;
    int         count;
    int         i;
    
    Log_Trace();

    while (true)
    {
        nread = read(asyncPipeOp.pipe[0], callables, SIZE(callables));
        count = nread / sizeof(Callable*);
        
        for (i = 0; i < count; i++)
            Call(*callables[i]);
        
        if (count < (int) SIZE(callables))
            break;
    }
}

void ProcessTCPRead(TCPRead* tcpread)
{
    int readlen, nread;

    if (tcpread->listening)
    {
        Call(tcpread->onComplete);
        return;
    }
    
    if (tcpread->requested == IO_READ_ANY)
        readlen = tcpread->buffer->GetRemaining();
    else
        readlen = tcpread->requested - tcpread->buffer->GetLength();
    
    if (readlen <= 0)
        return;

    nread = read(tcpread->fd,
                 tcpread->buffer->GetPosition(),
                 readlen);
    
    if (nread < 0)
    {
        if (errno == EWOULDBLOCK || errno == EAGAIN)
        {
            IOProcessor::Add(tcpread);
        }
        else
        {
            Log_Errno();
            Call(tcpread->onClose);
        }
    }
    else if (nread == 0)
    {
        Call(tcpread->onClose);
    }
    else
    {
        tcpread->buffer->Lengthen(nread);
        if (tcpread->requested == IO_READ_ANY || 
            tcpread->buffer->GetLength() == (unsigned)tcpread->requested)
            Call(tcpread->onComplete);
        else
            IOProcessor::Add(tcpread);
    }
}

void ProcessTCPWrite(TCPWrite* tcpwrite)
{
    int writelen, nwrite;

    // this indicates check for connect() readyness
    if (tcpwrite->buffer == NULL)
    {
        sockaddr_in sa;
        socklen_t   socklen = sizeof(sa);
        
        nwrite = getpeername(tcpwrite->fd, (sockaddr*) &sa, &socklen);
        if (nwrite < 0)
        {
            Log_Errno();
            Call(tcpwrite->onClose);
        }
        else
            Call(tcpwrite->onComplete);
    
        return;
    }

    writelen = tcpwrite->buffer->GetLength() - tcpwrite->transferred;
    if (writelen <= 0)
    {
        ASSERT_FAIL();
    }
    
    nwrite = write(tcpwrite->fd,
                   tcpwrite->buffer->GetBuffer() + tcpwrite->transferred,
                   writelen);
                   
    if (nwrite < 0)
    {
        if (errno == EWOULDBLOCK || errno == EAGAIN)
        {
            IOProcessor::Add(tcpwrite);
        }
        else
        {
            Log_Errno();
            Call(tcpwrite->onClose);
        }
    } 
    else if (nwrite == 0)
    {
        Call(tcpwrite->onClose);
    }
    else
    {
        tcpwrite->transferred += nwrite;
        if (tcpwrite->transferred == tcpwrite->buffer->GetLength())
            Call(tcpwrite->onComplete);
        else
            IOProcessor::Add(tcpwrite);
    }
}

void ProcessUDPRead(UDPRead* udpread)
{
    int         nread;
    socklen_t   salen = ENDPOINT_SOCKADDR_SIZE;
    
    do
    {
        nread = recvfrom(udpread->fd,
                         udpread->buffer->GetBuffer(),
                         udpread->buffer->GetSize(),
                         0,
                         (sockaddr*) udpread->endpoint.GetSockAddr(),
                         &salen);
    
        if (nread < 0)
        {
            if (errno == EWOULDBLOCK || errno == EAGAIN)
            {
                IOProcessor::Add(udpread); // try again
            }
            else
            {
                Log_Errno();
                Call(udpread->onClose);
            }
        } 
        else if (nread == 0)
        {
            Call(udpread->onClose);
        }
        else
        {
            udpread->buffer->SetLength(nread);
            Call(udpread->onComplete);
        }
    } while (nread > 0);
}

void ProcessUDPWrite(UDPWrite* udpwrite)
{
    int         nwrite;
    socklen_t   salen = ENDPOINT_SOCKADDR_SIZE;

    nwrite = sendto(udpwrite->fd,
                    udpwrite->buffer->GetBuffer() + udpwrite->offset,
                    udpwrite->buffer->GetLength() - udpwrite->offset,
                    0,
                    (const sockaddr*) udpwrite->endpoint.GetSockAddr(),
                    salen);

    if (nwrite < 0)
    {
        if (errno == EWOULDBLOCK || errno == EAGAIN)
        {
            IOProcessor::Add(udpwrite); // try again
        }
        else
        {
            Log_Errno();
            Call(udpwrite->onClose);
        }
    }
    else if (nwrite == 0)
    {
        Call(udpwrite->onClose);
    }
    else
    {
        if (nwrite == (int)udpwrite->buffer->GetLength() - udpwrite->offset)
        {
            Call(udpwrite->onComplete);
        }
        else
        {
            udpwrite->offset += nwrite;
            Log_Trace("sendto() datagram fragmentation");
            IOProcessor::Add(udpwrite); // try again
        }
    }
}

#endif // PLATFORM_LINUX
