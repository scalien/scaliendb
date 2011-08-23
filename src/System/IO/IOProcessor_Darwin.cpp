#ifdef PLATFORM_DARWIN

#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <math.h>
#include <errno.h>
#include <signal.h>

#include "IOProcessor.h"
#include "System/Containers/List.h"
#include "System/Common.h"
#include "System/Log.h"
#include "System/Time.h"
#include "System/Events/EventLoop.h"
#include "System/Threading/Mutex.h"

// see http://wiki.netbsd.se/index.php/kqueue_tutorial

#define MAX_KEVENTS     1024

#ifdef IOPROCESSOR_MULTITHREADED

#define UNLOCKED_CALL(callable)     \
do                                  \
{                                   \
    mutex.Unlock();                 \
    Call(callable);                 \
    mutex.Lock();                   \
} while (0)

#define UNLOCKED_ADD(ioop)          \
do                                  \
{                                   \
    mutex.Unlock();                 \
    IOProcessor::Add(ioop);         \
    mutex.Lock();                   \
} while (0)

#else // IOPROCESSOR_MULTITHREADED

#define UNLOCKED_CALL(callable)     \
    Call(callable)

#define UNLOCKED_ADD(ioop)          \
    IOProcessor::Add(ioop)

#endif // IOPROCESSOR_MULTITHREADED

static int              kq = 0;         // the kqueue
static int              asyncOpPipe[2];
static bool*            readOps;
static bool*            writeOps;
static int              numOps;
static volatile bool    terminated;
static volatile int     numClient = 0;
static IOProcessorStat  iostat;
static Mutex            mutex;

static bool AddKq(int ident, short filter, IOOperation* ioop);
static void ProcessAsyncOp();
static void ProcessTCPRead(struct kevent* ev);
static void ProcessTCPWrite(struct kevent* ev);
static void ProcessUDPRead(struct kevent* ev);
static void ProcessUDPWrite(struct kevent* ev);

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
        
        AddKq(SIGINT, EVFILT_SIGNAL, NULL);
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
    rlimit      rl;
    int         i;

    numClient++;
    
    if (kq != 0)
        return true;

    terminated = false;
    
    kq = kqueue();
    if (kq < 0)
    {
        Log_Errno();
        return false;
    }

    rl.rlim_cur = maxfd_;
    rl.rlim_max = maxfd_;
    if (setrlimit(RLIMIT_NOFILE, &rl) < 0)
    {
        Log_Errno();
    }
        
    // setup async pipe
    if (pipe(asyncOpPipe) < 0)
    {
        Log_Errno();
        return false;
    }
    
    fcntl(asyncOpPipe[0], F_SETFD, FD_CLOEXEC);
    fcntl(asyncOpPipe[0], F_SETFD, O_NONBLOCK);
    fcntl(asyncOpPipe[1], F_SETFD, FD_CLOEXEC);

    if (!AddKq(asyncOpPipe[0], EVFILT_READ, NULL))
        return false;
    
    numOps = maxfd_;
    readOps = new bool[numOps];
    writeOps = new bool[numOps];
    for (i = 0; i < numOps; i++)
    {
        readOps[i] = false;
        writeOps[i] = false;
    }
    
    memset(&iostat, 0, sizeof(iostat));
    
    return true;
}

void IOProcessor::Shutdown()
{
    numClient--;
    
    if (kq == 0 || numClient > 0)
        return;

    close(kq);
    kq = 0;
    close(asyncOpPipe[0]);
    close(asyncOpPipe[1]);
    delete[] readOps;
    delete[] writeOps;
}

bool IOProcessor::Add(IOOperation* ioop)
{
    short   filter;
    
    if (ioop->active)
        return true;
    
    if (ioop->pending)
        return true;
    
#ifdef IOPROCESSOR_MULTITHREADED
    MutexGuard guard(mutex);
#endif
    
    if (ioop->type == IOOperation::TCP_READ
     || ioop->type == IOOperation::UDP_READ)
    {
        readOps[ioop->fd] = true;
        filter = EVFILT_READ;
    }
    else
    {
        writeOps[ioop->fd] = true;
        filter = EVFILT_WRITE;
    }
    
    return AddKq(ioop->fd, filter, ioop);
}

bool AddKq(int ident, short filter, IOOperation* ioop)
{
    int             nev;
    struct kevent   ev;
    struct timespec timeout = { 0, 0 };
    
    if (kq < 0)
    {
        Log_Trace("kq < 0");
        return false;
    }
    
    EV_SET(&ev, ident, filter, EV_ADD | EV_ONESHOT, 0, 0, ioop);
    
    // add our interest in the event
    nev = kevent(kq, &ev, 1, NULL, 0, &timeout);
    if (nev < 0)
    {
        Log_Errno();
        return false;
    }
    
    if (ioop)
        ioop->active = true;
    
    return true;
}

bool IOProcessor::Remove(IOOperation* ioop)
{
    short           filter;
    int             nev;
    struct kevent   ev;
    struct timespec timeout = { 0, 0 };

    if (!ioop->active)
        return true;

#ifdef IOPROCESSOR_MULTITHREADED
    MutexGuard guard(mutex);
#endif

    if (ioop->pending)
    {
        if (ioop->type == IOOperation::TCP_READ || ioop->type == IOOperation::UDP_READ)
            readOps[ioop->fd] = false;
        else
            writeOps[ioop->fd] = false;

        ioop->active = false;
        ioop->pending = false;
        return true;
    }
    
    if (kq < 0)
    {
        Log_Trace("kq < 0");
        return false;
    }

    if (ioop->type == IOOperation::TCP_READ
     || ioop->type == IOOperation::UDP_READ)
    {
        readOps[ioop->fd] = false;
        filter = EVFILT_READ;
    }
    else
    {
        writeOps[ioop->fd] = false;
        filter = EVFILT_WRITE;
    }
    
    EV_SET(&ev, ioop->fd, filter, EV_DELETE, 0, 0, 0);
    
    // delete event
    nev = kevent(kq, &ev, 1, NULL, 0, &timeout);
    if (nev < 0)
    {
        Log_Errno();
        // HACK:
        ioop->active = false;
        return false;
    }
    
    ioop->active = false;
    
    return true;
}

bool IOProcessor::Poll(int sleep)
{
    int                     i, nevents;
    static struct kevent    events[MAX_KEVENTS];
    struct timespec         timeout;
    IOOperation*            ioop;
    uint64_t                startTime;
    
    iostat.numPolls++;
    
    timeout.tv_sec = (time_t) floor(sleep / 1000.0);
    timeout.tv_nsec = (sleep - 1000 * timeout.tv_sec) * 1000000;
    
    startTime = EventLoop::Now();
    nevents = kevent(kq, NULL, 0, events, SIZE(events), &timeout);
    EventLoop::UpdateTime();
    iostat.lastPollTime = EventLoop::Now();
    iostat.totalPollTime += iostat.lastPollTime - startTime;

    if (nevents < 0 || terminated)
    {
        Log_Errno();
        Log_Trace("terminated = %s", terminated ? "true" : "false");
        if (terminated)
            return false;
        else
            return true;
    }
    
#ifdef IOPROCESSOR_MULTITHREADED
    MutexGuard guard(mutex);
#endif
        
    iostat.lastNumEvents = (unsigned) nevents;
    iostat.totalNumEvents += nevents;
    for (i = 0; i < nevents; i++)
    {
        if (events[i].flags & EV_ERROR)
            Log_Trace("%s", strerror(events[i].data));
        if (events[i].udata == NULL)
            continue;
        ioop = (IOOperation*) events[i].udata;
        ioop->pending = true;
    }
    
    for (i = 0; i < nevents; i++)
    {
        if (events[i].filter == EVFILT_SIGNAL)
        {
            Log_Trace("SIGNAL caught, %d", events[i].ident);
            return false;
        }

        if (events[i].flags & EV_ERROR)
            continue;
        
        if (events[i].udata == NULL)
        {
            ProcessAsyncOp();
            // re-register for notification
            if (!AddKq(asyncOpPipe[0], EVFILT_READ, NULL))
                return false;
            continue;
        }

        // ioop was removed and possibly deleted, just skip it
        if (readOps[events[i].ident] == false && writeOps[events[i].ident] == false)
            continue;

        ioop = (IOOperation*) events[i].udata;
        ioop->pending = false;
        if (!ioop->active)
            continue; // ioop was removed, just skip it         
        ioop->active = false;
        
        if (ioop->type == IOOperation::TCP_READ && readOps[events[i].ident]
         && (events[i].filter & EVFILT_READ))
        {
            readOps[events[i].ident] = false;
            ProcessTCPRead(&events[i]);
        }
        else if (ioop->type == IOOperation::TCP_WRITE && writeOps[events[i].ident]
         && (events[i].filter & EVFILT_WRITE))
        {
            writeOps[events[i].ident] = false;
            ProcessTCPWrite(&events[i]);
        }
        else if (ioop->type == IOOperation::UDP_READ && readOps[events[i].ident]
         && (events[i].filter & EVFILT_READ))
        {
            readOps[events[i].ident] = false;
            ProcessUDPRead(&events[i]);
        }
        else if (ioop->type == IOOperation::UDP_WRITE && writeOps[events[i].ident]
         && (events[i].filter & EVFILT_WRITE))
        {
            writeOps[events[i].ident] = false;
            ProcessUDPWrite(&events[i]);
        }
    }
    
    return true;
}

bool IOProcessor::Complete(Callable* callable)
{
    Log_Trace();

    int nwrite;
    
    iostat.numCompletions++;
    
    nwrite = write(asyncOpPipe[1], callable, sizeof(Callable));
    if (nwrite < 0)
        Log_Errno();
    
    if (nwrite >= 0)
        return true;
    
    return false;
}

void ProcessAsyncOp()
{
    Log_Trace();
    
    static Callable callables[256];
    int nread;
    int count;
    int i;

    while (1)
    {
        nread = read(asyncOpPipe[0], callables, SIZE(callables));
        count = nread / sizeof(Callable);
        
        // TODO: optimization: unlock before for-loop and lock after it only once
        for (i = 0; i < count; i++)
        {
            UNLOCKED_CALL(callables[i]);
        }
        
        if (count < (int) SIZE(callables))
            break;
    }
}

void ProcessTCPRead(struct kevent* ev)
{
    int         readlen, nread;
    TCPRead*    tcpread;
    
    Log_Trace();
    
    iostat.numTCPReads++;
    tcpread = (TCPRead*) ev->udata;

    if (tcpread->listening)
    {
        UNLOCKED_CALL(tcpread->onComplete);
    }
    else
    {
        if (tcpread->requested == IO_READ_ANY)
            readlen = tcpread->buffer->GetRemaining();
        else
            readlen = tcpread->requested - tcpread->buffer->GetLength();
        readlen = MIN(ev->data, readlen);
        if (readlen > 0)
        {
            nread = read(tcpread->fd,
                         tcpread->buffer->GetPosition(),
                         readlen);
            
            if (nread < 0)
            {
                if (errno == EWOULDBLOCK || errno == EAGAIN)
                    UNLOCKED_ADD(tcpread);
                else if (!(ev->flags & EV_EOF))
                    Log_Errno();
            }
            else
            {
                iostat.numTCPBytesReceived += nread;
                tcpread->buffer->Lengthen(nread);
                if (tcpread->requested == IO_READ_ANY ||
                (int) tcpread->buffer->GetLength() == tcpread->requested)
                    UNLOCKED_CALL(tcpread->onComplete);
                else
                    UNLOCKED_ADD(tcpread);
            }
        }
        else if (ev->flags & EV_EOF)
            UNLOCKED_CALL(tcpread->onClose);
    }
}

void ProcessTCPWrite(struct kevent* ev)
{
    int         writelen, nwrite;
    TCPWrite*   tcpwrite;

    Log_Trace();

    iostat.numTCPWrites++;
    tcpwrite = (TCPWrite*) ev->udata;
    
    if (ev->flags & EV_EOF)
    {
        UNLOCKED_CALL(tcpwrite->onClose);
        return;
    }
    
    if (tcpwrite->buffer == NULL)
    {
        if (ev->flags & EV_EOF)
            UNLOCKED_CALL(tcpwrite->onClose);
        else
            UNLOCKED_CALL(tcpwrite->onComplete);

        return;
    }

    writelen = tcpwrite->buffer->GetLength() - tcpwrite->transferred;
    writelen = MIN(ev->data, writelen);
    
    if (writelen > 0)
    {
        nwrite = write(tcpwrite->fd,
                       tcpwrite->buffer->GetBuffer() + tcpwrite->transferred,
                       writelen);
        
        if (nwrite < 0)
        {
            if (errno == EWOULDBLOCK || errno == EAGAIN)
                UNLOCKED_ADD(tcpwrite);
            else if (!(ev->flags & EV_EOF))
                Log_Errno();
        }
        else
        {
            iostat.numTCPBytesSent += nwrite;
            tcpwrite->transferred += nwrite;
            if (tcpwrite->transferred == tcpwrite->buffer->GetLength())
                UNLOCKED_CALL(tcpwrite->onComplete);
            else
                UNLOCKED_ADD(tcpwrite);
        }
    }
}

void ProcessUDPRead(struct kevent* ev)
{
    int         salen, nread;
    UDPRead*    udpread;

    iostat.numUDPReads++;
    udpread = (UDPRead*) ev->udata;
    
    salen = ENDPOINT_SOCKADDR_SIZE;
    nread = recvfrom(udpread->fd,
                     udpread->buffer->GetBuffer(),
                     udpread->buffer->GetSize(),
                     0,
                     (sockaddr*) udpread->endpoint.GetSockAddr(),
                     (socklen_t*)&salen);
    
    if (nread < 0)
    {
        if (errno == EWOULDBLOCK || errno == EAGAIN)
            UNLOCKED_ADD(udpread); // try again
        else
            Log_Errno();
    }
    else
    {
        iostat.numUDPBytesReceived += nread;
        udpread->buffer->SetLength(nread);
        UNLOCKED_CALL(udpread->onComplete);
    }
                    
    if (ev->flags & EV_EOF)
        UNLOCKED_CALL(udpread->onClose);
}

void ProcessUDPWrite(struct kevent* ev)
{
    int         nwrite;
    UDPWrite*   udpwrite;

    iostat.numUDPWrites++;
    udpwrite = (UDPWrite*) ev->udata;

    if (ev->data >= (int) udpwrite->buffer->GetLength())
    {
        nwrite = sendto(udpwrite->fd,
                        udpwrite->buffer->GetBuffer(),
                        udpwrite->buffer->GetLength(),
                        0,
                        (const sockaddr*) udpwrite->endpoint.GetSockAddr(),
                        ENDPOINT_SOCKADDR_SIZE);
        
        if (nwrite < 0)
        {
            if (errno == EWOULDBLOCK || errno == EAGAIN)
                UNLOCKED_ADD(udpwrite); // try again
            else
                Log_Errno();
        }
        else
        {
            iostat.numUDPBytesSent += nwrite;
            if (nwrite == (int) udpwrite->buffer->GetLength())
            {
                UNLOCKED_CALL(udpwrite->onComplete);
            }
            else
            {
                Log_Trace("sendto() datagram fragmentation");
                UNLOCKED_ADD(udpwrite); // try again
            }
        }
    }
    
    if (ev->flags & EV_EOF)
        UNLOCKED_CALL(udpwrite->onClose);
}

void IOProcessor::GetStats(IOProcessorStat* stat_)
{
    *stat_ = iostat;
}

#endif // PLATFORM_DARWIN
