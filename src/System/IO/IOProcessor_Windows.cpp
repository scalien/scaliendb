#ifdef _WIN32

#include "IOProcessor.h"
#include "System/Containers/InQueue.h"
#include "System/Events/EventLoop.h"
#include "System/Threading/Mutex.h"
#include "System/Platform.h"
#include "System/Common.h"
#include "System/Log.h"

#include <winsock2.h>
#include <mswsock.h>

#define IN_PENDING_ONCLOSE(ioop)    ((ioop)->next != (ioop) && (ioop)->active == false)

// support for multithreaded IOProcessor
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

#define MUTEX_GUARD_DECLARE()       MutexGuard guard(mutex)
#define MUTEX_GUARD_LOCK()          guard.Lock()
#define MUTEX_GUARD_UNLOCK()        guard.Unlock()

#else // IOPROCESSOR_MULTITHREADED

#define UNLOCKED_CALL(callable)     \
    CallWarnTimeout(callable)

#define UNLOCKED_ADD(ioop)          \
    IOProcessor::Add(ioop)

#define MUTEX_GUARD_DECLARE()
#define MUTEX_GUARD_LOCK()
#define MUTEX_GUARD_UNLOCK()

#endif // IOPROCESSOR_MULTITHREADED


// the address buffer passed to AcceptEx() must be 16 more than the max address length (see MSDN)
// http://msdn.microsoft.com/en-us/library/ms737524(VS.85).aspx
#define ACCEPT_ADDR_LEN     (sizeof(sockaddr_in) + 16)
#define MAX_TCP_READ        8192


// this structure is used for storing Windows/IOCP specific data
struct IODesc
{
    IOOperation*    read;
    IOOperation*    write;

    OVERLAPPED      ovlRead;            // for accept/recv
    OVERLAPPED      ovlWrite;           // for connect/send

    FD              acceptFd;           // with a listening socket, the accepted FD is stored here
    byte            acceptData[2 * ACCEPT_ADDR_LEN];    // TODO allocate dynamically

    IODesc*         next;               // pointer for free list handling
};

// this structure is used for storing completion callables
struct CallableItem
{
    Callable        callable;
    CallableItem*   next;
};

typedef InQueue<CallableItem> CallableQueue;
typedef InList<IOOperation> OpList;

// globals
static HANDLE           iocp = NULL;        // global completion port handle
static IODesc*          iods;               // pointer to allocated array of IODesc's
static IODesc*          freeIods;           // pointer to the free list of IODesc's
static IODesc           callback;           // special IODesc for handling IOProcessor::Complete events
const FD                INVALID_FD = {-1, INVALID_SOCKET};  // special FD to indicate invalid value
unsigned                SEND_BUFFER_SIZE = 8001;
static volatile bool    terminated = false;
static unsigned         numIOProcClients = 0;
static IOProcessorStat  iostat;
static Mutex            callableMutex;
static CallableQueue    callableQueue;
static OpList           pendingOps;
static OpList           pendingOnClose;
static Mutex            mutex;
static int				numIods;

static LPFN_CONNECTEX   ConnectEx;

bool ProcessTCPRead(TCPRead* tcpread);
bool ProcessTCPWrite(TCPWrite* tcpwrite);
void ProcessCompletionCallbacks();

static void CallWarnTimeout(Callable& callable)
{
    uint64_t    start;
    uint64_t    elapsed;

    start = NowClock();
    Call(callable);
    elapsed = NowClock() - start;
    if (elapsed > 100)
        Log_Debug("IOProcessor callback elapsed time: %U", elapsed);
}

// helper function for FD -> IODesc mapping
static IODesc* GetIODesc(const FD& fd)
{
    ASSERT(fd.index >= 0 && fd.index < numIods);
    return &iods[fd.index];
}

// return the asynchronously accepted FD
bool IOProcessorAccept(const FD& listeningFd, FD& fd)
{
    IODesc*     iod;
	int			ret;

    iod = GetIODesc(listeningFd);
    fd = iod->acceptFd;

    // this need to be called so that getpeername works
    ret = setsockopt(fd.handle, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, (const char *)&listeningFd.handle, sizeof(SOCKET));
	if (ret < 0)
		return false;

    return true;
}

// asynchronous connect
bool IOProcessorConnect(FD& fd, Endpoint& endpoint)
{
    sockaddr_in localAddr;
    IODesc*     iod;

    //Log_Debug("IOProcessorConnect: fd = %d, endpoint = %s", fd.index, endpoint.ToString());

    memset(&localAddr, 0, sizeof(sockaddr_in));
    localAddr.sin_family = AF_INET;
    localAddr.sin_addr.S_un.S_addr = INADDR_ANY;

    // the socket must be bound so that ConnectEx works
    if (bind(fd.handle, (sockaddr*) &localAddr,  sizeof(sockaddr_in)) == SOCKET_ERROR)
    {
        // WSAEINVAL means it is already bound
        if (WSAGetLastError() != WSAEINVAL)
        {
            Log_Debug("Bind failed");
            return false;
        }
    }

    iod = GetIODesc(fd);
    memset(&iod->ovlWrite, 0, sizeof(OVERLAPPED));
    if (!ConnectEx(fd.handle, (sockaddr*) endpoint.GetSockAddr(), ENDPOINT_SOCKADDR_SIZE, NULL, 0, NULL, &iod->ovlWrite))
    {
        if (WSAGetLastError() != WSA_IO_PENDING)
        {
            Log_Debug("ConnectEx failed");
            return false;
        }
    }

    //Log_Debug("IOProcessorConnect: fd = %d, endpoint = %s finished", fd.index, endpoint.ToString());

    return true;
}

// register FD with an IODesc, also register in completion port
bool IOProcessorRegisterSocket(FD& fd)
{
    IODesc*     iod;

#ifdef IOPROCESSOR_MULTITHREADED
    MutexGuard  guard(mutex);
#endif

    if (freeIods == NULL)
        return false;

    iod = freeIods;
    // unlink iod from free list
    freeIods = iod->next;
    iod->next = NULL;
    iod->read = NULL;
    iod->write = NULL;

    fd.index = iod - iods;

    CreateIoCompletionPort((HANDLE)fd.handle, iocp, (ULONG_PTR)iod, 0);

    Log_Trace("fd = %d", fd.index);
        
    return true;
}

// put back the IODesc to the free list and cancel all IO and put back FD
bool IOProcessorUnregisterSocket(FD& fd)
{
    IODesc*     iod;
    BOOL        ret;

    Log_Trace("fd = %d", fd.index);

#ifdef IOPROCESSOR_MULTITHREADED
    MutexGuard  guard(mutex);
#endif

    iod = &iods[fd.index];
    iod->next = freeIods;
    freeIods = iod;

    ret = CancelIo((HANDLE)fd.handle);
    if (ret == 0)
    {
        Log_Errno("IOProcessorUnregisterSocket: CancelIo");
        return false;
    }

    return true;
}

BOOL WINAPI ConsoleCtrlHandler(DWORD /*ctrlType*/)
{
    Callable    emptyCallable;

    if (terminated)
    {
        STOP_FAIL(0, "aborting due to user request");
        return FALSE;
    }
        
    terminated = true;

    // the point is to wake up the main thread that is waiting for a completion event
    IOProcessor::Complete(&emptyCallable);
    return TRUE;
}

void IOProcessor::BlockSignals(int blockMode)
{
}

bool IOProcessor::Init(int maxfd)
{
    WSADATA     wsaData;
    SOCKET      s;
    GUID        guid = WSAID_CONNECTEX;
    DWORD       bytesReturned;

    terminated = false;
    numIOProcClients++;

    if (iocp)
        return true;

    // initialize a Console Control Handler routine
    SetConsoleCtrlHandler(ConsoleCtrlHandler, TRUE);

    // initialize Winsock2 library
    if (WSAStartup(MAKEWORD(2, 2), &wsaData))
        return false;

    // create a global completion port object
    iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, NULL, 0);

    // create a dummy socket to pass to WSAIoctl()
    s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (!s)
        return false;

    // get ConnectEx function pointer
    if (WSAIoctl(s, SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid, sizeof(GUID), &ConnectEx, sizeof(LPFN_CONNECTEX),
        &bytesReturned, NULL, NULL) == SOCKET_ERROR)
    {
        return false;
    }
    closesocket(s);

    // create an array for IODesc indexing
	numIods = maxfd;
    iods = new IODesc[numIods];
    for (int i = 0; i < numIods - 1; i++)
        iods[i].next = &iods[i + 1];

    iods[numIods - 1].next = NULL;
    freeIods = iods;

    memset(&iostat, 0, sizeof(iostat));

    return true;
}

void IOProcessor::Shutdown()
{
    numIOProcClients--;

    if (iocp == NULL || numIOProcClients > 0)
        return;

    delete[] iods;
    freeIods = NULL;
    CloseHandle(iocp);
    iocp = NULL;
    SetConsoleCtrlHandler(ConsoleCtrlHandler, FALSE);
    WSACleanup();
}

static bool RequestReadNotification(IOOperation* ioop)
{
    DWORD   numBytes, flags;
    WSABUF  wsabuf;
    IODesc* iod;
    int     ret;

//  Log_Trace("fd.index = %d", ioop->fd.index);

    iod = GetIODesc(ioop->fd);

    ASSERT(iod->read == NULL);

    wsabuf.buf = NULL;
    wsabuf.len = 0;

    flags = 0;
    memset(&iod->ovlRead, 0, sizeof(OVERLAPPED));

    ret = WSARecv(ioop->fd.handle, &wsabuf, 1, &numBytes, &flags, &iod->ovlRead, NULL);
    if (ret == SOCKET_ERROR)
    {
        ret = WSAGetLastError();
        if (ret != WSA_IO_PENDING)
        {
            Log_Debug("RequestReadNotification ret = %d", ret);
            //Call(ioop->onClose);
            pendingOnClose.Append(ioop);
            return false;
        }
    }

    iod->read = ioop;
    ioop->active = true;

    return true;
}

static bool RequestWriteNotification(IOOperation* ioop)
{
    DWORD   numBytes;
    WSABUF  wsabuf;
    IODesc* iod;
    int     ret;

//  Log_Trace("fd.index = %d", ioop->fd.index);

    iod = GetIODesc(ioop->fd);

    ASSERT(iod->write == NULL);

    wsabuf.buf = NULL;
    wsabuf.len = 0;

    memset(&iod->ovlWrite, 0, sizeof(OVERLAPPED));
    ret = WSASend(ioop->fd.handle, &wsabuf, 1, &numBytes, 0, &iod->ovlWrite, NULL);
    if (ret == SOCKET_ERROR)
    {
        ret = WSAGetLastError();
        if (ret != WSA_IO_PENDING)
        {
            Log_Debug("RequestWriteNotification ret = %d", ret);
            //Call(ioop->onClose);
            pendingOnClose.Append(ioop);
            return false;
        }
    }

    iod->write = ioop;
    ioop->active = true;

    return true;
}

static bool StartAsyncAccept(IOOperation* ioop)
{
    DWORD   numBytes;
    BOOL    trueval = TRUE;
    BOOL    acceptRet;
    IODesc* iod;
    int     ret;

    iod = GetIODesc(ioop->fd);

    if (iod->read || iod->write)
        ASSERT_FAIL();

    // create an accepting socket with WSA_FLAG_OVERLAPPED to support async operations
    iod->acceptFd.handle = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (iod->acceptFd.handle == INVALID_SOCKET)
	{
        Log_Debug("iod->acceptFd.handle == INVALID_SOCKET");
        //Call(ioop->onClose);
        pendingOnClose.Append(ioop);
        return false;
	}

    ret = setsockopt(iod->acceptFd.handle, SOL_SOCKET, SO_REUSEADDR, (char *)&trueval, sizeof(BOOL));
    if (ret == SOCKET_ERROR)
    {
        closesocket(iod->acceptFd.handle);
        Log_Debug("StartAsyncAccept 1 ret = %d", ret);
        //Call(ioop->onClose);
        pendingOnClose.Append(ioop);
        return false;
    }

    memset(&iod->ovlRead, 0, sizeof(OVERLAPPED));
    acceptRet = AcceptEx(ioop->fd.handle, iod->acceptFd.handle, iod->acceptData, 0, ACCEPT_ADDR_LEN, ACCEPT_ADDR_LEN, &numBytes, &iod->ovlRead);
    if (acceptRet == FALSE) 
    {
        ret = WSAGetLastError();
        if (ret != WSA_IO_PENDING)
        {
            Log_Errno();
            closesocket(iod->acceptFd.handle);
            Log_Debug("StartAsyncAccept 2 ret = %d", ret);
            //Call(ioop->onClose);
            pendingOnClose.Append(ioop);
            return false;
        }
    }
    else
    {
        Log_Debug("AcceptEx returned TRUE");
    }

    iod->read = ioop;
    ioop->active = true;

    return true;
}

bool StartAsyncConnect(IOOperation* ioop)
{
    IODesc* iod;

    iod = GetIODesc(ioop->fd);
    iod->write = ioop;
    ioop->active = true;
    
    return true;
}

bool IOProcessor_UnprotectedAdd(IOOperation* ioop)
{
    if (ioop->active)
    {
        ASSERT_FAIL();
//      return true;
    }

    if (ioop->next != ioop)
        return true;

    // listening flag indicates that we are waiting for accept event
    if (ioop->type == IOOperation::TCP_READ && ((TCPRead*) ioop)->listening)
        return StartAsyncAccept(ioop);

    // NULL buffer indicates that we are waiting for connect event
    if (ioop->type == IOOperation::TCP_WRITE && ioop->buffer == NULL)
        return StartAsyncConnect(ioop);

    if (ioop->type == IOOperation::TCP_READ)
        return RequestReadNotification(ioop);
    else if (ioop->type == IOOperation::TCP_WRITE)
        return RequestWriteNotification(ioop);
    else
        ASSERT_FAIL();

    return false;
}


bool IOProcessor::Add(IOOperation* ioop)
{
#ifdef IOPROCESSOR_MULTITHREADED
    MutexGuard  guard(mutex);
#endif
	return IOProcessor_UnprotectedAdd(ioop);
}

bool IOProcessor_UnprotectedRemove(IOOperation *ioop)
{
    int         ret;
    IODesc*     iod;

    // if the ioop is in the pendingOnClose list, then remove it
    // this is imoortant if a read and a write both receive OnClose events
    // the read's runs, while the write's is in the pendingOnClose list
    // the read's calls IOProcessor::Remove(tcpwrite) so here
    // we remove itfrom the pendingOnClose list
    // otherwise OnClose would be called again and it would lead to logical bugs
    // see #209
    if (IN_PENDING_ONCLOSE(ioop))
    {
        pendingOnClose.Remove(ioop);
        return true;
    }
    if (!ioop->active)
        return true;

    ioop->active = false;
    if (ioop->next != ioop)
    {
        // it's in pendingOps
        pendingOps.Remove(ioop);
        return true;
    }

    //if (ioop->type == IOOperation::TCP_READ)
    //{
    //    tcpread = (TCPRead*) ioop;
    //    if (tcpread->listening)
    //        ASSERT_FAIL(); // Remove() not supported for listening sockets
    //}

    iod = GetIODesc(ioop->fd);

    ret = CancelIo((HANDLE)ioop->fd.handle);
    if (ret == 0)
    {
        Log_Errno();
        return false;
    }

    if (ioop->type == IOOperation::TCP_READ)
    {
        iod->read = NULL;
        if (iod->write != NULL)
        {
            ASSERT(iod->write->active);
            ioop = iod->write;
            iod->write = NULL;
            ioop->active = false;
            IOProcessor_UnprotectedAdd(ioop);
        }
    }
    else
    {
        iod->write = NULL;
        if (iod->read != NULL)
        {
            ASSERT(iod->read->active);
            ioop = iod->read;
            iod->read = NULL;
            ioop->active = false;
            IOProcessor_UnprotectedAdd(ioop);
        }
    }

    return true;
}

bool IOProcessor::Remove(IOOperation* ioop)
{
#ifdef IOPROCESSOR_MULTITHREADED
		MutexGuard  guard(mutex);
#endif

	return IOProcessor_UnprotectedRemove(ioop);
}

void AddToPendingOps(IOOperation* ioop)
{
    if (ioop->priority)
        pendingOps.Prepend(ioop);
    else
        pendingOps.Append(ioop);
}

bool IOProcessor::Poll(int msec)
{
    DWORD           numBytes;
    DWORD           timeout;
    LPOVERLAPPED    overlapped;
    BOOL            ret;
    BOOL            result;
    DWORD           error;
    DWORD           flags;
    IODesc*         iod;
    IOOperation*    ioop;
    uint64_t        startTime;
    uint64_t        elapsed;

    iostat.numPolls++;

    if (pendingOps.GetLength() > 0)
        timeout = 0;
    else if (msec >= 0)
        timeout = msec;
    else
        timeout = INFINITE;

    //Log_Debug("IOProcessor::Poll timeout = %u", timeout);

#ifdef IOPROCESSOR_MULTITHREADED
            MutexGuard  guard(mutex);
#endif

    startTime = NowClock();
    FOREACH_FIRST (ioop, pendingOnClose)
    {
        pendingOnClose.Remove(ioop);
        UNLOCKED_CALL(ioop->onClose);
        elapsed = NowClock() - startTime;
        if (elapsed > YIELD_TIME)
            break;
    }

    while (true)
    {
        startTime = EventLoop::Now();
        
        // make sure we don't hold the lock while waiting for completion events
        MUTEX_GUARD_UNLOCK();
        ret = GetQueuedCompletionStatus(iocp, &numBytes, (PULONG_PTR)&iod, &overlapped, timeout);
        MUTEX_GUARD_LOCK();

        // this happens when CTRL-C is pressed or the console window is closed
        if (terminated)
            return false;

        EventLoop::UpdateTime();
        iostat.lastPollTime = EventLoop::Now();
        iostat.totalPollTime += iostat.lastPollTime - startTime;
        iostat.lastNumEvents = 1;
        iostat.totalNumEvents++;

        ioop = NULL;
        flags = 0;
        // ret == TRUE: a completion packet for a successful I/O operation was dequeued
        // ret == FALSE && overlapped != NULL: a completion packet for a failed I/O operation was dequeued
        if (ret || overlapped)
        {
            if (iod == &callback)
            {
                ProcessCompletionCallbacks();
            }
            // sometimes we get this after closesocket, so check first
            // if iod is in the freelist
            // TODO: clarify which circumstances lead to this issue
            else if (iod->next == NULL && (iod->read || iod->write))
            {
                if (overlapped == &iod->ovlRead && iod->read)
                {
                    ioop = iod->read;
                    ASSERT(ioop->active);
                    iod->read = NULL;

                    result = WSAGetOverlappedResult(ioop->fd.handle, &iod->ovlRead, &numBytes, FALSE, &flags);
                    
                    if (result)
                        AddToPendingOps(ioop);
                    else
                    {
                        error = GetLastError();
                        if (error != WSA_IO_INCOMPLETE)
                        {
						    // special case for listening sockets
						    if (ioop->type == IOOperation::TCP_READ && ((TCPRead*)ioop)->listening == true)
						    {
							    // the error happened on the accepted socket and async accept failed so we close it
							    closesocket(iod->acceptFd.handle);
							    iod->acceptFd.handle = INVALID_SOCKET;
							    Log_Debug("Async accept failed");
						    }

                            Log_Debug("Read last error = %d", error);
                            ioop->active = false;
                            //UNLOCKED_CALL(ioop->onClose);
                            pendingOnClose.Append(ioop);
                        }
                        else
                        {
                            Log_Debug("error == WSA_IO_INCOMPLETE");
                        }
                    }
                }
                else if (overlapped == &iod->ovlWrite && iod->write)
                {
                    ioop = iod->write;
                    ASSERT(ioop->active);
                    iod->write = NULL;

                    result = WSAGetOverlappedResult(ioop->fd.handle, &iod->ovlWrite, &numBytes, FALSE, &flags);

                    if (result && ioop->type == IOOperation::TCP_WRITE)
                        AddToPendingOps(ioop);
                    else
                    {
                        error = GetLastError();
                        if (error != WSA_IO_INCOMPLETE)
                        {
                            Log_Debug("Write last error = %d", error);
                            ioop->active = false;
                            //UNLOCKED_CALL(ioop->onClose);
                            pendingOnClose.Append(ioop);
                        }
                        else
                        {
                            Log_Debug("error == WSA_IO_INCOMPLETE");
                        }
                    }
                }
            }
            timeout = 0;
        }
        else
        {
            // no completion packet was dequeued
            error = GetLastError();
            if (error != WAIT_TIMEOUT)
                Log_Errno();

            break; //return true;
        }
    }

    startTime = NowClock();
    FOREACH_FIRST (ioop, pendingOnClose)
    {
        pendingOnClose.Remove(ioop);
        UNLOCKED_CALL(ioop->onClose);
        elapsed = NowClock() - startTime;
        if (elapsed > YIELD_TIME)
            break;
    }

    startTime = NowClock();
    FOREACH_FIRST(ioop, pendingOps)
    {
        pendingOps.Remove(ioop);
        if (ioop->type == IOOperation::TCP_READ)
            ProcessTCPRead((TCPRead*)ioop);
        else if (ioop->type == IOOperation::TCP_WRITE)
            ProcessTCPWrite((TCPWrite*)ioop);
        elapsed = NowClock() - startTime;
        if (elapsed > YIELD_TIME)
            break;
    }

    return true;
}

bool IOProcessor::Complete(Callable* callable)
{
    BOOL            ret;
    DWORD           error;
    CallableItem*   item;

    item = new CallableItem;
    item->callable = *callable;
    item->next = item;

    callableMutex.Lock();
    callableQueue.Enqueue(item);
    callableMutex.Unlock();

    iostat.numCompletions++;

    // notify IOCP that there is a new completion callback
    ret = PostQueuedCompletionStatus(iocp, 0, (ULONG_PTR) &callback, (LPOVERLAPPED) NULL);
    if (!ret)
    {
        error = GetLastError();
        Log_Trace("error = %d", error);
        return false;
    }

    return true;
}

void ProcessCompletionCallbacks()
{
    CallableItem*   item;
    CallableItem*   next;

    mutex.Unlock();

    callableMutex.Lock();
    item = callableQueue.First();
    callableQueue.ClearMembers();
    callableMutex.Unlock();

    while (item)
    {
        CallWarnTimeout(item->callable);
        next = item->next;
        delete item;
        item = next;
    }

    mutex.Lock();
}

bool ProcessTCPRead(TCPRead* tcpread)
{
    int         ret;
    DWORD       flags;
    DWORD       error;
    WSABUF      wsabuf;
    Callable    callable;
    DWORD       numBytes;

    iostat.numTCPReads++;

    if (tcpread->listening)
    {
        callable = tcpread->onComplete;
    }
    else
    {
        wsabuf.buf = (char*) tcpread->buffer->GetBuffer() + tcpread->buffer->GetLength();
        wsabuf.len = tcpread->buffer->GetSize() - tcpread->buffer->GetLength();
        if (wsabuf.len > MAX_TCP_READ)
            wsabuf.len = MAX_TCP_READ;

        flags = 0;
        ret = WSARecv(tcpread->fd.handle, &wsabuf, 1, &numBytes, &flags, NULL, NULL);
        if (ret == SOCKET_ERROR)
        {
            error = GetLastError();
            if (error == WSAEWOULDBLOCK)
            {
                tcpread->active = false; // otherwise Add() returns
                IOProcessor_UnprotectedAdd(tcpread);
            }
            else
                callable = tcpread->onClose;
        }
        else if (numBytes == 0)
            callable = tcpread->onClose;
        else
        {
            iostat.numTCPBytesReceived += numBytes;
            tcpread->buffer->Lengthen(numBytes);
            ASSERT(tcpread->buffer->GetLength() <= tcpread->buffer->GetSize());
            callable = tcpread->onComplete;
        }
    }

    if (callable.IsSet())
    {
        tcpread->active = false;
        UNLOCKED_CALL(callable);
    }

    return true;
}

bool ProcessTCPWrite(TCPWrite* tcpwrite)
{
    int         ret;
    WSABUF      wsabuf;
    Callable    callable;
    DWORD       numBytes;
    DWORD       error;

    iostat.numTCPWrites++;

    if (tcpwrite->buffer == NULL)
        callable = tcpwrite->onComplete; // tcp connect case
    else
    {
        // handle tcp write partial writes
        wsabuf.buf = (char*) tcpwrite->buffer->GetBuffer() + tcpwrite->transferred;
        // the -1 is actually a windows bug, for more info see:
        // http://support.microsoft.com/kb/823764/EN-US/
        wsabuf.len = MIN(tcpwrite->buffer->GetLength() - tcpwrite->transferred, SEND_BUFFER_SIZE - 1);

        // perform non-blocking write
        ret = WSASend(tcpwrite->fd.handle, &wsabuf, 1, &numBytes, 0, NULL, NULL);
        if (ret == SOCKET_ERROR)
        {
            error = GetLastError();
            if (error == WSAEWOULDBLOCK)
            {
                tcpwrite->active = false; // otherwise Add() returns
                IOProcessor_UnprotectedAdd(tcpwrite);
            }
            else
                callable = tcpwrite->onClose;
        }
        else if (numBytes == 0)
            callable = tcpwrite->onClose;
        else
        {
            iostat.numTCPBytesSent += numBytes;
            tcpwrite->transferred += numBytes;
            if (tcpwrite->transferred == tcpwrite->buffer->GetLength())
                callable = tcpwrite->onComplete;
            else
            {
                tcpwrite->active = false; // otherwise Add() returns
                IOProcessor_UnprotectedAdd(tcpwrite);
            }
        }
    }

    if (callable.IsSet())
    {
        tcpwrite->active = false;
        UNLOCKED_CALL(callable);
    }

    return true;
}

void IOProcessor::GetStats(IOProcessorStat* stat_)
{
    *stat_ = iostat;
}

#endif
