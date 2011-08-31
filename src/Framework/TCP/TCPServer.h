#ifndef TCPSERVER_H
#define TCPSERVER_H

#include "System/Events/Callable.h"
#include "System/IO/Socket.h"
#include "System/IO/IOOperation.h"
#include "System/IO/IOProcessor.h"
#include "System/Containers/InQueue.h"
#include "System/Containers/InList.h"
#include "TCPConnection.h"

/*
===============================================================================================

 TCPServer

 -
 
 TCPServer is a template class for listening for incoming TCP connections
 on a port. TCPServer will create and manage connections given by template
 parameter Conn.

===============================================================================================
*/

template<class T, class Conn>
class TCPServer
{
public:
    TCPServer();
    ~TCPServer();
    
    bool                    Init(int port, bool listen = true, int backlog = 3);
    bool                    Listen();
    void                    Close();
    void                    DeleteConn(Conn* conn);

    unsigned                GetNumActiveConns();
    unsigned                GetNumInactiveConns();

    uint64_t                GetMemoryUsage();

protected:
    void                    OnConnect();
	void					OnClose();
    Conn*                   GetConn();
    void                    InitConn(Conn* conn);
    bool                    IsManaged();

    
    TCPRead                 tcpread;
    Socket                  listener;
    int                     backlog;
    InList<Conn>            activeConns;
    InQueue<Conn>           inactiveConns;
};

/*
===============================================================================================
*/

template<class T, class Conn>
TCPServer<T, Conn>::TCPServer()
{
}

template<class T, class Conn>
TCPServer<T, Conn>::~TCPServer()
{
    Conn* conn;
    T* pT = static_cast<T*>(this);

    if (pT->IsManaged())
    {
        while ((conn = inactiveConns.Dequeue()) != NULL)
            delete conn;
    }

	IOProcessor::Remove(&tcpread);
}

template<class T, class Conn>
bool TCPServer<T, Conn>::Init(int port_, bool listen, int backlog_)
{
    bool ret;
    
    Log_Trace();
    backlog = backlog_;

    ret = listener.Create(Socket::TCP);
    if (!ret)
        return false;
    ret = listener.Listen(port_);
    if (!ret)
        return false;   
    ret = listener.SetNonblocking();
    if (!ret)
        return false;

    tcpread.Listen(listener.fd, MFUNC(TCPServer, OnConnect));
	tcpread.onClose = MFUNC(TCPServer, OnClose);

    if (listen)
        return IOProcessor::Add(&tcpread);
    else
        return true;
}

template<class T, class Conn>
bool TCPServer<T, Conn>::Listen()
{
    return IOProcessor::Add(&tcpread);
}

template<class T, class Conn>
void TCPServer<T, Conn>::Close()
{
    Conn* it;
    Conn* next;

    T* pT = static_cast<T*>(this);

    if (pT->IsManaged())
    {
        for (it = activeConns.First(); it != NULL; /* advanced in body */)
        {
            next = activeConns.Next(it);
            it->OnClose();
            it = next;
        }
    }
    
    listener.Close();
}

template<class T, class Conn>
void TCPServer<T, Conn>::DeleteConn(Conn* conn)
{
    Log_Trace();
    
    T* pT = static_cast<T*>(this);
    
    if (pT->IsManaged())
    {
        activeConns.Remove(conn);

        if (inactiveConns.GetLength() >= backlog)
            delete conn;
        else
            inactiveConns.Enqueue(conn);
    }
    else
        delete conn;
}

template<class T, class Conn>
unsigned TCPServer<T, Conn>::GetNumActiveConns()
{
    return activeConns.GetLength();
}

template<class T, class Conn>
unsigned TCPServer<T, Conn>::GetNumInactiveConns()
{
    return inactiveConns.GetLength();
}

template<class T, class Conn>
uint64_t TCPServer<T, Conn>::GetMemoryUsage()
{
    uint64_t    memoryUsage;
    Conn*       conn;

    memoryUsage = 0;
    FOREACH (conn, activeConns)
        memoryUsage += conn->GetMemoryUsage();

    FOREACH (conn, inactiveConns)
        memoryUsage += conn->GetMemoryUsage();

    return memoryUsage;
}

template<class T, class Conn>
void TCPServer<T, Conn>::OnConnect()
{
    Log_Trace();
    
    T* pT = static_cast<T*>(this);
    Conn* conn = pT->GetConn();
    if (listener.Accept(&(conn->GetSocket())))
    {
        conn->GetSocket().SetNonblocking();
        conn->GetSocket().SetNodelay();
        if (pT->IsManaged())
            activeConns.Append(conn);
        pT->InitConn(conn);
    }
    else
    {
        Log_Trace("Accept() failed");
        pT->DeleteConn(conn);
    }
    
    IOProcessor::Add(&tcpread);     
}

template<class T, class Conn>
void TCPServer<T, Conn>::OnClose()
{
	// there was an error while accepting the socket and it was closed
    IOProcessor::Add(&tcpread);
}

template<class T, class Conn>
Conn* TCPServer<T, Conn>::GetConn()
{
    T* pT = static_cast<T*>(this);

    if (pT->IsManaged() && inactiveConns.GetLength() > 0)
        return inactiveConns.Dequeue();
    
    return new Conn;
}

template<class T, class Conn>
void TCPServer<T, Conn>::InitConn(Conn* conn)
{
    T* pT = static_cast<T*>(this);
    conn->Init(pT);
}

template<class T, class Conn>
bool TCPServer<T, Conn>::IsManaged()
{
    return true;
}

#endif
