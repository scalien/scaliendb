#ifndef TCPSERVER_H
#define TCPSERVER_H

#include "System/Events/Callable.h"
#include "System/IO/Socket.h"
#include "System/IO/IOOperation.h"
#include "System/IO/IOProcessor.h"
#include "System/Containers/Queue.h"
#include "System/Containers/List.h"
#include "TCPConn.h"

/*
===============================================================================

 TCPServer

 TCPServer is a template class for listening for incoming TCP connections
 on a port. TCPServer will create and manage connections given by template
 parameter Conn.

===============================================================================
*/

template<class T, class Conn>
class TCPServer
{
	typedef Queue<TCPConn, &TCPConn::next> ConnList;

public:
	TCPServer();
	~TCPServer();
	
	bool				Init(int port_, int backlog_ = 3);
	void				Close();
	void				DeleteConn(Conn* conn);

protected:
	void				OnConnect();
	Conn*				GetConn();
	void				InitConn(Conn* conn);

	
	TCPRead				tcpread;
	Socket				listener;
	int					backlog;
	ConnList			conns;
	int					numActive;
	List<Conn*>			activeConns;
};

/*
===============================================================================
*/

template<class T, class Conn>
TCPServer<T, Conn>::TCPServer()
{
	numActive = 0;
}

template<class T, class Conn>
TCPServer<T, Conn>::~TCPServer()
{
	Conn* conn;
	while ((conn = dynamic_cast<Conn*>(conns.Dequeue())) != NULL)
		delete conn;
}

template<class T, class Conn>
bool TCPServer<T, Conn>::Init(int port_, int backlog_)
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
	return IOProcessor::Add(&tcpread);	
}

template<class T, class Conn>
void TCPServer<T, Conn>::Close()
{
	Conn** it;

	for (it = activeConns.Head(); it != NULL; it = activeConns.Head())
		(*it)->OnClose();

	listener.Close();
}

template<class T, class Conn>
void TCPServer<T, Conn>::DeleteConn(Conn* conn)
{
	Log_Trace();
	
	numActive--;
	assert(numActive >= 0);
	
	activeConns.Remove(conn);

	if (conns.Length() >= backlog)
		delete conn;
	else
		conns.Enqueue((TCPConn*)conn);
}

template<class T, class Conn>
void TCPServer<T, Conn>::OnConnect()
{
	T* pT = static_cast<T*>(this);
	Conn* conn = pT->GetConn();
	numActive++;
	if (listener.Accept(&(conn->GetSocket())))
	{
		conn->GetSocket().SetNonblocking();
		conn->GetSocket().SetNodelay();
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
Conn* TCPServer<T, Conn>::GetConn()
{
	if (conns.Length() > 0)
		return dynamic_cast<Conn*>(conns.Dequeue());
	
	return new Conn;
}

template<class T, class Conn>
void TCPServer<T, Conn>::InitConn(Conn* conn)
{
	T* pT = static_cast<T*>(this);
	conn->Init(pT);
}

#endif
