#ifndef TCPSERVER_H
#define TCPSERVER_H

#include "System/Events/Callable.h"
#include "System/IO/Socket.h"
#include "System/IO/IOOperation.h"
#include "System/IO/IOProcessor.h"
#include "System/Containers/Queue.h"
#include "TCPConn.h"

template<class T, class Conn>
class TCPServer
{
public:
	TCPServer() :
	onConnect(this, &TCPServer<T, Conn>::OnConnect)
	{
		numActive = 0;
	}

	~TCPServer()
	{
		Conn* conn;
		while ((conn = dynamic_cast<Conn*>(conns.Dequeue())) != NULL)
			delete conn;
	}
	
	bool Init(int port_, int backlog_ = 3)
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

		tcpread.fd = listener.fd;
		tcpread.listening = true;
		tcpread.onComplete = &onConnect;
		
		return IOProcessor::Add(&tcpread);	
	}
	
	void Close()
	{
		Conn** it;
	
		for (it = activeConns.Head(); it != NULL; it = activeConns.Head())
			(*it)->OnClose();

		listener.Close();
	}

	void DeleteConn(Conn* conn)
	{
		Log_Trace();
		
		numActive--;
		assert(numActive >= 0);
		
		activeConns.Remove(conn);

		if (conns.Size() >= backlog)
			delete conn;
		else
			conns.Enqueue((TCPConn*)conn);
	}

protected:
	typedef Queue<TCPConn, &TCPConn::next> ConnList;
	
	TCPRead				tcpread;
	Socket				listener;
	MFunc<TCPServer>	onConnect;
	int					backlog;
	ConnList			conns;
	int					numActive;
	List<Conn*>			activeConns;
	
	void OnConnect()
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
	
	Conn* GetConn()
	{
		if (conns.Size() > 0)
			return dynamic_cast<Conn*>(conns.Dequeue());
		
		return new Conn;
	}
	
	void InitConn(Conn* conn)
	{
		T* pT = static_cast<T*>(this);
		conn->Init(pT);
	}
	
};

#endif
