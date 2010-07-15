#ifndef TCPCONNECTION_H
#define TCPCONNECTION_H

#include "System/Events/Callable.h"
#include "System/Events/CdownTimer.h"
#include "System/Buffer/Buffer.h"
#include "System/Containers/QueueP.h"
#include "System/IO/Socket.h"
#include "System/IO/IOOperation.h"
#include "TCPWriteProxy.h"

#define TCP_CONNECT_TIMEOUT 3000
#define TCP_BUFFER_SIZE		8196

/*
===============================================================================

 TCPConnection

===============================================================================
*/

class TCPConnection
{
public:
	enum State			{ DISCONNECTED, CONNECTED, CONNECTING };

	TCPConnection();
	virtual ~TCPConnection();
	
	void				InitConnected(bool startRead = true);
	void				Connect(Endpoint &endpoint, unsigned timeout);
	void				Close();

	Socket&				GetSocket() { return socket; }
	const State			GetState() { return state; }
	
	void				AsyncRead(bool start = true);

	void				SetWriteProxy(TCPWriteProxy* writeProxy);
	TCPWriteProxy*		GetWriteProxy();
	void				OnWritePending(); // for TCPWriteProxy
	
	TCPConnection*		next;
	TCPConnection*		prev;

protected:
	State				state;
	Socket				socket;
	TCPRead				tcpread;
	TCPWrite			tcpwrite;
	TCPWriteProxy*		writeProxy;
	Buffer				readBuffer;
	CdownTimer			connectTimeout;
		
	void				Init(bool startRead = true);

	virtual void		OnRead() = 0;
	virtual void		OnWrite();
	virtual void		OnClose() = 0;
	virtual void		OnConnect();
	virtual void		OnConnectTimeout();

};

#endif
