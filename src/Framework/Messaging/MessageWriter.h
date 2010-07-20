#ifndef MESSAGEWRITER_H
#define MESSAGEWRITER_H

#include "Framework/TCP/TCPConnection.h"
#include "Message.h"

/*
===============================================================================

 MessageWriter

===============================================================================
*/

class MessageWriter : public TCPConnection
{
public:
	
	virtual bool		Init(Endpoint &endpoint);
	
	void				Write(const Buffer& prefix, const Message& msg);
	void				WritePriority(const Buffer& prefix, const Message& msg);

private:
	void				Connect();
	void				OnConnect();
	void				OnConnectTimeout();
	
	// TCPConn interface
	virtual void		OnRead();
	virtual void		OnClose();

	Endpoint			endpoint;
};

#endif
