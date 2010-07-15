#ifndef MESSAGEWRITER_H
#define MESSAGEWRITER_H

#include "Framework/TCP/TCPConnection.h"

/*
===============================================================================

 TCPWriter

===============================================================================
*/

class MessageWriter : public TCPConnection
{
public:
	
	virtual bool		Init(Endpoint &endpoint);
	
	void				WritePrefix(Buffer* prefix, Buffer* message);

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
