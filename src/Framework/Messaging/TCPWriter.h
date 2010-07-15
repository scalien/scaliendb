#ifndef TCPWRITER_H
#define TCPWRITER_H

#include "Framework/TCP/TCPConnection.h"

/*
===============================================================================

 TCPWriter

===============================================================================
*/

class TCPWriter : public TCPConnection
{
public:
	
	virtual bool		Init(Endpoint &endpoint_);
	virtual void		Write(Buffer* buffer);

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
