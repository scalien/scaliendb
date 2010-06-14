#ifndef TRANSPORT_TCP_WRITER_H
#define TRANSPORT_TCP_WRITER_H

#include "TCPConn.h"

class TransportTCPWriter : public TCPConn
{
public:
	
	virtual bool	Init(Endpoint &endpoint_);
	virtual void	Write(ByteString &bs);

private:
	void			Connect();
	void			OnConnect();
	void			OnConnectTimeout();
	
	// TCPConn interface
	virtual void	OnRead();
	virtual void	OnClose();

	Endpoint		endpoint;
};


#endif
