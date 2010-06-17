#ifndef TRANSPORT_TCP_WRITER_H
#define TRANSPORT_TCP_WRITER_H

#include "TCPConn.h"

/*************************************************************************************************

									TCPWriter

 *************************************************************************************************/

class TCPWriter : public TCPConn
{
public:
	
	virtual bool	Init(Endpoint &endpoint_);
	virtual void	Write(const ByteString &bs);

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
