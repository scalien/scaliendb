#ifndef TRANSPORTTCPREADER_H
#define	TRANSPORTTCPREADER_H

#include "TCPServer.h"
#include "MessageConn.h"

class TransportTCPReader; // forward

class TransportTCPConn : public MessageConn<>
{
public:
	TransportTCPConn(TransportTCPReader* reader_)
	{
		reader = reader_;
	}
	
	void		OnMessageRead(const ByteString& message);
	void		OnClose();

private:
	TransportTCPReader* reader;
};


class TransportTCPReader : public TCPServer<TransportTCPReader, TransportTCPConn>
{
friend class TransportTCPConn;
typedef List<TransportTCPConn*> ConnsList;

public:
	bool		Init(int port);

	void		SetOnRead(Callable* onRead);
	void		GetMessage(ByteString& msg_);
	void		Stop();
	void		Continue();
	bool		IsActive();
	void		OnConnect();
	void		OnConnectionClose(TransportTCPConn* conn);

private:
	void		SetMessage(const ByteString& msg_);
	
	Callable*	onRead;
	ByteString	msg;
	ConnsList	conns;
	bool		running;
};

#endif
