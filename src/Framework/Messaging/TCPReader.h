#ifndef TCPREADER_H
#define	TCPREADER_H

#include "Framework/TCP/TCPServer.h"
#include "MessageConn.h"

class TCPReader; // forward

/*
===============================================================================

 TCPReaderConn

===============================================================================
*/

class TCPReaderConn : public MessageConn<>
{
public:
	void			SetServer(TCPReader* reader_);
		
	void			OnMessageRead(const ByteString& message);
	void			OnClose();

private:
	TCPReader*		reader;
};

/*
===============================================================================

 TCPReader

===============================================================================
*/

class TCPReader : public TCPServer<TCPReader, TCPReaderConn>
{
friend class TCPReaderConn;
typedef List<TCPReaderConn*> ConnsList;

public:
	bool		Init(int port);

	void		SetOnRead(const Callable& onRead);
	void		GetMessage(ByteString& msg_);
//	void		Stop();
//	void		Continue();
//	bool		IsActive();
	void		InitConn(TCPReaderConn* conn);
	void		OnConnectionClose(TCPReaderConn* conn);

private:
	void		SetMessage(const ByteString& msg_);
	
	Callable	onRead;
	ByteString	msg;
	ConnsList	conns;
	bool		running;
};

#endif
