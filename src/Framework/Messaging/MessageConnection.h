#ifndef MESSAGECONNECTION_H
#define MESSAGECONNECTION_H

#include "System/Stopwatch.h"
#include "System/Events/EventLoop.h"
#include "System/Buffers/Buffer.h"
#include "Framework/TCP/TCPConnection.h"

#define YIELD_TIME	 10 // msec

/*
===============================================================================

MessageConnection

===============================================================================
*/

class MessageConnection : public TCPConnection
{
public:
	MessageConnection();
		
	virtual void		Init(bool startRead = true);
	virtual void		OnMessageRead() = 0;
	virtual void		OnClose() = 0;
	virtual void		OnRead();
	void				OnResumeRead();

	ReadBuffer			GetMessage();
	
	virtual void		Stop();
	virtual void		Continue();

	virtual void		Close();

protected:
	bool				running;
	CdownTimer			resumeRead;
	ReadBuffer			msg;
};

#endif
