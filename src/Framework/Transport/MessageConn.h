#ifndef MESSAGECONN_H
#define MESSAGECONN_H

#include "TCPConn.h"
#include "System/Stopwatch.h"
#include "System/Events/EventLoop.h"

#define DEFAULT_MESSAGE_SIZE 8196

template<int bufSize = DEFAULT_MESSAGE_SIZE>
class MessageConn : public TCPConn
{
	typedef MFunc<MessageConn>	Func;
public:
	MessageConn() :
	onResumeRead(this, &MessageConn::OnResumeRead),
	resumeRead(1, &onResumeRead)
	{
		running = true;
	}
	
	virtual void	Init(bool startRead = true);
	virtual void	OnMessageRead(const ByteString& msg) = 0;
	virtual void	OnClose() = 0;
	virtual void	OnRead();
	void			OnResumeRead();
	
	virtual void	Stop();
	virtual void	Continue();

	virtual void	Close();

protected:
	bool			running;
	Func			onResumeRead;
	CdownTimer		resumeRead;

};

template<int bufSize>
void MessageConn<bufSize>::Init(bool startRead)
{
	running = true;
	TCPConn::Init(startRead);
}


template<int bufSize>
void MessageConn<bufSize>::OnRead()
{
	bool			yield;
	uint64_t		start;
	Stopwatch		sw;

	sw.Start();

	TCPRead& tcpread = TCPConn::tcpread;
	unsigned pos, msglength, nread, msgbegin, msgend;
	ByteString msg;
	
	Log_Trace("Read buffer: %.*s",
	 tcpread.data.Length(), tcpread.data.Buffer());
	
	tcpread.requested = IO_READ_ANY;

	pos = 0;

	start = Now();
	yield = false;

	while(running)
	{
		msglength = strntouint64(tcpread.data.Buffer() + pos,
								 tcpread.data.Length() - pos,
								 &nread);
		
		if (msglength > tcpread.data.Size() - numlen(msglength) - 1)
		{
			OnClose();
			return;
		}
		
		if (nread == 0 || (unsigned) tcpread.data.Length() - pos <= nread)
			break;
			
		if (tcpread.data.CharAt(pos + nread) != ':')
		{
			Log_Trace("Message protocol error");
			OnClose();
			return;
		}
	
		msgbegin = pos + nread + 1;
		msgend = pos + nread + 1 + msglength;
		
		if ((unsigned) tcpread.data.Length() < msgend)
		{
			tcpread.requested = msgend - pos;
			break;
		}

		msg.Set(tcpread.data.Buffer() + msgbegin, msglength);
		OnMessageRead(msg);
		
		pos = msgend;
		
		// if the user called Close() in OnMessageRead()
		if (TCPConn::state != TCPConn::CONNECTED)
			return;
		
		if (tcpread.data.Length() == msgend)
			break;

		if (Now() - start > 10 && running)
		{
			// let other code run every 10 msec
			yield = true;
			if (resumeRead.IsActive())
				ASSERT_FAIL();
			EventLoop::Add(&resumeRead);
			break;
		}
	}
	
	if (pos > 0)
	{
		memmove(tcpread.data.Buffer(),
				tcpread.data.Buffer() + pos,
				tcpread.data.Length() - pos);
		
		tcpread.data.Shorten(pos);
	}
	
	if (TCPConn::state == TCPConn::CONNECTED
	 && running && !tcpread.active && !yield)
		IOProcessor::Add(&tcpread);

	sw.Stop();
	Log_Trace("time spent in OnRead(): %ld", sw.Elapsed());
}

template<int bufSize>
void MessageConn<bufSize>::OnResumeRead()
{
	Log_Trace();

	OnRead();
}

template<int bufSize>
void MessageConn<bufSize>::Stop()
{
	Log_Trace();
	running = false;
}

template<int bufSize>
void MessageConn<bufSize>::Continue()
{
	Log_Trace();
	running = true;
	OnRead();
}

template<int bufSize>
void MessageConn<bufSize>::Close()
{
	EventLoop::Remove(&resumeRead);
	TCPConn::Close();
}

#endif
