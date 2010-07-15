#ifdef _WIN32
#define _WIN32_WINNT			0x0400	// Windows NT 4.0
#define WIN32_LEAN_AND_MEAN

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <windows.h>
#include <process.h>

#include "ThreadPool.h"
#include "System/Events/Callable.h"

/*
===============================================================================

 ThreadPool_Windows

===============================================================================
*/

class ThreadPool_Windows : public ThreadPool
{
public:
	ThreadPool_Windows(int numThread);
	~ThreadPool_Windows();

	virtual void				Start();
	virtual void				Stop();

	virtual void				Execute(Callable *callable);
	
private:
	HANDLE*						threads;
	CRITICAL_SECTION			critsec;
	HANDLE						event;	
	static unsigned __stdcall	ThreadFunc(void *arg);
	void						ThreadPoolFunc();
};

/*
===============================================================================
*/

ThreadPool* ThreadPool::Create(int numThread)
{
	return new ThreadPool_Windows(numThread);
}

ThreadPool_Windows::ThreadPool_Windows(int numThread_)
{
	numThread = numThread_;
}

ThreadPool_Windows::~ThreadPool_Windows()
{
	Stop();
}

void ThreadPool_Windows::Start()
{
	int		i;

	numPending = 0;
	numActive = 0;
	running = false;

	InitializeCriticalSection(&critsec);
	// create the event as an auto-reset event object, set to nonsignaled state
	event = CreateEvent(NULL, FALSE, FALSE, NULL);
	threads = new HANDLE[numThread];

	for (i = 0; i < numThread; i++)
		threads[i] = (HANDLE)_beginthreadex(NULL, 0, ThreadFunc, this, 0, NULL);
	
	running = true;
}

void ThreadPool_Windows::Stop()
{
	int		i;

	if (!running)
		return;

	running = false;
	if (threads)
	{
		// the finished thread will call SetEvent again, see ThreadPoolFunc
		SetEvent(event);

		WaitForMultipleObjects(numThread, threads, TRUE, INFINITE);
		for (i = 0; i < numThread; i++)
			CloseHandle(threads[i]);
		
		delete[] threads;
	}

	if (event)
		CloseHandle(event);

	DeleteCriticalSection(&critsec);
}

void ThreadPool_Windows::Execute(Callable *callable)
{
	EnterCriticalSection(&critsec);

	callables.Append(callable);
	numPending++;

	LeaveCriticalSection(&critsec);
	SetEvent(event);

}

void ThreadPool_Windows::ThreadPoolFunc()
{
	Callable*	callable;
	Callable**	it;

	callable = NULL;
	while (running)
	{
		// TODO simplify the logic here & make it similar to Posix implementation
		if (!callable && running)
			WaitForSingleObject(event, INFINITE);

		do
		{
			callable = NULL;
			EnterCriticalSection(&critsec);

			numActive--;
			
			it = callables.Head();
			if (it)
			{
				callable = *it;
				callables.Remove(it);
				numPending--;
			}
			
			numActive++;

			LeaveCriticalSection(&critsec);

			Call(callable);
		} while (callable);
	}

	// wake up next sleeping thread
	SetEvent(event);
}

unsigned ThreadPool_Windows::ThreadFunc(void *arg)
{
	ThreadPool_Windows *tp = (ThreadPool_Windows *) arg;
	tp->ThreadPoolFunc();

	return 0;
}

#endif
