#ifndef _WIN32
#include <pthread.h>

#include "ThreadPool.h"
#include "System/Common.h"
#include "System/Log.h"
#include "System/Events/Callable.h"

class ThreadPool_Pthread : public ThreadPool
{
public:
	ThreadPool*		Create(int numThread);

	ThreadPool_Pthread(int numThread);
	~ThreadPool_Pthread();

	virtual void	Start();
	virtual void	Stop();

	virtual void	Execute(const Callable& callable);

protected:
	pthread_t*		threads;
	pthread_mutex_t	mutex;
	pthread_cond_t	cond;
	
	static void*	thread_function(void *param);
	void			ThreadFunction();
};

ThreadPool* ThreadPool::Create(int numThread)
{
	return new ThreadPool_Pthread(numThread);
}

void* ThreadPool_Pthread::thread_function(void* param)
{
	ThreadPool_Pthread* tp = (ThreadPool_Pthread*) param;
	
	blocksigs();
	tp->ThreadFunction();
	
	// pthread_exit should be called for cleanup, instead it creates
	// more unreachable bytes with valgrind:
	//pthread_exit(NULL);
	return NULL;
}

void ThreadPool_Pthread::ThreadFunction()
{	
	Callable callable;
	Callable* it;
	
	while (running)
	{
		pthread_mutex_lock(&mutex);
		numActive--;
		
	wait:
		while (numPending == 0 && running)
			pthread_cond_wait(&cond, &mutex);
		
		if (!running)
		{
			pthread_mutex_unlock(&mutex);
			break;
		}
		it = callables.Head();
		if (!it)
			goto wait;
			
		callable = *it;
		callables.Remove(it);
		numPending--;
		numActive++;
		
		pthread_mutex_unlock(&mutex);
		
		Call(callable);
	}	
}

ThreadPool_Pthread::ThreadPool_Pthread(int numThread_)
{
	numThread = numThread_;
	numPending = 0;
	numActive = 0;
	running = false;
	
	pthread_mutex_init(&mutex, NULL);
	pthread_cond_init(&cond, NULL);
	
	threads = new pthread_t[numThread];
}

ThreadPool_Pthread::~ThreadPool_Pthread()
{
	Stop();
	delete[] threads;
}

void ThreadPool_Pthread::Start()
{
	int i;
	
	if (running)
		return;
	
	running = true;
	numActive = numThread;
	
	for (i = 0; i < numThread; i++)
	{
		pthread_create(&threads[i], NULL, thread_function, this);
	}
}

void ThreadPool_Pthread::Stop()
{
	int i;
	
	if (!running)
		return;

	running = false;
	
	pthread_mutex_lock(&mutex);
	pthread_cond_broadcast(&cond);
	pthread_mutex_unlock(&mutex);

	for (i = 0; i < numThread; i++)
	{
		pthread_join(threads[i], NULL);
		pthread_detach(threads[i]);
	}
	
	numActive = 0;
}

void ThreadPool_Pthread::Execute(const Callable& callable)
{
	pthread_mutex_lock(&mutex);
	
	callables.Append((Callable&)callable);
	numPending++;
	
	pthread_cond_signal(&cond);
	
	pthread_mutex_unlock(&mutex);
}

#endif
