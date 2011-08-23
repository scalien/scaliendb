#ifndef _WIN32
#include "Signal.h"

Signal::Signal()
{
    pthread_mutex_init(&impl.mutex, NULL);
    pthread_cond_init(&impl.cond, NULL);
    signaled = false;
    waiting = false;
}

Signal::~Signal()
{
}

bool Signal::IsWaiting()
{
    return waiting;
}

void Signal::SetWaiting(bool waiting_)
{
    pthread_mutex_lock(&impl.mutex);
    waiting = waiting_;
    pthread_mutex_unlock(&impl.mutex);
}

void Signal::Wake()
{
    pthread_mutex_lock(&impl.mutex);
    signaled = true;
    pthread_cond_signal(&impl.cond);
    pthread_mutex_unlock(&impl.mutex);
}

void Signal::Wait()
{
    pthread_mutex_lock(&impl.mutex);
    waiting = true;
    while (!signaled)
        pthread_cond_wait(&impl.cond, &impl.mutex);
    waiting = false;
    signaled = false;
    pthread_mutex_unlock(&impl.mutex);
}

#endif
