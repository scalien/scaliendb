#ifndef PLATFORM_WINDOWS

#include "Mutex.h"

Mutex::Mutex()
{
    pthread_mutex_init(&mutex, NULL);
}

Mutex::~Mutex()
{
    // not sure if this is necessary
    pthread_mutex_destroy(&mutex);
}

void Mutex::Lock()
{
    pthread_mutex_lock(&mutex);
    threadID = (uint64_t) pthread_self();
}

bool Mutex::TryLock()
{
    int     ret;
    
    ret = pthread_mutex_trylock(&mutex);
    if (ret == 0)
    {
        threadID = (uint64_t) pthread_self();
        return true;
    }
    return false;
}

void Mutex::Unlock()
{
    threadID = 0;
    pthread_mutex_unlock(&mutex);
}

#endif
