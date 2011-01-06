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
}

void Mutex::Unlock()
{
    pthread_mutex_unlock(&mutex);
}
