#ifndef INCACHE_H
#define INCACHE_H

#include <new>
#include "System/Platform.h"
#include "InList.h"

template<class T>
class InCache
{
public:
    void                        Init(uint64_t size);
    void                        Shutdown();

    T*                          Acquire();
    void                        Release(T* t);

    uint64_t                    GetMemorySize();

private:

    unsigned                    maxSize;
    InList<T>                   freeList;
};

template<class T>
void InCache<T>::Init(uint64_t size)
{
    maxSize = size;    
}

template<class T>
void InCache<T>::Shutdown()
{
    freeList.DeleteList();
}

template<class T>
T* InCache<T>::Acquire()
{
    T* t;
    
    if (freeList.GetLength() > 0)
    {
        t = freeList.First();
        freeList.Remove(t);
        return new(t) T;
    }
 
    return new T;
}

template<class T>
void InCache<T>::Release(T* t)
{
    if (freeList.GetLength() < maxSize)
    {
        t->~T();
        freeList.Append(t);
        return;
    }

    delete t;
}

template<class T>
uint64_t InCache<T>::GetMemorySize()
{
    return (uint64_t)freeList.GetLength() * sizeof(T);
}

#endif
