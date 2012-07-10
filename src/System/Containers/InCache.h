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

#ifdef _DEBUG
#ifdef _CRTDBG_MAP_ALLOC
// undef new because InCache uses placement new
// if we see memleaks without source traces
// then it's coming from InCache
#undef new
#endif
#endif

template<class T>
void InCache<T>::Init(uint64_t size)
{
    maxSize = size;    
}

template<class T>
void InCache<T>::Shutdown()
{
    T*  t;

    // TODO: HACK because all elems in the list are already destroyed, 
    // we need to call the contstructors before deleting the cache
    FOREACH_FIRST (t, freeList)
    {
        freeList.Remove(t);
        new(t) T;
        delete t;
    }
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
