#ifndef POINTERGUARD_H
#define POINTERGUARD_H

#include "Platform.h"

/*
===============================================================================================

 PointerGuard

===============================================================================================
*/

#define P(a) ((a).Get())

template<class T>
class PointerGuard
{
public:
    PointerGuard();
    PointerGuard(T* ptr);
    ~PointerGuard();
    
    void    SetAutoCreate(bool autoCreate);
    
    void    Set(T* ptr);
    T*      Get();
    T*      Release();
    void    Free();
    void    Transfer(PointerGuard<T>& other);

private:
    bool    autoCreate;
    T*      ptr;
};

/*
===============================================================================================
*/

template<class T>
PointerGuard<T>::PointerGuard()
{
    autoCreate = false;
    ptr = NULL;
}

template<class T>
PointerGuard<T>::PointerGuard(T* ptr_)
{
    ptr = ptr_;
}

template<class T>
PointerGuard<T>::~PointerGuard()
{
    if (ptr != NULL)
        delete ptr;
}

template<class T>
void PointerGuard<T>::SetAutoCreate(bool autoCreate_)
{
    autoCreate = autoCreate_;
}


template<class T>
void PointerGuard<T>::Set(T* ptr_)
{
    delete ptr;
    ptr = ptr_;
}

template<class T>
T* PointerGuard<T>::Get()
{
    if (ptr == NULL)
        ptr = new T;
    return ptr;
}

template<class T>
T* PointerGuard<T>::Release()
{
    T* ret;
    ret = ptr;
    ptr = NULL;
    return ret;
}

template<class T>
void PointerGuard<T>::Free()
{
    delete ptr;
    ptr = NULL;
}

template<class T>
void PointerGuard<T>::Transfer(PointerGuard<T>& other)
{
    other.Free();
    other.Set(Release());
}


#endif
