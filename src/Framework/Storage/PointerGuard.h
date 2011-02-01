#ifndef POINTERGUARD_H
#define POINTERGUARD_H

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
    
    void    Set(T* ptr);
    T*      Get();
    T*      Release();
    
    void    Free();

private:
    T*      ptr;
};

/*
===============================================================================================
*/

template<class T>
PointerGuard<T>::PointerGuard()
{
    ptr = NULL;
}

template<class T>
void PointerGuard<T>::Set(T* ptr_)
{
    delete ptr;
    ptr = ptr_;
}

template<class T>
PointerGuard<T>::~PointerGuard()
{
    if (ptr != NULL)
        delete ptr;
}

template<class T>
PointerGuard<T>::PointerGuard(T* ptr_)
{
    ptr = ptr_;
}

template<class T>
T* PointerGuard<T>::Get()
{
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
    if (ptr != NULL)
        delete ptr;
    
    ptr = NULL;
}


#endif
