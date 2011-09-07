#ifndef LOCKGUARD_H
#define LOCKGUARD_H

/*
===============================================================================================

 Generic scoped lock guard for classes with Lock and Unlock member functions

===============================================================================================
*/

template<typename T>
class LockGuard
{
public:
    LockGuard(T& t);
    ~LockGuard();

    void        Lock();
    void        Unlock();

private:
    T&          t;
    bool        locked;
};

/*
===============================================================================================
*/

template<typename T>
LockGuard<T>::LockGuard(T& t_) : t(t_)
{
    t.Lock();
    locked = true;
}

template<typename T>
LockGuard<T>::~LockGuard()
{
    Unlock();
}

template<typename T>
void LockGuard<T>::Lock()
{
    if (!locked)
    {
        t.Lock();
        locked = true;
    }
}

template<typename T>
void LockGuard<T>::Unlock()
{
    if (locked)
    {
        locked = false;
        t.Unlock();
    }
}

#endif
