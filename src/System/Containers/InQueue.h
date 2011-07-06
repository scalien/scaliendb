#ifndef INQUEUE_H
#define INQUEUE_H

#include "System/Common.h"
#include "System/Macros.h"

/*
===============================================================================================

 InQueue for storing objects with pre-allocated next pointer.

===============================================================================================
*/

template<class T>
class InQueue
{
public:
    InQueue();

    void    Enqueue(T* elem);       // add to tail/last
    T*      Dequeue();              // remove from head/first

    void    Clear();
    void    ClearMembers();
    void    DeleteQueue();
    
    T*      First() const;  
    T*      Last() const;
    int     GetLength() const;
    
    T*      Next(T* t) const;

    bool    Contains(T* t);

private:
    T*      head;
    T*      tail;
    int     length;
};

/*
===============================================================================================
*/

template<class T>
InQueue<T>::InQueue()
{
    length = 0;
    head = NULL;
    tail = NULL;
}


template<class T>
void InQueue<T>::Enqueue(T* elem)
{
    ASSERT(elem != NULL);
    ASSERT(elem->next == elem);
    
    elem->next = NULL;
    if (tail)
        tail->next = elem;
    else
        head = elem;
    tail = elem;
    length++;
}

template<class T>
T* InQueue<T>::Dequeue()
{
    T* elem;
    elem = head;
    if (elem)
    {
        head = dynamic_cast<T*>(elem->next);
        if (tail == elem)
            tail = NULL;
        elem->next = elem;
        length--;
    }
    return elem;
}

template<class T>
void InQueue<T>::Clear()
{
    T* elem;

    do elem = Dequeue();
    while (elem);
}

template<class T>
void InQueue<T>::ClearMembers()
{
    head = NULL;
    tail = NULL;
    length = 0;
}

template<class T>
void InQueue<T>::DeleteQueue()
{
    T*  elem;
    
    do
    {
        elem = Dequeue();
        delete elem;
    } while (elem);
}

template<class T>
T* InQueue<T>::First() const
{
    return head;
}

template<class T>
T* InQueue<T>::Last() const
{
    return tail;
}

template<class T>
T* InQueue<T>::Next(T* t) const
{
    return dynamic_cast<T*>(t->next);
}

template<class T>
int InQueue<T>::GetLength() const
{
    return length;
}

template<class T>
bool InQueue<T>::Contains(T* t)
{
    T* it;
    
    FOREACH (it, *this)
    {
        if (it == t)
            return true;
    }
    
    return false;
}

#endif
