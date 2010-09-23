#ifndef INQUEUE_H
#define INQUEUE_H

#include "System/Common.h"

/*
===============================================================================

 InQueue for storing objects with pre-allocated next pointer.

===============================================================================
*/

template<class T>
class InQueue
{
public:
    InQueue();

    void    Enqueue(T* elem);   
    T*      Dequeue();

    void    Clear();
    
    T*      First() const;  
    T*      Last() const;
    int     GetLength() const;
    
    T*      Next(T* t) const;

private:
    T*      head;
    T*      tail;
    int     length;
};

/*
===============================================================================
*/

template<class T>
InQueue<T>::InQueue()
{
    length = 0;
    head = NULL;
    tail = NULL;
    Clear();
}


template<class T>
void InQueue<T>::Enqueue(T* elem)
{
    assert(elem != NULL);
    
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
        elem->next = NULL;
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

#endif
