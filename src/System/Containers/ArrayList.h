#ifndef ARRAYLIST_H
#define ARRAYLIST_H

#include <stdlib.h>

#include "System/Macros.h"

/*
===============================================================================================

 ArrayList implements the List interface for bounded number of items.

===============================================================================================
*/

template<typename T, unsigned size>
class ArrayList
{
public:
    ArrayList();
    
    T&          Get(unsigned i);

    bool        Append(T& t);
    bool        Prepend(T& t);
    
    bool        Remove(T& t);
    T*          Remove(T* t);
    void        Clear();
    
    unsigned    GetLength();
    unsigned    GetSize();
    
    T*          First();
    T*          Last();
    
    T*          Next(T* t);
    T*          Prev(T* t);

    bool        Contains(T& t);

private:
    T           buffer[size];
    unsigned    num;
};

/*
===============================================================================
*/

template<typename T, unsigned size>
ArrayList<T, size>::ArrayList()
{
    num = 0;
}

template<typename T, unsigned size>
T& ArrayList<T, size>::Get(unsigned i)
{
    ASSERT(i <= size);

    return buffer[i];
}

template<typename T, unsigned size>
bool ArrayList<T, size>::Append(T& t)
{
    if (num == size)
        return false;
    
    Get(num) = t;
    num++;
    
    return true;
}

template<typename T, unsigned size>
bool ArrayList<T, size>::Prepend(T& t)
{
    if (num == size)
        return false;

    // shift all elems
    memmove(buffer + 1, buffer, num * sizeof(T));
    
    Get(0) = t;
    num++;
    
    return true;
}

template<typename T, unsigned size>
unsigned ArrayList<T, size>::GetLength()
{
    return num;
}

template<typename T, unsigned size>
unsigned ArrayList<T, size>::GetSize()
{
    return size;
}

template<typename T, unsigned size>
bool ArrayList<T, size>::Remove(T& t)
{
    T*  it;

    for (it = First(); it != NULL; it = Next(it))
    {
        if (*it == t)
        {
            Remove(it);
            return true;
        }
    }
    
    return false;
}

template<typename T, unsigned size>
T* ArrayList<T, size>::Remove(T* t)
{
    unsigned    pos;
    
    ASSERT(t >= buffer && t < buffer + num);
    
    pos = (unsigned) (t - buffer);
    if (pos == num - 1)
    {
        // last elem
        num--;
        return NULL;
    }
    
    memmove(buffer + pos, buffer + pos + 1, (num - pos - 1) * sizeof(T));
    num--;

    return &Get(pos);
}

template<typename T, unsigned size>
void ArrayList<T, size>::Clear()
{
    num = 0;
}

template<typename T, unsigned size>
T* ArrayList<T, size>::First()
{
    if (num == 0)
        return NULL;
    
    return &buffer[0];
}

template<typename T, unsigned size>
T* ArrayList<T, size>::Last()
{
    if (num == 0)
        return NULL;
    
    return &buffer[num - 1];
}

template<typename T, unsigned size>
T* ArrayList<T, size>::Next(T* t)
{
    unsigned    pos;
    
    ASSERT(t >= buffer && t < buffer + num);
    
    pos = (unsigned) (t - buffer);
    if (pos == num - 1)
    {
        // last elem
        return NULL;
    }
    
    return &Get(pos + 1);
}

template<typename T, unsigned size>
T* ArrayList<T, size>::Prev(T* t)
{
    unsigned    pos;
    
    ASSERT(t >= buffer && t < buffer + num);
    
    pos = t - buffer;
    if (pos == 0)
    {
        // first elem
        return NULL;
    }
    
    return &Get(pos - 1);
}

template<typename T, unsigned size>
bool ArrayList<T, size>::Contains(T& t)
{
    T* it;
    
    FOREACH(it, *this)
    {
        if (*it == t)
            return true;
    }
    
    return false;
}

#endif
