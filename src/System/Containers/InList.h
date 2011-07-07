#ifndef INLIST_H
#define INLIST_H

#include <stddef.h>
#include "System/Common.h"
#include "System/Macros.h"

template<class T> class InSortedList;   // for friend

/*
===============================================================================================

 InList is a generic doubly-linked list assuming pre-defined
 next and prev pointers.

===============================================================================================
*/

template<class T>
class InList
{
    friend class InSortedList<T>;

public:
    InList();
    ~InList();

    T*              Get(int i);
    
    void            Prepend(T* t);
    void            PrependList(InList<T>& list);
    void            Append(T* t);
    void            InsertAfter(T* before, T* t);

    T*              Delete(T* t);
    T*              Pop();
    T*              Remove(T* t);   
    bool            Remove(T &t);
    void            Clear();
    void            ClearMembers();
    void            DeleteList();
    
    T*              First() const;
    T*              Last() const;
    unsigned        GetLength() const;
    
    T*              Next(T* t) const;
    T*              Prev(T* t) const;

    bool            Contains(T* t);

private:
    T*              head;
    T*              tail;
    unsigned        length;
};

/*
===============================================================================================
*/

template<class T>
InList<T>::InList()
{
    head = NULL;
    tail = NULL;
    length = 0;
}

template<class T>
InList<T>::~InList()
{
    Clear();
}

template<class T>
T* InList<T>::Get(int i)
{
    T* it;
    
    for (it = First(); it != NULL; it = Next(it))
    {
        if (i == 0)
            return it;
        i--;
    }
    
    
    ASSERT_FAIL();
    return NULL;
}

template<class T>
void InList<T>::Prepend(T* t)
{
    ASSERT(t->next == t && t->prev == t);

    t->next = head;
    t->prev = NULL;
    if (head != NULL)
        head->prev = t;
    head = t;
    length++;
    
    if (tail == NULL)
        tail = t;
}

template<class T>
void InList<T>::PrependList(InList<T>& list)
{
    if (list.length == 0)
        return;
    
    if (length == 0)
    {
        *this = list;
        list.ClearMembers();
        return;
    }

    list.tail->next = head;
    head->prev = list.tail;
    head = list.head;
    list.ClearMembers();
}

template<class T>
void InList<T>::Append(T* t)
{
    ASSERT(t->next == t && t->prev == t);

    t->prev = tail;
    t->next = NULL;
    if (tail != NULL)
        tail->next = t;
    tail = t;
    length++;
    
    if (head == NULL)
        head = t;
}

template<class T>
void InList<T>::InsertAfter(T* before, T* t)
{
    ASSERT(t->next == t && t->prev == t);

    length++;
    
    if (before == NULL)
    {
        t->prev = NULL;
        t->next = head;
        head = t;
        if (tail == NULL)
            tail = t;
        return;
    }
    
    t->prev = before;
    t->next = before->next;
    if (before->next != NULL)
        before->next->prev = t;
    before->next = t;
    if (tail == before)
        tail = t;
}


template<class T>
T* InList<T>::Delete(T* t)
{
    T* next;
    next = Remove(t);
    delete t;
    return next;
}

template<class T>
T* InList<T>::Pop()
{
    T* h;
    
    if (length == 0)
        return NULL;
    
    h = head;
    Remove(h);
    return h;
}

template<class T>
T* InList<T>::Remove(T* t)
{
    T*  ret;
    
    ASSERT(t->next != t || t->prev != t);
        
    if (head == t)
        head = static_cast<T*>(t->next);
    else if (t->prev != NULL)
        t->prev->next = t->next;    
    
    if (tail == t)
        tail = static_cast<T*>(t->prev);
    else if (t->next != NULL)
        t->next->prev = t->prev;
    
    length--;

    ret = static_cast<T*>(t->next);
    t->next = t;
    t->prev = t;
//  Log_Trace("length = %d", length);
    
    return ret;
}

template<class T>
bool InList<T>::Remove(T &t)
{
    T* it;
    
    for (it = First(); it != NULL; it = Next(it))
    {
        if (*it == t)
        {
            Remove(it);
            return true;
        }
    }
    
    // not found
    return false;
}

template<class T>
void InList<T>::Clear()
{
    T*  it;
    
    for (it = First(); it != NULL; it = Remove(it)) { /* empty */ }
    
    ASSERT(length == 0);
}

template<class T>
void InList<T>::ClearMembers()
{
    head = NULL;
    tail = NULL;
    length = 0;
}

template<class T>
void InList<T>::DeleteList()
{
    T*  it;
    
    for (it = First(); it != NULL; it = Delete(it)) { /* empty */ }
    
    ASSERT(length == 0);
}

template<class T>
T* InList<T>::First() const
{
    return head;
}

template<class T>
T* InList<T>::Last() const
{
    return tail;
}

template<class T>
unsigned InList<T>::GetLength() const
{
    return length;
}

template<class T>
T* InList<T>::Next(T* t) const
{
    return static_cast<T*>(t->next);
}

template<class T>
T* InList<T>::Prev(T* t) const
{
    return static_cast<T*>(t->prev);
}

template<class T>
bool InList<T>::Contains(T* t)
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
