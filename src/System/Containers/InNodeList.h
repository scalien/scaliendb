#ifndef INNODELIST_H
#define INNODELIST_H

#include <stddef.h>
#include "System/Common.h"
#include "System/Macros.h"

/*
===============================================================================================

 InNodeList is a generic doubly-linked list assuming pre-defined nodes.

===============================================================================================
*/

template<typename T>
class InListNode
{
public:

    T*              prev;
    T*              next;

    InListNode(T* parent)
    {
        prev = next = parent;
    }
};

template<class T, InListNode<T> T::*pnode = &T::listNode>
class InNodeList
{
public:
    InNodeList();
    ~InNodeList();

    T*              Get(int i);
    
    void            Prepend(T* t);
    void            PrependList(InNodeList<T, pnode>& list);
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

template<class T, InListNode<T> T::*pnode>
InNodeList<T, pnode>::InNodeList()
{
    head = NULL;
    tail = NULL;
    length = 0;
}

template<class T, InListNode<T> T::*pnode>
InNodeList<T, pnode>::~InNodeList()
{
    Clear();
}

template<class T, InListNode<T> T::*pnode>
T* InNodeList<T, pnode>::Get(int i)
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

template<class T, InListNode<T> T::*pnode>
void InNodeList<T, pnode>::Prepend(T* t)
{
    ASSERT((t->*pnode).next == t && (t->*pnode).prev == t);

    (t->*pnode).next = head;
    (t->*pnode).prev = NULL;
    if (head != NULL)
        head->pnode.prev = t;
    head = t;
    length++;
    
    if (tail == NULL)
        tail = t;
}

template<class T, InListNode<T> T::*pnode>
void InNodeList<T, pnode>::PrependList(InNodeList<T, pnode>& list)
{
    if (list.length == 0)
        return;
    
    if (length == 0)
    {
        *this = list;
        list.ClearMembers();
        return;
    }

    list.tail->pnode.next = head;
    head->pnode.prev = list.tail;
    head = list.head;
    list.ClearMembers();
}

template<class T, InListNode<T> T::*pnode>
void InNodeList<T, pnode>::Append(T* t)
{
    ASSERT((t->*pnode).next == t && (t->*pnode).prev == t);

    (t->*pnode).prev = tail;
    (t->*pnode).next = NULL;
    if (tail != NULL)
        (tail->*pnode).next = t;
    tail = t;
    length++;
    
    if (head == NULL)
        head = t;
}

template<class T, InListNode<T> T::*pnode>
void InNodeList<T, pnode>::InsertAfter(T* before, T* t)
{
    ASSERT((t->*pnode).next == t && (t->*pnode).prev == t);

    length++;
    
    if (before == NULL)
    {
        (t->*pnode).prev = NULL;
        (t->*pnode).next = head;
        head = t;
        if (tail == NULL)
            tail = t;
        return;
    }
    
    (t->*pnode).prev = before;
    (t->*pnode).next = before->pnode.next;
    if (before->pnode.next != NULL)
        before->pnode.nex(t->*pnode).prev = t;
    before->pnode.next = t;
    if (tail == before)
        tail = t;
}


template<class T, InListNode<T> T::*pnode>
T* InNodeList<T, pnode>::Delete(T* t)
{
    T* next;

    next = Remove(t);
    delete t;
    return next;
}

template<class T, InListNode<T> T::*pnode>
T* InNodeList<T, pnode>::Pop()
{
    T* h;
    
    if (length == 0)
        return NULL;
    
    h = head;
    Remove(h);
    return h;
}

template<class T, InListNode<T> T::*pnode>
T* InNodeList<T, pnode>::Remove(T* t)
{
    T*  ret;
    
    ASSERT((t->*pnode).next != t && (t->*pnode).prev != t);
        
    if (head == t)
        head = static_cast<T*>((t->*pnode).next);
    else if ((t->*pnode).prev != NULL)
        ((t->*pnode).prev->*pnode).next = (t->*pnode).next;    
    
    if (tail == t)
        tail = static_cast<T*>((t->*pnode).prev);
    else if ((t->*pnode).next != NULL)
        ((t->*pnode).next->*pnode).prev = (t->*pnode).prev;
    
    length--;

    ret = static_cast<T*>((t->*pnode).next);
    (t->*pnode).next = t;
    (t->*pnode).prev = t;
    
    return ret;
}

template<class T, InListNode<T> T::*pnode>
bool InNodeList<T, pnode>::Remove(T &t)
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

template<class T, InListNode<T> T::*pnode>
void InNodeList<T, pnode>::Clear()
{
    T*  it;
    
    for (it = First(); it != NULL; it = Remove(it)) { /* empty */ }
    
    ASSERT(length == 0);
}

template<class T, InListNode<T> T::*pnode>
void InNodeList<T, pnode>::ClearMembers()
{
    head = NULL;
    tail = NULL;
    length = 0;
}

template<class T, InListNode<T> T::*pnode>
void InNodeList<T, pnode>::DeleteList()
{
    T*  it;
    
    for (it = First(); it != NULL; it = Delete(it)) { /* empty */ }
    
    ASSERT(length == 0);
}

template<class T, InListNode<T> T::*pnode>
T* InNodeList<T, pnode>::First() const
{
    return head;
}

template<class T, InListNode<T> T::*pnode>
T* InNodeList<T, pnode>::Last() const
{
    return tail;
}

template<class T, InListNode<T> T::*pnode>
unsigned InNodeList<T, pnode>::GetLength() const
{
    return length;
}

template<class T, InListNode<T> T::*pnode>
T* InNodeList<T, pnode>::Next(T* t) const
{
    return static_cast<T*>((t->*pnode).next);
}

template<class T, InListNode<T> T::*pnode>
T* InNodeList<T, pnode>::Prev(T* t) const
{
    return static_cast<T*>((t->*pnode).prev);
}

template<class T, InListNode<T> T::*pnode>
bool InNodeList<T, pnode>::Contains(T* t)
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
