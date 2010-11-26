#ifndef LIST_H
#define LIST_H

#include <stddef.h>
#include <assert.h>
#include "System/Common.h"

template<class T> struct ListNode;  // forward
template<class T> class SortedList; // for friend

/*
===============================================================================================

 List is a generic doubly-linked list.

===============================================================================================
*/

template<class T>
class List
{
    friend class SortedList<T>;

public:
    List();
    ~List();

    List<T>&       operator=(const List<T>& other);

    T&              Get(int i);
    
    void            Prepend(T &t);
    void            Append(T &t);
    T*              Remove(T* t);   
    bool            Remove(T &t);
    void            Copy(List<T>& other) const;
    void            Clear();
    void            ClearMembers();
    
    T*              First() const;
    T*              Last() const;
    unsigned        GetLength() const;
    
    T*              Next(T* t) const;
    T*              Prev(T* t) const;

private:
    ListNode<T>*    head;
    ListNode<T>*    tail;
    unsigned        length;
};

/*
===============================================================================================
*/

template<class T>
struct ListNode
{
    T               data;
    
    ListNode<T>*    next;
    ListNode<T>*    prev;
    
    void Init(T &data_, ListNode<T>* next_, ListNode<T>* prev_)
    {
        data = data_;
        next = next_;
        prev = prev_;
    }
};

template<class T>
List<T>::List()
{
    head = NULL;
    tail = NULL;
    length = 0;
}

template<class T>
List<T>::~List()
{
    Clear();
}

template<class T>
List<T>& List<T>::operator=(const List<T>& other)
{
    T* it;
    
    Clear();
    
    for (it = other.First(); it != NULL; it = other.Next(it))
        Append(*it);
    
    return *this;
}


template<class T>
T& List<T>::Get(int i)
{
    T* it;
    
    for (it = First(); it != NULL && i > 0; it = Next(it), i--)
        return *it;
    
    ASSERT_FAIL();
}

template<class T>
void List<T>::Prepend(T &t)
{
    ListNode<T>* node;
    
    node = new ListNode<T>;
    node->Init(t, head, NULL);
    
    if (head != NULL)
        head->prev = node;
    head = node;
    length++;
    
    if (tail == NULL)
        tail = node;
}

template<class T>
void List<T>::Append(T &t)
{
    ListNode<T>* node;
    
    node = new ListNode<T>;
    node->Init(t, NULL, tail);
    
    if (tail != NULL)
        tail->next = node;
    tail = node;
    length++;
    
    if (head == NULL)
        head = node;
}

template<class T>
T* List<T>::Remove(T* t)
{
    ListNode<T>* node;
    T* ret;
    
    node = (ListNode<T>*) t;
    if (head == node)
        head = node->next;
    else
        node->prev->next = node->next;
    
    if (tail == node)
        tail = node->prev;
    else
        node->next->prev = node->prev;
    
    length--;
    
    ret = NULL;
    if (node->next != NULL)
        ret = &node->next->data;
    
    delete node;
    return ret;
}

template<class T>
bool List<T>::Remove(T &t)
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
void List<T>::Copy(List<T>& other) const
{
    T* it;
    
    for (it = First(); it != NULL; it = Next(it))
        other.Append(*it);
}

template<class T>
void List<T>::Clear()
{
    ListNode<T> *t, *n;
    
    t = head;
    while (t)
    {
        n = t->next;
        delete t;
        t = n;
        length--;
    }
    
    head = tail = NULL;
    
    assert(length == 0);
}

template<class T>
void List<T>::ClearMembers()
{
    head = NULL;
    tail = NULL;
    length = 0;
}

template<class T>
T* List<T>::First() const
{
    if (head == NULL)
        return NULL;
    else
        return &head->data;
}

template<class T>
T* List<T>::Last() const
{
    if (tail == NULL)
        return NULL;
    else
        return &tail->data;
}

template<class T>
unsigned List<T>::GetLength() const
{
    return length;
}

template<class T>
T* List<T>::Next(T* t) const
{
    ListNode<T>* node = (ListNode<T>*) t;
    if (node->next == NULL)
        return NULL;
    else
        return &node->next->data;
}

template<class T>
T* List<T>::Prev(T* t) const
{
    ListNode<T>* node = (ListNode<T>*) t;
    if (node->prev == NULL)
        return NULL;
    else
        return &node->prev->data;
}

#endif
