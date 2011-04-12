#ifndef SORTEDLIST_H
#define SORTEDLIST_H

#include "List.h"
#include "System/Common.h"

/*
===============================================================================================

 SortedList is a generic doubly-linked sorted list.
 To use it define LessThan(const T &a, const T &b).

===============================================================================================
*/

template<class T>
class SortedList
{
public:

    bool            operator==(const SortedList<T>& other) const;
    bool            operator!=(const SortedList<T>& other) const;

    bool            Add(T t, bool unique = false);
    T*              Remove(T* t);   
    bool            Remove(T &t);
    void            Clear();
    
    T*              First() const;
    T*              Last() const;
    unsigned        GetLength() const;
    
    T*              Next(T* t) const;
    T*              Prev(T* t) const;

    bool            Contains(T& t);

protected:
    List<T>         list;
};

/*
===============================================================================================
*/

template<class T>
bool SortedList<T>::operator==(const SortedList<T>& other) const
{
    T* it1;
    T* it2;
    
    for (it1 = First(), it2 = other.First();
     it1 != NULL && it2 != NULL;
     it1 = Next(it1), it2 = other.Next(it2))
    {
        if (*it1 != *it2)
            return false;
    }
    
    return (it1 == NULL && it2 == NULL);
}

template<class T>
bool SortedList<T>::operator!=(const SortedList<T>& other) const
{
    return !operator==(other);
}

template<class T>
bool SortedList<T>::Add(T t, bool unique)
{
    ListNode<T>*  node;
    ListNode<T>** curr = &(list.head);

    while(true)
    {
        if (*curr == NULL || LessThan(t, (*curr)->data))
        {
            node = new ListNode<T>;
            node->data = t;
            node->next = *curr;
            if (curr != &(list.head))
                node->prev =
                 (ListNode<T>*) ((char*)curr - offsetof(ListNode<T>, next));
            else
                node->prev = NULL;
            if (*curr != NULL)
                (*curr)->prev = node;
            if (*curr == NULL)
                list.tail = node;
            *curr = node;
            list.length++;      
            return true;
        } 
        else
        {
            if (unique && (*curr)->data == t)
                return false;
            curr = &(*curr)->next;
        }
    }
    
    ASSERT_FAIL();
}

template<class T>
T* SortedList<T>::Remove(T* t)
{
    return list.Remove(t);
}

template<class T>
bool SortedList<T>::Remove(T &t)
{
    return list.Remove(t);
}

template<class T>
void SortedList<T>::Clear()
{
    return list.Clear();
}

template<class T>
T* SortedList<T>::First() const
{
    return list.First();
}

template<class T>
T* SortedList<T>::Last() const
{
    return list.Last();
}

template<class T>
unsigned SortedList<T>::GetLength() const
{
    return list.GetLength();
}

template<class T>
T* SortedList<T>::Next(T* t) const
{
    return list.Next(t);
}

template<class T>
T* SortedList<T>::Prev(T* t) const
{
    return list.Prev(t);
}

template<class T>
bool SortedList<T>::Contains(T& t)
{
    return list.Contains(t);
}

#endif
