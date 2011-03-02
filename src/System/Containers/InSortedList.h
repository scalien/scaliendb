#ifndef INSORTEDLIST_H
#define INSORTEDLIST_H

#include "InList.h"
#include "System/Common.h"

/*
===============================================================================================

 InSortedList is a generic doubly-linked sorted list assuming pre-defined
 next and prev pointers.
 To use it define LessThan(const T &a, const T &b).

===============================================================================================
*/

template<class T>
class InSortedList
{
public:

    bool            Add(T* t);
    T*              Delete(T* t);
    T*              Remove(T* t);
    bool            Remove(T &t);
    void            Clear();
    void            DeleteList();
    
    T*              First() const;
    T*              Last() const;
    int             GetLength() const;
    
    T*              Next(T* t) const;
    T*              Prev(T* t) const;

    bool            Contains(T* t);

protected:
    InList<T>       list;
};

/*
===============================================================================================
*/

template<class T>
bool InSortedList<T>::Add(T* t)
{
    T*  curr = list.head;

    while(true)
    {
        assert(curr != t); // it's already linked

        if (curr == NULL || LessThan(*t, *curr))
        {
            t->next = curr;
            if (curr != list.head)
            {
                if (curr == NULL)
                {
                    // end of list
                    list.tail->next = t;
                    t->prev = list.tail;
                }
                else
                {
                    curr->prev->next = t;
                    t->prev = curr->prev;
                }
            }
            else
            {
                // start of list
                list.head = t;
                t->prev = NULL;
            }
            if (curr != NULL)
                curr->prev = t;
            if (curr == NULL)
                list.tail = t;
            list.length++;
            return true;
        } 

        curr = curr->next;
    }
    
    ASSERT_FAIL();
}

template<class T>
T* InSortedList<T>::Delete(T* t)
{
    return list.Delete(t);
}

template<class T>
T* InSortedList<T>::Remove(T* t)
{
    return list.Remove(t);
}

template<class T>
bool InSortedList<T>::Remove(T &t)
{
    return list.Remove(t);
}

template<class T>
void InSortedList<T>::Clear()
{
    return list.Clear();
}

template<class T>
void InSortedList<T>::DeleteList()
{
    return list.DeleteList();
}

template<class T>
T* InSortedList<T>::First() const
{
    return list.First();
}

template<class T>
T* InSortedList<T>::Last() const
{
    return list.Last();
}

template<class T>
int InSortedList<T>::GetLength() const
{
    return list.GetLength();
}

template<class T>
T* InSortedList<T>::Next(T* t) const
{
    return list.Next(t);
}

template<class T>
T* InSortedList<T>::Prev(T* t) const
{
    return list.Prev(t);
}

template<class T>
bool InSortedList<T>::Contains(T* t)
{
    T* it;
    
    FOREACH(it, *this)
    {
        if (it == t)
            return true;
    }
    
    return false;
}

#endif
