#ifndef INSORTEDLIST_H
#define INSORTEDLIST_H

#include "InList.h"
#include "System/Common.h"

/*
===============================================================================

 InSortedList is a generic doubly-linked sorted list assuming pre-defined
 next and prev pointers.
 To use it define LessThan(const T &a, const T &b).

===============================================================================
*/

template<class T>
class InSortedList
{
public:

	bool			Add(T* t);
	T*				Delete(T* t);
	T*				Remove(T* t);
	bool			Remove(T &t);
	void			Clear();
	
	T*				Head() const;
	T*				Tail() const;
	int				GetLength() const;
	
	T*				Next(T* t) const;
	T*				Prev(T* t) const;

protected:
	InList<T>		list;
};

/*
===============================================================================
*/

template<class T>
bool InSortedList<T>::Add(T* t)
{
	T*	curr = list.head;

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
				list.head = t;
				t->prev = NULL;
			}
			if (curr != NULL)
				curr->prev = t;
			if (curr == NULL)
				list.tail = t;
			curr = t;
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
T* InSortedList<T>::Head() const
{
	return list.Head();
}

template<class T>
T* InSortedList<T>::Tail() const
{
	return list.Tail();
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

#endif
