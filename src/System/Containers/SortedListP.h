#ifndef SORTEDLISTP_H
#define SORTEDLISTP_H

#include "ListP.h"
#include "System/Common.h"

/*
===============================================================================

 SortedListP is a generic doubly-linked sorted list assuming pre-defined
 next and prev pointers.
 To use it define LessThan(const T &a, const T &b).

===============================================================================
*/

template<class T>
class SortedListP
{
public:

	bool			Add(T* t);
	T*				Remove(T* t);	
	bool			Remove(T &t);
	void			Clear();
	
	T*				Head() const;
	T*				Tail() const;
	int				GetLength() const;
	
	T*				Next(T* t) const;
	T*				Prev(T* t) const;

protected:
	ListP<T>		list;
};

/*
===============================================================================
*/

template<class T>
bool SortedListP<T>::Add(T* t)
{
	T** curr = &(list.head);

	while(true)
	{
		assert(*curr != t); // it's already linked

		if (*curr == NULL || LessThan(*t, **curr))
		{
			t->next = *curr;
			if (curr != &(list.head))
				t->prev = (T*) ((char*)curr - offsetof(T, next));
			else
				t->prev = NULL;
			if (*curr != NULL)
				(*curr)->prev = t;
			if (*curr == NULL)
				list.tail = t;
			*curr = t;
			list.length++;		
			return true;
		} 
		curr = &(*curr)->next;
	}
	
	ASSERT_FAIL();
}

template<class T>
T* SortedListP<T>::Remove(T* t)
{
	return list.Remove(t);
}

template<class T>
bool SortedListP<T>::Remove(T &t)
{
	return list.Remove(t);
}

template<class T>
void SortedListP<T>::Clear()
{
	return list.Clear();
}

template<class T>
T* SortedListP<T>::Head() const
{
	return list.Head();
}

template<class T>
T* SortedListP<T>::Tail() const
{
	return list.Tail();
}

template<class T>
int SortedListP<T>::GetLength() const
{
	return list.GetLength();
}

template<class T>
T* SortedListP<T>::Next(T* t) const
{
	return list.Next(t);
}

template<class T>
T* SortedListP<T>::Prev(T* t) const
{
	return list.Prev(t);
}

#endif
