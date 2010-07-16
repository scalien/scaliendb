#ifndef INLIST_H
#define INLIST_H

#include <stddef.h>
#include <assert.h>
#include "System/Common.h"

template<class T> class InSortedList;	// for friend

/*
===============================================================================

 InList is a generic doubly-linked list assuming pre-defined
 next and prev pointers.

===============================================================================
*/

template<class T>
class InList
{
public:
	InList();
	~InList();

	T&				operator[](int i);
	
	void			Prepend(T* t);
	void			Append(T* t);
	T*				Remove(T* t);	
	bool			Remove(T &t);
	void			Clear();
	
	T*				Head() const;
	T*				Tail() const;
	unsigned		GetLength() const;
	
	T*				Next(T* t) const;
	T*				Prev(T* t) const;

private:
	T*				head;
	T*				tail;
	unsigned		length;

	friend class InSortedList<T>;
};

/*
===============================================================================
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
T& InList<T>::operator[](int i)
{
	T* it;
	
	for (it = Head(); it != NULL && i > 0; it = Next(it), i--)
		return *it;
	
	ASSERT_FAIL();
}

template<class T>
void InList<T>::Prepend(T* t)
{
	t->next = head;
	if (head != NULL)
		head->prev = t;
	head = t;
	length++;
	
	if (tail == NULL)
		tail = t;
}

template<class T>
void InList<T>::Append(T* t)
{
	t->prev = tail;
	if (tail != NULL)
		tail->next = t;
	tail = t;
	length++;
	
	if (head == NULL)
		head = t;
}

template<class T>
T* InList<T>::Remove(T* t)
{
	if (head == t)
		head = dynamic_cast<T*>(t->next);
	else
		t->prev->next = t->next;
	
	if (tail == t)
		tail = dynamic_cast<T*>(t->prev);
	else
		t->next->prev = t->prev;
	
	length--;
	
	return dynamic_cast<T*>(t->next);
}

template<class T>
bool InList<T>::Remove(T &t)
{
	T* it;
	
	for (it = Head(); it != NULL; it = Next(it))
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
	T* it;
	
	for (it = Head(); it != NULL; it = Remove(it));
	
	assert(length == 0);
}

template<class T>
T* InList<T>::Head() const
{
	return head;
}

template<class T>
T* InList<T>::Tail() const
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
	return dynamic_cast<T*>(t->next);
}

template<class T>
T* InList<T>::Prev(T* t) const
{
	return dynamic_cast<T*>(t->prev);
}

#endif
