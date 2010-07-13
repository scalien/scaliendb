#ifndef LISTP_H
#define LISTP_H

#include <stddef.h>
#include <assert.h>
#include "System/Common.h"

/*
===============================================================================

 List is a generic singly-linked list assuming pre-defined
 next and prev pointers.

===============================================================================
*/

template<class T>
class ListP
{
public:
	ListP();
	~ListP();

	T&				operator[](int i);
	
	void			Prepend(T* t);
	void			Append(T* t);
	T*				Remove(T* t);	
	bool			Remove(T &t);
	void			Clear();
	
	T*				Head() const;
	T*				Tail() const;
	unsigned		Length() const;
	
	T*				Next(T* t) const;
	T*				Prev(T* t) const;

private:
	T*				head;
	T*				tail;
	unsigned		length;
};

/*
===============================================================================
*/

template<class T>
ListP<T>::ListP()
{
	head = NULL;
	tail = NULL;
	length = 0;
}

template<class T>
ListP<T>::~ListP()
{
	Clear();
}

template<class T>
T& ListP<T>::operator[](int i)
{
	T* it;
	
	for (it = Head(); it != NULL && i > 0; it = Next(it), i--)
		return *it;
	
	ASSERT_FAIL();
}

template<class T>
void ListP<T>::Prepend(T* t)
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
void ListP<T>::Append(T* t)
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
T* ListP<T>::Remove(T* t)
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
bool ListP<T>::Remove(T &t)
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
void ListP<T>::Clear()
{
	T* it;
	
	for (it = Head(); it != NULL; it = Remove(it));
	
	assert(length == 0);
}

template<class T>
T* ListP<T>::Head() const
{
	return head;
}

template<class T>
T* ListP<T>::Tail() const
{
	return tail;
}

template<class T>
unsigned ListP<T>::Length() const
{
	return length;
}

template<class T>
T* ListP<T>::Next(T* t) const
{
	return dynamic_cast<T*>(t->next);
}

template<class T>
T* ListP<T>::Prev(T* t) const
{
	return dynamic_cast<T*>(t->prev);
}

#endif
