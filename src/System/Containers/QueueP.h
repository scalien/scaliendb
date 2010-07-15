#ifndef QUEUE_H
#define QUEUE_H

#include "Common.h"

/*
===============================================================================

 Queue for storing objects with pre-allocated next pointer.

===============================================================================
*/

template<class T>
class QueueP
{
public:
	QueueP();

	void	Enqueue(T* elem);	
	T*		Dequeue();

	void	Clear();
	
	T*		Head() const;	
	T*		Tail() const;
	int		GetLength() const;
	
	T*		Next(T* t) const;

private:
	T*		head;
	T*		tail;
	int		length;
};

/*
===============================================================================
*/

template<class T>
QueueP<T>::QueueP()
{
	length = 0;
	head = NULL;
	tail = NULL;
	Clear();
}


template<class T>
void QueueP<T>::Enqueue(T* elem)
{
	assert(elem != NULL);
	
	elem->next = NULL;
	if (tail)
		tail->next = elem;
	else
		head = elem;
	tail = elem;
	length++;
}

template<class T>
T* QueueP<T>::Dequeue()
{
	T* elem;
	elem = head;
	if (elem)
	{
		head = dynamic_cast<T*>(elem->next);
		if (tail == elem)
			tail = NULL;
		elem->next = NULL;
		length--;
	}
	return elem;
}

template<class T>
void QueueP<T>::Clear()
{
	T* elem;

	do elem = Dequeue();
	while (elem);
}

template<class T>
T* QueueP<T>::Head() const
{
	return head;
}

template<class T>
T* QueueP<T>::Tail() const
{
	return tail;
}

template<class T>
T* QueueP<T>::Next(T* t) const
{
	return dynamic_cast<T*>(t->next);
}

template<class T>
int QueueP<T>::GetLength() const
{
	return length;
}

#endif
