#ifndef PRIORITYQUEUE_H
#define PRIORITYQUEUE_H

#include <assert.h>

/*
===============================================================================

 PriorityQueue for storing objects with pre-allocated next pointer.

===============================================================================
*/

template<class T, T* T::*pnext>
class PriorityQueue
{
public:
	PriorityQueue();

	void	Enqueue(T* elem);	
	void	EnqueuePriority(T* elem);	
	T*		Dequeue();

	void	Clear();
	
	T*		Head() const;
	T*		Tail() const;
	int		Length() const;
	
	T*		Next(T* t) const;

private:
	T*		head;
	T*		tail;
	T*		prio;
	int		length;
};

/*
===============================================================================
*/

template<class T, T* T::*pnext>
PriorityQueue<T, pnext>::PriorityQueue()
{
	length = 0;
	head = NULL;
	tail = NULL;
	prio = NULL;
	Clear();
}


template<class T, T* T::*pnext>
void PriorityQueue<T, pnext>::Enqueue(T* elem)
{
	assert(elem != NULL);
	
	elem->*pnext = NULL;
	if (tail)
		tail->*pnext = elem;
	else
		head = elem;
	tail = elem;
	length++;
}

template<class T, T* T::*pnext>
void PriorityQueue<T, pnext>::EnqueuePriority(T* elem)
{
	assert(elem != NULL);
	
	elem->*pnext = NULL;
	if (prio)
	{
		elem->*pnext = prio->*pnext;
		prio->*pnext = elem;
	}
	else
	{
		elem->*pnext = head;
		head = elem;
	}
	if (prio == tail)
		tail = elem;
	prio = elem;
	length++;
}

template<class T, T* T::*pnext>
T* PriorityQueue<T, pnext>::Dequeue()
{
	T* elem;
	elem = head;
	if (elem)
	{
		head = elem->*pnext;
		if (tail == elem)
			tail = NULL;
		if (prio == elem)
			prio = NULL;
		elem->*pnext = NULL;
		length--;
	}
	return elem;
}

template<class T, T* T::*pnext>
void PriorityQueue<T, pnext>::Clear()
{
	T* elem;

	do
	{
		elem = Dequeue();
	} while (elem);
}

template<class T, T* T::*pnext>
T* PriorityQueue<T, pnext>::Head() const
{
	return head;
}

template<class T, T* T::*pnext>
T* PriorityQueue<T, pnext>::Tail() const
{
	return tail;
}

template<class T, T* T::*pnext>
T* PriorityQueue<T, pnext>::Next(T* t) const
{
	return t->next;
}

template<class T, T* T::*pnext>
int PriorityQueue<T, pnext>::Length() const
{
	return length;
}

#endif
