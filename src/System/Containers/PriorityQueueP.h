#ifndef PRIORITYQUEUE_H
#define PRIORITYQUEUE_H

#include <assert.h>

/*
===============================================================================

 PriorityQueue for storing objects with pre-allocated next pointer.

===============================================================================
*/

template<class T>
class PriorityQueueP
{
public:
	PriorityQueueP();

	void	Enqueue(T* elem);	
	void	EnqueuePriority(T* elem);	
	T*		Dequeue();

	void	Clear();
	
	T*		Head() const;
	T*		Tail() const;
	int		GetLength() const;
	
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

template<class T>
PriorityQueueP<T>::PriorityQueueP()
{
	length = 0;
	head = NULL;
	tail = NULL;
	prio = NULL;
	Clear();
}


template<class T>
void PriorityQueueP<T>::Enqueue(T* elem)
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
void PriorityQueueP<T>::EnqueuePriority(T* elem)
{
	assert(elem != NULL);
	
	elem->next = NULL;
	if (prio)
	{
		elem->next = prio->next;
		prio->next = elem;
	}
	else
	{
		elem->next = head;
		head = elem;
	}
	if (prio == tail)
		tail = elem;
	prio = elem;
	length++;
}

template<class T>
T* PriorityQueueP<T>::Dequeue()
{
	T* elem;
	elem = head;
	if (elem)
	{
		head = dynamic_cast<T*>(elem->next);
		if (tail == elem)
			tail = NULL;
		if (prio == elem)
			prio = NULL;
		elem->next = NULL;
		length--;
	}
	return elem;
}

template<class T>
void PriorityQueueP<T>::Clear()
{
	T* elem;

	do
	{
		elem = Dequeue();
	} while (elem);
}

template<class T>
T* PriorityQueueP<T>::Head() const
{
	return head;
}

template<class T>
T* PriorityQueueP<T>::Tail() const
{
	return tail;
}

template<class T>
T* PriorityQueueP<T>::Next(T* t) const
{
	return dynamic_cast<T*>(t->next);
}

template<class T>
int PriorityQueueP<T>::GetLength() const
{
	return length;
}

#endif
