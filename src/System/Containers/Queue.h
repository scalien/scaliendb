#ifndef QUEUE_H
#define QUEUE_H

/*
 * Queue for storing objects with pre-allocated next pointer
 */

template<class T, T* T::*pnext>
class Queue
{
public:

	Queue();

	void	Enqueue(T* elem);	
	T*		Dequeue();

	void	Clear();
	
	T*		Head() const;	
	T*		Tail() const;
	int		Length() const;
	
	T*		Next(T* t) const;
private:
	T*		head;
	T*		tail;
	int		length;
};

/****************************************************************************/

template<class T, T* T::*pnext>
Queue<T, pnext>::Queue()
{
	length = 0;
	head = 0;
	tail = 0;
	Clear();
}


template<class T, T* T::*pnext>
void Queue<T, pnext>::Enqueue(T* elem)
{
	assert(elem != 0);
	
	elem->*pnext = 0;
	if (tail)
		tail->*pnext = elem;
	else
		head = elem;
	tail = elem;
	length++;
}

template<class T, T* T::*pnext>
T* Queue<T, pnext>::Dequeue()
{
	T* elem;
	elem = head;
	if (elem)
	{
		head = elem->*pnext;
		if (tail == elem)
			tail = 0;
		elem->*pnext = 0;
		length--;
	}
	return elem;
}

template<class T, T* T::*pnext>
void Queue<T, pnext>::Clear()
{
	T* elem;

	do
	{
		elem = Dequeue();
	} while (elem);
}

template<class T, T* T::*pnext>
T* Queue<T, pnext>::Head() const
{
	return head;
}

template<class T, T* T::*pnext>
T* Queue<T, pnext>::Tail() const
{
	return tail;
}

template<class T, T* T::*pnext>
T* Queue<T, pnext>::Next(T* t) const
{
	return t->next;
}

template<class T, T* T::*pnext>
int Queue<T, pnext>::Length() const
{
	return length;
}

#endif
