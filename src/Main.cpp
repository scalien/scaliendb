#include <stdio.h>
#include "System/Containers/PriorityQueue.h"

struct S
{
	S(int num_) { num = num_; }
	int		num;
	S*		next;
};

int main(void)
{
	PriorityQueue<S, &S::next> pq;
	S* s;
	
	pq.Enqueue(new S(100));
	pq.Enqueue(new S(101));
	pq.Enqueue(new S(102));
	pq.EnqueuePriority(new S(0));
	pq.EnqueuePriority(new S(1));
	pq.EnqueuePriority(new S(2));
	pq.Enqueue(new S(103));
	pq.EnqueuePriority(new S(3));
	
	
	while (s = pq.Dequeue())
	{
		printf("len: %d\n", pq.Length());
		printf("%d\n", s->num);
		delete s;
	}
}
