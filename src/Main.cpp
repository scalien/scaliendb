#include <stdio.h>
#include "System/Containers/SortedListP.h"

struct S
{
	S(int num_) { num = num_; }
	
	bool operator==(const S &other) { return num == other.num; }
	int		num;
	
	S*		next;
	S*		prev;
};

bool LessThan(const S& a, const S& b)
{
	return a.num < b.num;
}

int main(void)
{
	SortedListP<S> list;
	
	list.Add(new S(2));
	list.Add(new S(1));
	list.Add(new S(3));
	list.Add(new S(0));

	printf("%d\n\n", list.GetLength());
	
	for (S* s = list.Head(); s != NULL; s = list.Next(s))
	{
		printf("%d\n", s->num);
	}
}
