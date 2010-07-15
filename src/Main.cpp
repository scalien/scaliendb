#include <stdio.h>
#include "System/Buffers/BufferPool.h"

struct S
{
	S(int num_) { num = num_; }
	
	bool operator==(const S &other) { return num == other.num; }
	int		num;
	
	S*		next;
	S*		prev;
};

int main(void)
{
	Buffer* buf;
	
	buf = DEFAULT_BUFFERPOOL->Acquire(100);
	buf->Write("hello\n");
	printf("%.*s", P(buf));
	DEFAULT_BUFFERPOOL->Release(buf);
	buf = DEFAULT_BUFFERPOOL->Acquire(100);
	buf->Write("1234\n");
	printf("%.*s", P(buf));
	printf("available: %u\n", DEFAULT_BUFFERPOOL->GetAvailableSize());
	DEFAULT_BUFFERPOOL->Release(buf);
	printf("available: %u\n", DEFAULT_BUFFERPOOL->GetAvailableSize());
}
