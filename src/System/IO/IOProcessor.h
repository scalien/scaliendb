#ifndef IOPROCESSOR_H
#define IOPROCESSOR_H

#include "IOOperation.h"

class Callable;

class IOProcessor
{
public:
	static bool Init(int maxfd);
	static void Shutdown();

	static bool Add(IOOperation* ioop);
	static bool Remove(IOOperation* ioop);

	static bool Poll(int sleep);

	static bool Complete(Callable* callable);
};

#endif
