#ifndef CALLABLE_H
#define CALLABLE_H

#include "System/Common.h"
#include <stdlib.h>

#define MFUNC(T, Func) MFunc<T, &T::Func>(this)

class Callable
{
public:
	Callable() { func = NULL; arg = NULL; }
	
	void Execute() const { if (!func) ASSERT_FAIL(); func(arg); }

protected:
	Callable(void (*func)(void*), void* arg) : func(func), arg(arg) {}

private:
	void	(*func)(void*);
	void	*arg;
};


class CFunc : public Callable
{
public:
	CFunc(void (*func)(void)) : Callable(Thunk, (void*) func) {}

private:
	static void Thunk(void* arg)
	{
		void (*cfunc)(void) = (void (*)(void)) arg;
		cfunc();
	}
};

template<class Type, void (Type::*Member)()>
class MFunc : public Callable
{
public:
	MFunc(Type* obj) : Callable(Thunk, obj) {}

private:
	static void Thunk(void* arg)
	{
		Type* obj = (Type*) arg;
		(obj->*Member)();
	}
};

inline void Call(const Callable& callable)
{
	callable.Execute();
}

#endif
