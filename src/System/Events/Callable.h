#ifndef CALLABLE_H
#define CALLABLE_H

#include "System/Common.h"
#include <stdlib.h>

#define MFUNC(T, Func) MFunc<T, &T::Func>(this)

/*
===============================================================================

 Callable

===============================================================================
*/

class Callable
{
public:
	Callable()			{ func = NULL; arg = NULL; }
	
	void Execute()		{ if (!func) return; func(arg); }
	bool IsSet()		{ return func != NULL; }

protected:
	Callable(void (*func)(void*), void* arg) : func(func), arg(arg) {}

private:
	void	(*func)(void*);
	void	*arg;
};

/*
===============================================================================

 CFunc

===============================================================================
*/

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

/*
===============================================================================

 MFunc

===============================================================================
*/

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

inline void Call(Callable& callable)
{
	callable.Execute();
}

#endif
