#ifndef CALLABLE_H
#define CALLABLE_H

/*
 * Callable is a function that you can Call() on
 */
 
class Callable
{
public:
	virtual ~Callable() {}
    virtual void Execute() = 0;
};

/*
 * MFunc is for creating Callables from class member functions
 * like MFunc<C>(&obj, &obj->func)
 */
 
template<class T>
class MFunc : public Callable
{
public:
	typedef void (T::*Callback)();
		
	MFunc(T* object_, Callback callback_)
	{
		object = object_;
		callback = callback_;
	}
	
	void Execute()
	{
		(object->*callback)();
	}
	
private:
	T*			object;
	Callback	callback;
};

/*
 * CFunc is for creating Callables from plain old C functions
 */

class CFunc : public Callable
{
public:
	typedef void (*Callback)();
		
	CFunc(Callback callback_)
	{
		callback = callback_;
	}
	
	CFunc()
	{
		callback = 0;
	}
	
	void Execute()
	{
		if (callback)
			(*callback)();
	}
private:
	Callback	callback;
};

inline void Call(Callable* callable)
{
	if (callable)
		callable->Execute();
}

#endif
