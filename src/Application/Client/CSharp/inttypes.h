#ifndef INTTYPES_H
#define INTTYPES_H

// Compatibility header file for Visual C++

#include <stdint.h>

typedef struct 
{
    intmax_t quot, rem;
} imaxdiv_t;

inline intmax_t imaxabs(intmax_t i)
{
	if (i < 0)
		return -i;

	return i;
}

inline imaxdiv_t imaxdiv(intmax_t numer, intmax_t denom)
{
	imaxdiv_t retval;

	retval.quot = numer / denom;
	retval.rem = numer % denom;
	
	if (numer >= 0 && retval.rem < 0) 
	{
		retval.quot++;
		retval.rem -= denom;
	}

	return (retval);
}

#define strtoimax _strtoi64
#define strtoumax _strtoui64



#endif
