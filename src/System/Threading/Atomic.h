#ifndef ATOMIC_H
#define ATOMIC_H

#include "System/Platform.h"

/*
===============================================================================================

 Atomic operations

===============================================================================================
*/

#ifdef PLATFORM_WINDOWS

/*
 * The variable pointed to by the parameter must be aligned on a 32-bit boundary; 
 * otherwise, this function will behave unpredictably on multiprocessor x86 systems and 
 * any non-x86 systems. See _aligned_malloc.
 *
 * There are specialized versions for unsigned 64-bit operations, because Windows doesn't
 * provide those by default.
 */

#include <Windows.h>

#define AtomicIncrement32(u32)      InterlockedIncrement(&(u32))
#define AtomicIncrement64(u64)      InterlockedIncrement64(&(u64))

#define AtomicDecrement32(u32)      InterlockedDecrement(&(u32))
#define AtomicDecrement64(u64)      InterlockedDecrement64(&(u64))

#define AtomicAdd32(u32, v32)       InterlockedAdd(&(u32), (v32))
// NOTE: there is no InterlockedAdd64 on Windows, therefore there is no AtomicAdd64.
// You can use AtomicExchange64(u, u + diff) instead.

#define AtomicExchange32(u32, v32)  InterlockedExchange(&(u32), (v32))
#define AtomicExchange64(u64, v64)  InterlockedExchange64(&(u64), (v64))

inline uint64_t AtomicIncrementU64(volatile uint64_t& target)
{
    return InterlockedIncrement64((volatile LONGLONG*) &target);
}

inline uint64_t AtomicDecrementU64(volatile uint64_t& target)
{
    return InterlockedDecrement64((volatile LONGLONG*) &target);
}

inline uint64_t AtomicExchangeU64(volatile uint64_t& target, uint64_t value)
{
    return InterlockedExchange64((volatile LONGLONG*) &target, (LONGLONG) value);
}

#else // PLATFORM_WINDOWS

/*
 * With GCC there are other caveats:
 * http://gcc.gnu.org/onlinedocs/gcc-4.1.2/gcc/Atomic-Builtins.html
 */

#define AtomicIncrement32(u32)      __sync_fetch_and_add(&(u32), 1)
#define AtomicIncrement64(u64)      __sync_fetch_and_add(&(u64), 1)

#define AtomicDecrement32(u32)      __sync_fetch_and_sub(&(u32), 1)
#define AtomicDecrement64(u64)      __sync_fetch_and_sub(&(u64), 1)

#define AtomicAdd32(u32, v32)       __sync_fetch_and_add(&(u32), (v32))
#define AtomicAdd64(u64, v64)       __sync_fetch_and_add(&(u64), (v64))

#define AtomicExchange32(u32, v32)  __sync_bool_compare_and_swap(&(u32), (u32), (v32))
#define AtomicExchange64(u64, v64)  __sync_bool_compare_and_swap(&(u64), (u64), (v64))

#define AtomicIncrementU32(u32)     __sync_fetch_and_add(&(u32), 1)
#define AtomicIncrementU64(u64)     __sync_fetch_and_add(&(u64), 1)

#define AtomicDecrementU32(u32)     __sync_fetch_and_sub(&(u32), 1)
#define AtomicDecrementU64(u64)     __sync_fetch_and_sub(&(u64), 1)

#define AtomicAddU32(u32, v32)      __sync_fetch_and_add(&(u32), (v32))
#define AtomicAddU64(u64, v64)      __sync_fetch_and_add(&(u64), (v64))

#define AtomicExchangeU32(u32, v32) __sync_bool_compare_and_swap(&(u32), (u32), (v32))
#define AtomicExchangeU64(u64, v64) __sync_bool_compare_and_swap(&(u64), (u64), (v64))

#endif // PLATFORM_WINDOWS

#endif
