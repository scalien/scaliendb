#include "StateProcessor.h"

	// A.
	// a shard is HEALTHY if
	// enough quorum members are assigned
	// all quorum members are up
	// primary lease is active

	// B.
	// never split a shard that is not HEALTHY

	// C.
	// never remove the last node from a quorum
	// => if a shard's quorum has zero members, that means it's a table's first
	// and has not been assigned yet

	// D. (do this later)
	// how to make sure there is only one PRIMARY
	// the MASTER sends a PRIMARY lease
	// the PRIMARY sends its PRIMARY lease along with the first message it replicates
	// all other nodes receive it and start timers
	// the other nodes will refuse to become masters as long as their timers are active

	// 1.
	// for each shard
	// if some quorum members are assigned
	// if all quorum members are up
	// and no primary lease is active
	// ACTION: give out lease to the primary
	// TRIGGER EVENT: last quorum member signs in
	// or
	// TRIGGER EVENT: previous lease should be renewed
	
	// 2.
	// for each shard
	// if no quorum members are assigned
	// look for the max. number of servers that are available,
	// no less than the replication target!
	// ACTION: and create the quorum
	// TRIGGER EVENT: new table is created
	// 1. will cause it to receive a PRIMARY lease
	
	// 3.
	// for each shard
	// if some quorum members are assigned
	// but one of them is not alive
	// ACTION: decrease quorum
	// TRIGGER EVENT: shard doesn't report in in N secods
	
	// 4. (do this later)
	// for each shard
	// if quorum members are assigned
	// but not enough to meet the replication target
	// and a shard server is reporting it has an almost-up-to-date version
	// and nobody is currently catching up on that shard
	// start process which joins the shard server into the quorum:
	// next time, give out the lease to the primary with the new quorum
	// then, if primary reports progress, add the shard server to the quorum "officially"
	// TRIGGER EVENT: shard server reports in
		
	// 5. (do this later)
	// if quorum members are assigned
	// but not enought to meet the replication target
	// and no shard server is reporting it has an almost-up-to-date version
	// then tell a new shard server to catch up
	
	// 6. (do this later)
	// if a shard server reports a shard
	// but the shard server is not part of the quorum
	// and the replication target is met, then tell it to delete the shard
	
	// 7. (do this later)
	// if a shard server reports a shard
	// but the shard server is not part of the quorum
	// and its versionID is behind, then tell it to delete the shard

	// 8. (rebalancing)
	// remove old one from quorum
	// tell it to delete the shard
	// 5. => will trigger
