#ifndef SDBPCLIENTCONSTS_H
#define SDBPCLIENTCONSTS_H

#define SDBP_SUCCESS            0
#define SDBP_API_ERROR          -1

//
// TRANSPORT STATUS
//

// only a portion of the commands could be sent before a timeout occured
#define SDBP_PARTIAL            -101
// no commands could be sent
#define SDBP_FAILURE            -102

//
// CONNECTIVITY STATUS
//

// some controllers were reachable, but there was no master or it went down
#define SDBP_NOMASTER           -201
// the entire grid was unreachable within timeouts
#define SDBP_NOCONNECTION       -202
// the primary could not be found
#define SDBP_NOPRIMARY          -203

//
// TIMEOUT STATUS
//

// the master could not be found within the master timeout
#define SDBP_MASTER_TIMEOUT     -301
// the blocking client library call returned because the global timeout has expired
#define SDBP_GLOBAL_TIMEOUT     -302
// the primary could not be found within the primary timeout
#define SDBP_PRIMARY_TIMEOUT    -303

//
// COMMAND STATUS
//

// the command was not executed
#define SDBP_NOSERVICE          -401
// the command was executed, but its return value was FAILED
#define SDBP_FAILED             -402
// the command was not executed, because of bad schema specification
#define SDBP_BADSCHEMA          -403

#define SDBP_DEFAULT_TIMEOUT    120*1000 // msec


#endif
