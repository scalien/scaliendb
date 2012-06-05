#ifndef DATABASECONSTS_H
#define DATABASECONSTS_H

#include "System/Common.h"

#define DATABASE_NAME_SIZE                      (  1*KiB)
#define DATABASE_KEY_SIZE                       (  1*KiB)
#define DATABASE_VAL_SIZE                       ( 16*MiB)

#define DATABASE_REPLICATION_SIZE               (   1*MB)

#define HEARTBEAT_EXPIRE_TIME                   ( 3*1000) // msec
#define ACTIVATION_TIMEOUT                      (15*1000) // msec
#define RLOG_REACTIVATION_DIFF                  (    100) // paxos rounds of difference
#define SPLIT_COOLDOWN_TIME                     ( 3*1000) // msec

#endif
