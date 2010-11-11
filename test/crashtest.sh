#!/bin/sh

CRASHLOGFILE="/tmp/crash.log"
LOGFILE="/tmp/crash-last.log"
MAX_ERROR=20
ERRORCOUNT=1

log()
{
	DATE=`date "+%F %T"`
	echo "$DATE: ($ERRORCOUNT) $*" >> $CRASHLOGFILE
}

safe_exit()
{
        if [ "$CRASHTEST_ATEXIT" != "" ]; then
                $CRASHTEST_ATEXIT
        fi
        exit $1
}

infinite_loop()
{
	while (:); do
		log "Starting '$*'"
        	$* > $LOGFILE
                EXITSTATUS=$?
                if [ "$EXITSTATUS" = "250" ]; then
                        log "Assert fail"
                        export ERRORCOUNT=`expr $ERRORCOUNT + 1`
                        if [ $ERRORCOUNT -gt $MAX_ERROR ]; then
                        	safe_exit 1
                        fi
                fi
                if [ "$EXITSTATUS" = "1" ]; then
                	log "Test failed, exiting"
                	safe_exit 1
                fi
                log "Program exited with status $EXITSTATUS"
	done
}

infinite_loop $* & 
