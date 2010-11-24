#!/bin/sh

CRASHLOGFILE="/tmp/crash.log"
LOGFILE="/tmp/crash-last.log"
PREVLOGFILE="/tmp/crash-prev.log"
MAX_ERROR=1
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
	ulimit -c unlimited
	echo > $CRASHLOGFILE
	while (:); do
                mv -f $LOGFILE $PREVLOGFILE > /dev/null 2>&1
		log "Starting '$*'"
        	$* > $LOGFILE 2>&1
                EXITSTATUS=$?
                if [ "$EXITSTATUS" = "250" ]; then
                        log "Assert fail"
                        export ERRORCOUNT=`expr $ERRORCOUNT + 1`
                        if [ $ERRORCOUNT -gt $MAX_ERROR ]; then
                        	log "Exceeded error count $MAX_ERROR"
                        	safe_exit 1
                        fi
                else
                	rm core.*
                fi
                
                if [ "$EXITSTATUS" = "1" ]; then
                	log "Test failed, exiting"
                	safe_exit 1
                fi
                log "Program exited with status $EXITSTATUS"
	done
}

infinite_loop $* & 
