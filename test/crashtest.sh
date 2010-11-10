#!/bin/sh

CRASHLOGFILE="/tmp/crash.log"
LOGFILE="/tmp/crash-last.log"

log()
{
	DATE=`date "+%F %T"`
	echo "$DATE: $*"
}

infinite_loop()
{
	while (:); do
        	$* > $LOGFILE
                EXITSTATUS=$?
                if [ "$EXITSTATUS" = "6" ]; then
                        log "Assert fail" >> $CRASHLOGFILE
                fi
                if [ "$EXITSTATUS" = "1" ]; then
                	log "Test failed, exiting" >> $CRASHLOGFILE
                	exit 1
                fi
                log "Program exited with status $EXITSTATUS" >> $CRASHLOGFILE
	done
}

infinite_loop $* & 
