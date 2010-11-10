#!/bin/sh

CRASHLOGFILE="/tmp/crash.log"
LOGFILE="/tmp/crash-last.log"

infinite_loop()
{
	while (:); do
		echo $* 
                exec $*
                EXITSTATUS=$?
                if [ "$EXITSTATUS" = "6" ]; then
                        DATE=`date`
                        echo "Assert fail at $DATE" >> $CRASHLOGFILE
                fi
                if [ "$EXITSTATUS" = "1" ]; then
                	echo "Test failed" >> $CRASHLOGFILE
                	exit 1
                fi
	done
}

infinite_loop $* & 
