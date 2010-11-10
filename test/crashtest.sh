#!/bin/sh

CRASHLOGFILE="/tmp/crash.log"
LOGFILE="/tmp/crash-last.log"

infinite_loop()
{
	while (:); do
                $* > $LOGFILE
                if [ "$?" = "6" ]; then
                        DATE=`date`
                        echo "Assert fail at $DATE" >> $CRASHLOGFILE
                fi
	done
}

infinite_loop & 
