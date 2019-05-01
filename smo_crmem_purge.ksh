#!/usr/bin/ksh

#########################################################################################################################
# This script will purge data in the S_CRMEM & S_CRMEM_ITEM tables
# It will call /web/local/orderlink/linkload/sql_ctl/smo_crmem_purge.sql which 
#   executes purge procedures in an ORACLE package called "PKG_SMO_CLEAN"
# The log file is located in /web/local/orderlink/linkload/logs/smo_crmem_purge.log
# This script was designed for a one time run.
#########################################################################################################################
PGMNAME=$0
DIRNAME=`dirname $0`

# set up environment for oracle based SMO database instance
. $DIRNAME/OrderingEnv.ksh

SVRNAME=`uname -n`

SCRIPTDIR=/web/local/orderlink/linkload/script
SQLDIR=/web/local/orderlink/linkload/sql_ctl
LOGDIR=/web/local/orderlink/linkload/logs

LOGFILE='smo_crmem_purge.log'
SQLFILE='smo_crmem_purge.sql'

##########################################################################################################
#
# Function                      :   timestamp
# Purpose                       :   Print the current time
#
##########################################################################################################
 timestamp()
 {
   echo "TIMESTAMP:`date`"
 }

##########################################################################################################
#
# Function                      :   executeSQLfile
# Purpose                       :   uses sqlplus to execute the /web/local/orderlink/linkload/sql_ctl/smo_crmem_purge.sql
#                                   which will purge data from the S_CRMEM & S_CRMEM_ITEM tables using an ORACLE package "PKG_SMO_CLEAN"
#                                   which contains purge procedures.
#
##########################################################################################################
 executeSQLfile()
 {
   if [ -f ${SQLDIR}/${SQLFILE} ]
   then
     echo "\nExecuting sqlplus to call purge procedures "
     echo "-----------------------------------------------------------------------------\n"
     sqlplus / @${SQLDIR}/${SQLFILE}
     RC=$?
     echo "\n----------------------------------------------------------------------------"
   else
     echo "The ${SQLDIR}/${SQLFILE} file was not found"
     RC=3
   fi
   return $RC
 }

##########################################################################################################
#
# Function                      :   main
# Purpose                       :   Main program flow
#
##########################################################################################################
main()
{
  echo "Start ${PGMNAME} on ${SVRNAME}"
  timestamp
  TIME=`date +%Y%m%d.%H%M`

     executeSQLfile
     RC=$?
     if [ "$RC" -ne 0 ]
     then
       echo "The executeSQLfile() function did not complete successfully"
       echo "Return = ${RC}"
     fi

    

  echo "End ${PGMNAME}"
  timestamp
  return $RC
}

##########################################################################################################
#calling the main function
main > ${LOGDIR}/${LOGFILE} 2>&1
RC=$?
\cp ${LOGDIR}/${LOGFILE} ${LOGDIR}/${LOGFILE}.${TIME}
exit ${RC}
##########################################################################################################
