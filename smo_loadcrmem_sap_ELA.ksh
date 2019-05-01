#!/bin/ksh
# /usr/bin/ksh does not exist in zone 3
DIRNAME=`dirname $0`

# Setup Global environment variables
. $DIRNAME/OrderingEnv.ksh

LOGFILEDIR=$DIRNAME
LOGFILE="$LOGFILEDIR/smo_loadcrmem_sap_ELA.log"
#DBSID=L525Z1 
#DBUSER=OL00
#DBPWD=OL00
# Invoke the stored procedure pkg_load_ack_sap_ELA.prc_load_ack_sap
#sqlplus -s $DBUSER/$DBPWD@$DBSID \@$DIRNAME/smo_loadcrmem_sap_ELA.sql >> $LOGFILE
#sqlplus -s ${ORALOGON}  \@${DIRNAME}/smo_loadcrmem_sap_ELA.sql >> $LOGFILE
sqlplus -silent / @${DIRNAME}/smo_loadcrmem_sap_ELA.sql >> $LOGFILE
	
