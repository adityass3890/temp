#!/bin/bash
source ~/.bash_profile
BASE_LOCATION="/app/tetra/DLM"
export SCRIPT_DIR="${BASE_LOCATION}/scripts/"
export CONF_DIR="${BASE_LOCATION}/config/"
export JAR_DIR="${BASE_LOCATION}/jar/"
export LOG_DIR="${BASE_LOCATION}/logs/"

SPARK_HOME=/app/tetra/elevate/software/spark/

dt=`/bin/date +%d%m%Y`


LOG_FILE=${LOG_DIR}dualimeiinfobyprimaryimeiExtract.log_"$dt"_1

touch $LOG_FILE
chmod 777 $LOG_FILE
# Removal of 3 days old log files.
for FILE in `find $LOG_DIR -mtime +3 | grep "TRANSID_SPARK_SPOOLING\.log_"`
do
rm -f "$FILE" 2>/dev/null
done
# Check if the instance of the script is already running.
if [ -e ${LOG_DIR}dualimeiinfobyprimaryimeiExtract.lck ]
then
echo "Another initiation Failed!!! Another Script is already running. New instance will not be invoked. !!" >> ${LOG_FILE}
(>&2 echo "Another initiation Failed!!! Another Script is already running. New instance will not be invoked. !!")
exit 1
fi
#------------------------------------------------------------------------------------------------------
echo $$
trap 'echo "Kill Signal Received.\n";exit' SIGHUP SIGINT SIGQUIT SIGTERM SIGSEGV

#cat $FILE

echo "Extracting data for yesterday from SOA for TRANSID Report" >> ${LOG_FILE}
touch ${LOG_DIR}dualimeiinfobyprimaryimeiExtract.lck
chmod 777 ${LOG_DIR}dualimeiinfobyprimaryimeiExtract.lck


#Starting the cassandra extraction spark job execution
${SPARK_HOME}bin/spark-submit \
--class com.tmobile.dlmExtract.dualimeiinfobyprimaryimeiExtract \
--name dualimeiinfobyprimaryimeiExtract \
--master yarn \
--deploy-mode cluster \
--conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=1024M" \
--conf spark.executor.memoryOverhead=1024m \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.sql.autoBroadcastJoinThreshold=209715200 \
--conf spark.sql.shuffle.partitions=50 \
--conf spark.shuffle.blockTransferService=nio \
--driver-java-options -XX:MaxPermSize=1024m \
--driver-memory 2g \
--executor-memory 6g \
--executor-cores 4 \
${JAR_DIR}tetra-elevate-conversion_2.12-1.0.jar ${CONF_DIR} extract_config.json HP $1

if [ $? -eq 0 ]
then
    echo "dualimeiinfobyprimaryimei extraction is completed successfully"
    #echo "Coping the data to monday date file"
    rm -f ${LOG_DIR}dualimeiinfobyprimaryimeiExtract.lck
    echo "Lock Released" >> ${LOG_FILE}
else
        echo "dualimeiinfobyprimaryimei extraction job has failed!!" >> ${LOG_FILE}
        cd $LOG_DIR
        sparkfn=$(ls -rt1 sparkstacktrace.log|tail -1)
        for LOG_DIR in ${sparkfn}
        do
            sparkfile1=`echo $LOG_DIR|cut -d ' ' -f 1`
            ( printf "Dear All,"
                        printf '%s\n'
                        printf '%s\n'
                        printf '%s\n' "dualimeiinfobyprimaryimei extraction job Failed."
                        printf '%s\n' "Please find the attached error stack log for analysis."
                        printf '%s\n'
                        printf '%s\n' "Thanks,"
                        printf '%s\n' "DLM-Extraction Team" ) | mailx -s "dualimeiinfobyprimaryimei extraction job failed" -a ${LOG_DIR} $sparkfile1 mounica.arepalli1@t-mobile.com
        done
        rm -f ${LOG_DIR}dualimeiinfobyprimaryimeiExtract.lck
        echo "Lock Released" >> ${LOG_FILE}
fi
