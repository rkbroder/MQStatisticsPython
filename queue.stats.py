## Import the needed libraries
import configparser
import subprocess
import platform
import os.path
from os.path import exists as file_exists
import sys
import logging
import logging.config
import calendar
from datetime import date
import pymqi
from pymqi import _MQConst2String
from datetime import datetime
import argparse

try:
    from pymqi import pymqe # type: ignore
except ImportError:
    import pymqe # type: ignore # Backward compatibility
from pymqi import CMQCFC
from pymqi import CMQC, CMQXC, CMQZC
#############################################################################
###                                                                       ###
### Change Log        :                                                   ###
### 09/2023 - added code to perform Client or Server side connect based on###
###           properties file. Python usually installs as a CLIENT        ###
###           implementation. For SERVER, you have to override and        ###
###           recompile the MQPYI modules.                                ###
### 08/2024 - Updated the CSV data. ENQ and DEQ from the Queue was missing###
###           Important to determine performance. There was a metric      ###
###           misalignment and PUT1's we not being calculates correct     ###
###                                                                       ###
#############################################################################
#############################################################################
###                                                                       ###
### Static definations:                                                   ###
###                                                                       ###
#############################################################################
QManagers = []
prefix = '*'
current_month_name = calendar.month_name[date.today().month]
mqgetbackoutcount=mqgetcount=mqgetbytecount=mqgetdestnpcount=mqgetdestpcount=mqgetdestnpbytecount=mqgetdestpbytecount=mqgetnpbrmsgcount=mqgetpbrmsgcount=mqgetnpbrbytecount=mqgetpbrbytecount=expiredmessages=queuepurgcount=queueavgtime=queuedepth=mqgetdestfail2033=mqgetdestfail2080=mqgetdestfail=mqgetbrfail2033=mqgetbrfail2080=mqgetbrfail=None 
mqputcount=mqputbytecount=mqputnpcount=mqputpcount=mqputbackoutcount=mqput1npcount=mqput1pcount=mqputnpbytecount=mqputpbytecount=None




############################################################################
###                     get_config_dict(section)                         ###
### This function will process load the sections from the property       ###
### file into the script.                                                ###
###                                                                      ###
############################################################################

def get_config_dict(section):
    get_config_dict.config_dict = dict(config.items(section))
    return get_config_dict.config_dict
    
############################################################################
###                     Initialization process                           ###
### This preProcess code will grab the properties file which contains    ###
### The Logger path. then the Logger wil be set up to route messages     ###
### to a logging file.                                                   ###
###                                                                      ###
############################################################################
    
config = configparser.RawConfigParser()
if len(sys.argv) < 2:
    print("Length of ARGS = len(sys.argv)");
    sys.exit("*************************************************************\nProperties file missing from Argument!! \nThis script takes two argument, Property file full path and Queue Manager\n*************************************************************\n");
else:
    proppath = sys.argv[1];
    config.read(proppath);

if os.path.exists(proppath):
    config.read(proppath);
    # create logger
    mq_logger_property = get_config_dict('MQLogger')
    loggerconfigpath_Property = mq_logger_property.get("logconfigpath")
    logging.config.fileConfig(loggerconfigpath_Property)
    loggerName = mq_logger_property.get("loggername")
    logger = logging.getLogger(loggerName)
    
    logger.debug('MQS-MQH-000 - mq_logger_property = {a}' .format(a=mq_logger_property))
    logger.debug('MQS-MQH-000 - loggerconfigpath_Property = {a}' .format(a=loggerconfigpath_Property))
    logger.debug('MQS-MQH-000 - loggerName = {a}' .format(a=loggerName))
    logger.debug('MQS-MQH-000 - Length of ARGS = {a}' .format(a=len(sys.argv)))
    logger.debug('MQS-MQH-000 - Argument 1 = {a}' .format(a=sys.argv[1]))
    logger.debug('MQS-MQH-000 - Properties file found');
    logger.debug('MQS-MQH-000 - Properties path = {a}' .format(a=proppath));

#  Debug Levels:
#  	logger.debug
#  	logger.info
#  	logger.warning
#  	logger.error
#  	logger.critical
else:
    print('MQS-MQH-000 - CRITICAL!! No Properties file');

    
#############################################################################
###                                                                       ###
###                         mq_queue_manager_names()                      ###
###  Get a list of Queue Managers on this server                          ###
###                                                                       ###
#############################################################################
def mq_queue_manager_names():
  if config.has_section('qmgrName'):
    # lets get the MQ Version from the property file
    config_details = get_config_dict('qmgrName')
    qmgrkey = config_details['key1']
    qmgrkey = qmgrkey.strip()
    QManagers.append(qmgrkey)
    logger.debug('MQS-MQH-000 - Queue Manager from Config File = {a}' .format(a=qmgrkey))
  else:
    # file and directory listing 
    returned_text = subprocess.check_output("dspmq", shell=True, universal_newlines=True) 
    logger.debug('MQS-MQH-000 - Queue Manager Listing {a}' .format(a=returned_text))
    returned_text_split = returned_text.split()
    logger.debug('MQS-MQH-000 - returned_text_split = {a}' .format(a=returned_text_split))
    for linex, elem in enumerate(returned_text_split):
      if 'QMNAME' in str(elem):
        if 'Running' in str(returned_text_split[linex+1]):
          result = str(elem)[str(elem).find('(')+1:str(elem).find(')')]
          QManagers.append(result)
          logger.debug('MQS-MQH-000 - Length of result = {a}' .format(a=len(result)))
          logger.debug('MQS-MQH-000 - result = {a}' .format(a=result))

  return True
  
###############################################################################
###                                                                         ###
###                            MQS-MQH-003                                  ###
###             queue_get_stats(queueManager, queue_name)                   ###
### The function collects the data from an amqsrua execution on the queue   ###
### statistics                                                              ###
###                                                                         ###
###############################################################################
def queue_get_stats(queueManager, qname):
    rc=True
    global mqgetbackoutcount, mqgetcount, mqgetbytecount, mqgetdestnpcount, mqgetdestpcount, mqgetdestnpbytecount, mqgetdestpbytecount, mqgetnpbrmsgcount, mqgetpbrmsgcount, mqgetnpbrbytecount, mqgetpbrbytecount, expiredmessages, queuepurgcount, queueavgtime, queuedepth, mqgetdestfail2033, mqgetdestfail2080, mqgetdestfail, mqgetbrfail2033, mqgetbrfail2080, mqgetbrfail
###
### Capture the System stats for the Queue Manager by executing amqsrua
###
    getqstats = subprocess.check_output(['amqsrua', '-m', queueManager, '-c', 'STATQ', '-t', 'GET', '-o', qname, '-n1'])
    getqstats = str(getqstats)
###
### Split response into individual lines on CR/LF
###
    getqstats = getqstats.split('\\n')

    count=0
    logger.debug('MQS-MQH-003 - GETQSTATS type = {a}' .format(a=type(getqstats)))

###
### process each response line looking for our Statistic
###
    for x in getqstats:
#        print('my x = ', x)
        logger.debug('/n')
        logger.debug('MQS-MQH-003 -        {a}' .format(a=x))
## rolled back MQGET count        
        if 'rolled back MQGET count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetbackoutcount = list1[-1]
            logger.info('MQS-MQH-003 -         MQGET rolled back MQGET count =  {a}' .format(a=mqgetbackoutcount))
            count=count+1
            continue
## MQGET count        
        if 'MQGET count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetcount = list1[-1]
            logger.info('MQS-MQH-003 -         MQGET Count =  {a}' .format(a=mqgetcount))
            count=count+1
            continue
## MQGET byte count        
        if 'MQGET byte count' in x:
            x=x.strip()
            list1=x.split(" ");
            if '/sec' in x:
              mqgetbytecount = list1[-2]
            else:
            	mqgetbytecount = list1[-1]
            logger.info('MQS-MQH-003 -         MQGET Byte Count =  {a}' .format(a=mqgetbytecount))
            count=count+1
            continue   
## destructive MQGET non-persistent message        
        if 'destructive MQGET non-persistent message' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetdestnpcount = list1[-1]
            logger.info('MQS-MQH-003 -         MQGET non-persistent destructive message =  {a}' .format(a=mqgetdestnpcount))
            count=count+1
            continue
## destructive MQGET persistent message count        
        if 'destructive MQGET persistent message count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetdestpcount = list1[-1]
            logger.info('MQS-MQH-003 -         MQGET persistent destructive message count =  {a}' .format(a=mqgetdestpcount))
            count=count+1
            continue
## destructive MQGET non-persistent byte count        
        if 'destructive MQGET non-persistent byte count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetdestnpbytecount = list1[-1]
            logger.info('MQS-MQH-003 -         MQGET non-persistent destructive byte count =  {a}' .format(a=mqgetdestnpbytecount))
            count=count+1
            continue
## destructive MQGET persistent byte count        
        if 'destructive MQGET persistent byte count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetdestpbytecount = list1[-1]
            logger.info('MQS-MQH-003 -         MQGET persistent destructive byte count =  {a}' .format(a=mqgetdestpbytecount))
            count=count+1
            continue
## MQGET browse non-persistent message count        
        if 'MQGET browse non-persistent message count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetnpbrmsgcount = int(list1[-1])
            logger.info('MQS-MQH-003 -         MQGET browse non-persistent message count (mqgetnpbrmsgcount) =  {a}' .format(a=mqgetnpbrmsgcount))
            count=count+1
            continue
## MQGET browse persistent message count        
        if 'MQGET browse persistent message count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetpbrmsgcount = int(list1[-1])
            logger.info('MQS-MQH-003 -         MQGET browse persistent message count =  {a}' .format(a=mqgetpbrmsgcount))
            count=count+1
            continue
## MQGET browse non-persistent byte count        
        if 'MQGET browse non-persistent byte count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetnpbrbytecount = list1[-1]
            logger.info('MQS-MQH-003 -         MQGET browse non-persistent byte message count =  {a}' .format(a=mqgetnpbrbytecount))
            count=count+1
            continue
## MQGET browse persistent byte count        
        if 'MQGET browse persistent byte count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetpbrbytecount = list1[-1]
            logger.info('MQS-MQH-003 -         MQGET browse persistent byte message count =  {a}' .format(a=mqgetpbrbytecount))
            count=count+1
            continue
## Expired Count        
        if 'messages expired' in x:
            x=x.strip()
            list1=x.split(" ");
            expiredmessages = list1[-1]
            logger.info('MQS-MQH-003 -         Expired Messages Count =  {a}' .format(a=expiredmessages))
            count=count+1
            continue
## queue purged count        
        if 'queue purged count' in x:
            x=x.strip()
            list1=x.split(" ");
            queuepurgcount = list1[-1]
            logger.info('MQS-MQH-003 -         Queue Purge Count =  {a}' .format(a=queuepurgcount))
            count=count+1
            continue
## queue purged count        
        if 'average queue time' in x:
            x=x.strip()
            list1=x.split(" ");
            queueavgtime = list1[-2]
            logger.info('MQS-MQH-003 -         Average Queue Time =  {a}' .format(a=queueavgtime))
            count=count+1
            continue
## Queue Depth
        if 'Queue depth' in x:
            x=x.strip()
            list1=x.split(" ");
            queuedepth = list1[-1].strip()
            logger.info('MQS-MQH-003 -         Queue Depth =  {a}' .format(a=queuedepth))
            count=count+1
            continue
## destructive MQGET fails with MQRC_NO_MSG_AVAILABLE
        if 'destructive MQGET fails with MQRC_NO_MSG_AVAILABLE' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetdestfail2033 = list1[-1].strip()
            logger.info('MQS-MQH-003 -         MQGET destructive fail with No MSG Avail =  {a}' .format(a=mqgetdestfail2033))
            count=count+1
            continue
## destructive MQGET fails with MQRC_TRUNCATED_MSG_FAILED
        if 'destructive MQGET fails with MQRC_TRUNCATED_MSG_FAILED' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetdestfail2080 = list1[-1].strip()
            logger.info('MQS-MQH-003 -         MQGET destructive fail with truncated =  {a}' .format(a=mqgetdestfail2080))
            count=count+1
            continue
## destructive MQGET fails
        if 'destructive MQGET fails' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetdestfail = list1[-1].strip()
            logger.info('MQS-MQH-003 -         MQGET destructive fail =  {a}' .format(a=mqgetdestfail))
            count=count+1
            continue
## MQGET browse fails with MQRC_NO_MSG_AVAILABLE
        if 'MQGET browse fails with MQRC_NO_MSG_AVAILABLE' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetbrfail2033 = list1[-1].strip()
            logger.info('MQS-MQH-003 -         MQGET Browse fail with No MSG Avail =  {a}' .format(a=mqgetbrfail2033))
            count=count+1
            continue
## MQGET browse fails with MQRC_TRUNCATED_MSG_FAILED
        if 'MQGET browse fails with MQRC_TRUNCATED_MSG_FAILED' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetbrfail2080 = list1[-1].strip()
            logger.info('MQS-MQH-003 -         MQGET Browse fail with Truncated =  {a}' .format(a=mqgetbrfail2080))
            count=count+1
            continue
## MQGET browse fails
        if 'MQGET browse fails' in x:
            x=x.strip()
            list1=x.split(" ");
            mqgetbrfail = list1[-1].strip()
            logger.info('MQS-MQH-003 -         MQGET Browse fail =  {a}' .format(a=mqgetbrfail))
            count=count+1
            continue
            
    return rc
    
###############################################################################
###                                                                         ###
###                            MQS-MQH-004                                  ###
###             queue_put_stats(queueManager, queue_name)                   ###
### The function collects the data from an amqsrua execution on the queue   ###
### statistics                                                              ###
###                                                                         ###
###############################################################################
def queue_put_stats(queueManager, qname):
    rc=True
    global mqputcount, mqputbytecount, mqputnpcount, mqputpcount, mqputbackoutcount, mqput1npcount, mqput1pcount, mqputnpbytecount, mqputpbytecount
###
### Execute the command line for amqsrua to capture QSTAT
###
    putqstats = subprocess.check_output(['amqsrua', '-m', queueManager, '-c', 'STATQ', '-t', 'PUT', '-o', qname, '-n1'])
    putqstats = str(putqstats)
    putqstats = putqstats.split('\\n')

    count=0
    logger.debug('MQS-MQH-004 - PUTQSTATS type = {a}' .format(a=type(putqstats)))

###
### process each response line looking for our Statistic
###
    for x in putqstats:
#        print('my x = ', x)
        logger.debug('/n')
        logger.debug('MQS-MQH-004 -        {a}' .format(a=x))
#
### MQPUT count        
        if 'MQPUT/MQPUT1 count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqputcount = list1[-1]
            logger.info('MQS-MQH-004 -         MQPUT Count =  {a}' .format(a=mqputcount))
            count=count+1
            continue
#
### MQPUT byte count        
#
        if 'MQPUT byte count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqputbytecount = list1[-1]
            logger.info('MQS-MQH-004 -         MQPUT Byte Count =  {a}' .format(a=mqputbytecount))
            count=count+1
            continue   
#
### MQPUT non-persistent message count
#
        if 'MQPUT non-persistent message count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqputnpcount = list1[-1]
            logger.info('MQS-MQH-004 -         MQPUT Non-Persistent Count =  {a}' .format(a=mqputnpcount))
            count=count+1
            continue
#
### MQPUT persistent message count
#
        if 'MQPUT persistent message count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqputpcount = list1[-1]
            logger.info('MQS-MQH-004 -         MQPUT Persistent Count =  {a}' .format(a=mqputpcount))
            count=count+1
            continue
#
### rolled back MQGET count
#
        if 'rolled back MQPUT count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqputbackoutcount = list1[-1]
            logger.info('MQS-MQH-004 -         MQPUT rolled back count =  {a}' .format(a=mqputbackoutcount))
            count=count+1
            continue
#
### MQPUT1 non-persistent message count
#
        if 'MQPUT1 non-persistent message count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqput1npcount = list1[-1]
            logger.info('MQS-MQH-004 -         MQPUT1 Non-Persistent Count =  {a}' .format(a=mqput1npcount))
            count=count+1
            continue
#
### MQPUT1 persistent message count
#
        if 'MQPUT1 persistent message count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqput1pcount = list1[-1]
            logger.info('MQS-MQH-004 -         MQPUT1 Persistent Count =  {a}' .format(a=mqput1pcount))
            count=count+1
            continue
#
### non-persistent byte count
#
        if 'non-persistent byte count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqputnpbytecount = list1[-1]
            logger.info('MQS-MQH-004 -         PUT Non-Persistent Byte Count =  {a}' .format(a=mqputnpbytecount))
            count=count+1
            continue
#
### MQPUT persistent byte count
#
        if 'persistent byte count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqputpbytecount = list1[-1]
            logger.info('MQS-MQH-004 -         PUT Persistent Byte Count =  {a}' .format(a=mqputpbytecount))
            count=count+1
            continue
            
    return rc
    
###############################################################################
###                            MQS-MQH-002                                  ###
###                        collect_queue_stats()                            ###
### The function check to vrify there is trigger data present on the xmit   ###
### queue definations.                                                      ###
###                                                                         ###
###############################################################################
def collect_queue_stats(queueManager):
    logger.debug('MQS-MQH-002 - Start collect_queue_stats\n')
    rc=True
    global mqgetbackoutcount, mqgetcount, mqgetbytecount, mqgetdestnpcount, mqgetdestpcount, mqgetdestnpbytecount, mqgetdestpbytecount, mqgetnpbrmsgcount, mqgetpbrmsgcount, mqgetnpbrbytecount, mqgetpbrbytecount, expiredmessages, queuepurgcount, queueavgtime, queuedepth, mqgetdestfail2033, mqgetdestfail2080, mqgetdestfail, mqgetbrfail2033, mqgetbrfail2080, mqgetbrfail
    global mqputcount, mqputbytecount, mqputnpcount, mqputpcount, mqputbackoutcount, mqput1npcount, mqput1pcount, mqputnpbytecount, mqputpbytecount
###
###  Create/reuse output file
###
##   from os.path import exists as file_exists
    hostname = subprocess.check_output("hostname", shell=True, universal_newlines=True)
    list0=hostname.split(".")
    hostHLQ=list0[0].strip()
    logger.debug('MQS-MQH-002 - Hostname = {a}' .format(a=hostHLQ))
    
###
### Set up report file name
###
    name = hostHLQ + ".QUEUE_STATS_" + current_month_name
    filename = "%s.csv" % name
    if file_exists(filename):
       logger.debug('MQS-MQH-002 - Statistics file exists = {a}' .format(a=filename))
       qstatsreport = open(filename, "a")
    else:
       logger.debug('MQS-MQH-002 - Statistics file DOES NOT exists = {a}' .format(a=filename))
       qstatsreport = open(filename, "w")
       outputL="queueName, creationDate, creationTime,queueType,definationType,queueMinDepth,queueMaxDepth,msgdeqcount,msgenqcount,avgQueueTime,puts,putsFailed,puts1,putBytes,gets,getBytes,getsFailed,browses,browseBytes,browseFailed,msgNotQueued,msgsExpired,msgPurged,Qmgr,Timestamp" + "\n"

####   outputL="queueName, creationDate, creationTime,queueType,definationType,queueMinDepth,queueMaxDepth,avgQueueTime,puts,putsFailed,puts1,putBytes,gets,getBytes,getsFailed,browses,browseBytes,browseFailed,msgNotQueued,msgsExpired,msgPurged,Qmgr,Timestamp" + "\n"
       qstatsreport.write(outputL)   
    count=0
    tag=False
    a=True

###
### Issure MQCMD_INQUIRE_Q on all Local Queues, then execute a MQCMD_RESET_Q_STATS against
### all non-SYSTEM queus
###
    queue_args = {pymqi.CMQC.MQCA_Q_NAME: prefix,
                 pymqi.CMQC.MQIA_Q_TYPE: pymqi.CMQC.MQQT_LOCAL,
                 pymqi.CMQCFC.MQIACF_Q_ATTRS: pymqi.CMQCFC.MQIACF_ALL}
###
### Issure MQCMD_INQUIRE_Q on all Local Queues
###
    try:
        queue_response = pcf.MQCMD_INQUIRE_Q(queue_args)
    except pymqi.MQMIError as e:
       if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_UNKNOWN_OBJECT_NAME:
           logger.error('MQS-MQH-002 - No queue definations - 1')
           rc=False
           return rc
       else:
           logger.error('MQS-MQH-002 - No queue defination for QUEUE_INQ - 2')
           raise
    else:
        logger.debug('MQS-MQH-002 - We got a response\n')
        logger.debug('MQS-MQH-002 - Length of queue_response = {a}' .format(a=len(queue_response)))
        for queue_info in queue_response:
            queue_name = queue_info[pymqi.CMQC.MQCA_Q_NAME].decode('utf-8').strip()
###
### Bypass SYSTEM queues
###
            if 'SYSTEM' not in queue_name:
                logger.info('MQS-MQH-002 - Queue Name = {a}' .format(a=queue_name))
                creationdate = queue_info[pymqi.CMQC.MQCA_CREATION_DATE].decode('utf-8').strip()
                logger.info('MQS-MQH-002 -         Creation Date = {a}' .format(a=creationdate))
                creationtime=queue_info[pymqi.CMQC.MQCA_CREATION_TIME].decode('utf-8')
                logger.info('MQS-MQH-002 -         Creation Time = {a}' .format(a=creationtime))
                qdefinationtypeINT=queue_info[pymqi.CMQC.MQIA_DEFINITION_TYPE]
                #
                #### translate queue type INT into string
                #
                d = _MQConst2String(CMQC, 'MQQDT_')
                if qdefinationtypeINT in d:
                    list1=d[qdefinationtypeINT].split("_")
                    qdefinationtype = list1[-1].strip()
                    logger.debug('MQS-MQH-003 -         Queue Defination Type = {a} '.format(a=qdefinationtype))
                else:
                    logger.debug('MQS-MQH-002 -         Queue Defination Type = {a}' .format(a=qdefinationtypeINT))
                    logger.info('MQS-MQH-002 -          Queue Defination Type = {a}' .format(a=qdefinationtypeINT))
                    qdefinationtype = srt(qdefinationtypeINT)
                qtypeINT=queue_info[pymqi.CMQC.MQIA_Q_TYPE]
                #
                #### translate queue type INT into string
                #
                d = _MQConst2String(CMQC, 'MQQT_')
                if qtypeINT in d:
                    qtype=d[qtypeINT]
                    logger.debug('MQS-MQH-003 -         Queue Type = {a} '.format(a=qtype))
                else:
                    logger.debug('MQS-MQH-002 -         Queue Type = {a}' .format(a=qtypeINT))
                    logger.info('MQS-MQH-002 -          Queue Type = {a}' .format(a=qtypeINT))
                    qtype=str(qtypeINT)

###
### Issure RESET STATS on Queue
###

                queue_args = {pymqi.CMQC.MQCA_Q_NAME: queue_name}
                 	
                try:
                    queue_response = pcf.MQCMD_RESET_Q_STATS(queue_args)
                except pymqi.MQMIError as e:
                   if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_UNKNOWN_OBJECT_NAME:
                       logger.error('MQS-MQH-002 - No queue definations - 1')
                       rc=False
                       return rc
                   else:
                       logger.error('MQS-MQH-002 - No queue defination - 2')
                       raise
                else:
                   logger.debug('MQS-MQH-002 - We got a response\n')
                   logger.debug('MQS-MQH-002 - Length of queue_response = {a}' .format(a=len(queue_response)))
                   for queue_info in queue_response:
###
### get MQIA_HIGH_Q_DEPTH
###

                      logger.info('MQS-MQH-002 - Queue Name = {a}' .format(a=queue_name))
                      highdepth = queue_info[pymqi.CMQC.MQIA_HIGH_Q_DEPTH] 
                      logger.info('MQS-MQH-002 -         High Depth = {a}' .format(a=highdepth))
###
### get MQIA_MSG_DEQ_COUNT
###
                      msgdeqcount=queue_info[pymqi.CMQC.MQIA_MSG_DEQ_COUNT]
                      logger.info('MQS-MQH-002 -         DEQ Count = {a}' .format(a=msgdeqcount))
                      logger.debug('MQS-MQH-002 -         Queue Manager Name = {a}' .format(a=queueManager))
###
### get MQIA_MSG_ENQ_COUNT
###
                      msgenqcount=queue_info[pymqi.CMQC.MQIA_MSG_ENQ_COUNT]
                      logger.info('MQS-MQH-002 -         ENQ Count = {a}' .format(a=msgenqcount))
                      logger.debug('MQS-MQH-002 -         Queue Manager Name = {a}' .format(a=queueManager))
###
### Get the PUT and GET Queue Statistics
###
                      queue_get_stats(queueManager, queue_name)
                      queue_put_stats(queueManager, queue_name)
###
### Calculate the Browse report Statistics
###
            browses = mqgetnpbrmsgcount + mqgetpbrmsgcount 
            browseBytes = mqgetnpbrbytecount + mqgetpbrbytecount
            browseFailed = mqgetbrfail2033 + mqgetbrfail2080 + mqgetbrfail

            msgNotQueued = 'none'
###
### The following are fields returned sometimes as none (null) from the MQ API calls. Set them for ouput
###        
            if queuedepth is None:
               queuedepth=0
               logger.info('MQS-MQH-002 -         queuedepth = None')	
            if queueavgtime is None:
               queueavgtime=0
               logger.info('MQS-MQH-002 -         queueavgtime = None')
            if expiredmessages is None:
               expiredmessages=0
               logger.info('MQS-MQH-002 -         expiredmessages = None') 
            if queuepurgcount is None:
               queuepurgcount=0
               logger.info('MQS-MQH-002 -         queuepurgcount = None') 
            dt = datetime.now()
            logger.debug('MQS-MQH-002 - queue_name = {a}' .format(a=type(queue_name)))
            logger.debug('MQS-MQH-002 - creationdate = {a}' .format(a=type(creationdate)))
            logger.debug('MQS-MQH-002 - creationtime = {a}' .format(a=type(creationtime)))
            logger.debug('MQS-MQH-002 - qtype = {a}' .format(a=type(qtype)))    
            logger.debug('MQS-MQH-002 - qdefinationtype = {a}' .format(a=type(qdefinationtype)))        
            logger.debug('MQS-MQH-002 - queuedepth = {a}' .format(a=type(queuedepth)))
            logger.debug('MQS-MQH-002 - highdepth = {a}' .format(a=type(highdepth)))
            logger.debug('MQS-MQH-002 - msgdeqcount = {a}' .format(a=type(msgdeqcount)))
            logger.debug('MQS-MQH-002 - msgenqcount = {a}' .format(a=type(msgenqcount)))
            logger.debug('MQS-MQH-002 - queueavgtime = {a}' .format(a=type(queueavgtime)))
            logger.debug('MQS-MQH-002 - mqput1npcount = {a}' .format(a=type(mqput1npcount)))
            logger.debug('MQS-MQH-002 - mqput1pcount = {a}' .format(a=type(mqput1pcount)))
            logger.debug('MQS-MQH-002 - mqputcount = {a}' .format(a=type(mqputcount)))
            logger.debug('MQS-MQH-002 - mqputbackoutcount = {a}' .format(a=type(mqputbackoutcount)))
            logger.debug('MQS-MQH-002 - mqputbytecount = {a}' .format(a=type(mqputbytecount)))
            logger.debug('MQS-MQH-002 - mqgetcount = {a}' .format(a=type(mqgetcount)))
            logger.debug('MQS-MQH-002 - mqgetbytecount = {a}' .format(a=type(mqgetbytecount)))
            logger.debug('MQS-MQH-002 - mqgetdestfail = {a}' .format(a=type(mqgetdestfail)))
            logger.debug('MQS-MQH-002 - browses = {a}' .format(a=type(browses)))
            logger.debug('MQS-MQH-002 - browseBytes = {a}' .format(a=type(browseBytes)))
            logger.debug('MQS-MQH-002 - browseFailed = {a}' .format(a=type(browseFailed)))
            logger.debug('MQS-MQH-002 - msgNotQueued = {a}' .format(a=type(msgNotQueued)))
            logger.debug('MQS-MQH-002 - expiredmessages = {a}' .format(a=type(expiredmessages)))
            logger.debug('MQS-MQH-002 - queuepurgcount = {a}' .format(a=type(queuepurgcount)))
            logger.debug('MQS-MQH-002 - queueManager = {a}' .format(a=type(queueManager)))
            logger.debug('MQS-MQH-002 - dt = {a}' .format(a=type(dt)))
                        
###
### Build and output the CSV report line from the captured Statistics
###  
            puts1count=mqput1npcount=mqput1pcount    
            outputL=queue_name + ',' + creationdate + ',' + creationtime + ',' + qtype + ',' + qdefinationtype + ',' + str(queuedepth) + ',' + str(highdepth) + ','  + str(msgdeqcount) + ','  + str(msgenqcount) + ',' + str(queueavgtime) + ',' + mqputcount + ',' + mqputbackoutcount + ','  + str(puts1count) + ','+ mqputbytecount + ',' + mqgetcount + ',' + mqgetbytecount + ',' + mqgetdestfail + ',' + str(browses) + ',' + str(browseBytes) + ',' + str(browseFailed) + ',' + msgNotQueued + ',' + str(expiredmessages) + ',' + str(queuepurgcount) + ',' + queueManager + ',' + str(dt) + "\n"
            qstatsreport.write(outputL)   
    qstatsreport.close()
    return rc 

###############################################################################
###                                                                         ###
###                        connect_queue_manager()                          ###
### This function provides the code to connect to the QMGR.                 ###
### It looks via a client     ###
### connection. This is the default build for PYMQI. It requires key values ###
### in the config.property file. IT can also connect using SSL provided a   ###
### valid KEY DB is suplied.                                                ###
### Stanza:                                                                 ###
###    [MQConnection]                                                       ###
###    qmgr=                                                                ###
###    ssl= (NO/YES)                                                        ###
###    host=                                                                ###
###    port=                                                                ###
###    channel=                                                             ###                     
###    cipher=                                                              ###
###    repos= (path to KDB)                                                 ###                       
###                                                                         ###
###############################################################################
def connect_queue_manager(queueManager):
###
### Check for connecion stanza (MQConnection), If it exists we are doing a 
### CLIENT connection and will ignore the queueManager parameter as it will be
### in the config.properties file.
###

  if config.has_section('MQConnection'):
      logger.debug('MQS-MQH-009 - Section MQConnection exist. We will run with that!')
      mq_connection_property = get_config_dict('MQConnection')
      logger.debug('MQS-MQH-000 - Connection Property = {a}' .format(a=mq_connection_property))
      property_found=True
      mq_connection_property = get_config_dict('MQConnection')
      logger.debug('MQS-MQH-000 - Connection Property = {a}' .format(a=mq_connection_property))
      qmgr=mq_connection_property.get("qmgr")
      ssl=mq_connection_property.get("ssl")
      host=mq_connection_property.get("ip")
      port=mq_connection_property.get("port")
      channel=mq_connection_property.get("channel")
      ssl_asbytes=str.encode(ssl)
      host_asbytes=str.encode(host)
      port_asbytes=str.encode(port)
      channel_asbytes=str.encode(channel)
      logger.debug('MQS-MQH-000 - MQ Connection Information /n Host = {a} /n Port = {b} /n Queue Manager = {c} /n Channel = {d}' .format(a=host, b=port, c=queueManager, d=channel))
###
### Check for ssl property to see if we are going to connect usig SSL
###
      logger.debug('MQS-MQH-000 - ssl flag = {a}' .format(a=ssl))
      if ssl == 'NO':
        conn_info = '%s(%s)' % (host, port)
        try:
          logger.debug('MQS-MQH-000 - About to connect to queue manager = {a}' .format(a=queueManager))
          qmgr = pymqi.connect(queueManager, channel, conn_info)
          rc=True
        except pymqi.MQMIError as e:
          if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_HOST_NOT_AVAILABLE:
            logger.error('MQS-MQH-000 - Such a host `%s` does not exist.' % host)
            logger.critical('MQS-MQH-000 - Reason code from connection attempt = {a}' .format(a=e.reason))
            rc=False
          else:
      	    rc=False
      	    logger.critical('MQS-MQH-000 - Other Connect Error')
      	    logger.critical('MQS-MQH-000 - Reason code from connection attempt = {a}' .format(a=e.reason))
      	    raise
      else:
        conn_info = '%s(%s)' % (host, port)
        conn_info_asbytes=str.encode(conn_info)
        ssl_cipher_spec = mq_connection_property.get("cipher")
        ssl_cipher_spec_asbytes=str.encode(ssl_cipher_spec)
        repos = mq_connection_property.get("repos")
        repos_asbytes=str.encode(repos)
        cd = pymqi.CD()
        cd.ChannelName = channel_asbytes
        cd.ConnectionName = conn_info_asbytes
        cd.ChannelType = pymqi.CMQXC.MQCHT_CLNTCONN
        cd.TransportType = pymqi.CMQXC.MQXPT_TCP
        cd.SSLCipherSpec = ssl_cipher_spec_asbytes
        options = CMQC.MQCNO_NONE
        cd.UserIdentifier = str.encode('mqm')
        cd.Password = str.encode('mqm')
        sco = pymqi.SCO()
        sco.KeyRepository = repos_asbytes
        logger.debug('MQS-MQH-000 - MQ SSL Connection Information \n queueManager = {a} \n cd = {b} \n sco = {c} \n' .format(a=queueManager, b=cd, c=sco))
#  qmgr.connect_with_options(queueManager, options, cd, sco)
        try:
           logger.debug('MQS-MQH-000 - About to connect_with_options to queue manager = {a}' .format(a=queueManager))
           qmgr = pymqi.QueueManager(None)
           qmgr = qmgr.connect_with_options(queueManager, cd, sco)
           rc=True
        except pymqi.MQMIError as e:
           if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_HOST_NOT_AVAILABLE:
              logger.error('MQS-MQH-000 - Such a host `%s` does not exist.' % host)
              logger.critical('MQS-MQH-000 - Reason code from connection attempt = {a}' .format(a=e.reason))
              rc=False
           else:
      	      logger.critical('MQS-MQH-000 - Other Connect Error')
      	      logger.critical('MQS-MQH-000 - Reason code from connection attempt = {a}' .format(a=e.reason))
      	      rc=False
      	      raise
  else:
      try:
        logger.error('MQS-MQH-000 - Section MQConnection not found! Doing a Server connection')
        logger.debug('MQS-MQH-000 - About to connect to queue manager = {a}' .format(a=queueManager))
###
### This method does a SERVER connection to the QMGR. The PYMQI package has been compile
### as 'BUILD SERVICE'. To do a client build the PYMQI code as client 'BUILD CLIENT' and
### then implement the client connection connect_queue_manager found in GITHUB.
###
        qmgr = pymqi.connect(queueManager)
        rc=True
      except pymqi.MQMIError as e:
        if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_HOST_NOT_AVAILABLE:
           logger.error('MQS-MQH-000 - Such a host `%s` does not exist.' % host)
           rc=False
        else:
    	     rc=False
    	     logger.critical('MQS-MQH-000 - Other Connect Error')
    	     raise
  

  if rc:
    return qmgr
  else:
  	return False

#
## End Connect to Queue Manager
#
                  
###############################################################################
###                                                                         ###
###                            Main Routines                                ###
###                                                                         ###
###############################################################################


###############################################################################
###                                                                         ###
###                        Get Queue mnager names                           ###
###                                                                         ###
###############################################################################
if mq_queue_manager_names():
  logger.error('MQS-MQH-000 - Queue Manager Names retrieved ********** {a}' .format(a=QManagers))
else:
  logger.critical('MQS-MQH-000 - Queueu Manager Name FAULT **********')

for QueueMGR in QManagers:            
  ###############################################################################
  ###                            MQS-MQH-002                                  ###
  ###                       collect_queue_stats()                             ###
  ###                        Collect Queue Stats                              ###
  ###                                                                         ###
  ###############################################################################
  prefix = '*'
  print('Queue Manager = ',QueueMGR)
  qmgr = connect_queue_manager(QueueMGR)
  pcf = pymqi.PCFExecute(qmgr)
  logger.debug('MQS-MQH-002 -  Process the mqm Group Membership')
  if collect_queue_stats(QueueMGR):
	  logger.debug('MQS-MQH-002 - Queue Stats retrieve processes correctly')
  else:
	  logger.critical('MQS-MQH-002 - Queue stats retrieveal failed')
  qmgr.disconnect()	  


