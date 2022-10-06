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
###  print("Does config has qmgrName section?  : {}".format(config.has_section("qmgrName")))
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
###                            MQS-MQH-002                                  ###
###                        collect_channel_stats()                          ###
### The function check to vrify there is trigger data present on the xmit   ###
### queue definations.                                                      ###
###                                                                         ###
###############################################################################
def collect_channel_stats(queueManager):
    logger.debug('MQS-MQH-002 - Start collect_channel_stats\n')
    rc=True
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
    name = hostHLQ + ".CHANNEL_STATS_" + current_month_name
    filename = "%s.csv" % name
    logger.debug('MQS-MQH-002 - File name = {a}' .format(a=filename))
    if file_exists(filename):
       logger.debug('MQS-MQH-002 - file EXISTS. File = {a}' .format(a=filename))
       c = open(filename, "a")
    else:
       logger.debug('MQS-MQH-002 - file DOES NOT EXIST. File = {a}' .format(a=filename))
       c = open(filename, "w")
       outputL="Channel Name,Channel Type,Remote Queue Mgr Name,Connection Name,Msgs,Bytes,Net Time Min,Net Time Max,Net Time Avg,Full Batches,Incomplete Batches,Avg Batch Size,Put Retries,Qmgr,Timestamp" + "\n"
       c.write(outputL)   

###
### Issure MQCMD_INQUIRE_CHANNEL to get a full list of all channels
###
    channel_args = {pymqi.CMQCFC.MQCACH_CHANNEL_NAME: '*', pymqi.CMQCFC.MQIACF_CHANNEL_ATTRS: [CMQCFC.MQCACH_CHANNEL_NAME,CMQCFC.MQIACH_CHANNEL_TYPE]}
    try:
        channel_response = pcf.MQCMD_INQUIRE_CHANNEL(channel_args)
    except pymqi.MQMIError as e:
       if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_UNKNOWN_OBJECT_NAME:
           logger.error('MQS-MQH-002 - No channel definations - 1')
           rc=False
           return rc
       else:
           logger.error('MQS-MQH-002 - No channel defination for CHANNEL_NAME_INQ - 2')
           raise
    else:
        logger.debug('MQS-MQH-002 - We got a response\n')
        logger.debug('MQS-MQH-002 - Length of channel_response = {a}' .format(a=len(channel_response)))
###
### Process each channel returned from the MQCMD_INQUIRE_CHANNEL command
### 
        for channel_info in channel_response:
            logger.debug('MQS-MQH-002 - Channel Inquire Instance = {a}' .format(a=channel_info))
            channel_name = channel_info[pymqi.CMQCFC.MQCACH_CHANNEL_NAME].decode('utf-8').strip()
###
### Bypass SYSTEM* Channels
###
            if 'SYSTEM' not in channel_name:
                chldeftypeINT=channel_info[pymqi.CMQCFC.MQIACH_CHANNEL_TYPE]
                #
                #### translate channel type INT into string
                #
                d = _MQConst2String(CMQXC, 'MQCHT_')
                if chldeftypeINT in d:
                    list1=d[chldeftypeINT].split("_")
                    chldeftype = list1[-1].strip()
                    logger.debug('MQS-MQH-003 -         Channel Defination Type = {a} '.format(a=chldeftype))
                    if 'RECEIVER' in chldeftype or 'SENDER' in chldeftype:
                        logger.debug('MQS-MQH-002 -         SENDER or RECEIVER Channel Defination Type = {a}' .format(a=chldeftype))
                        pass
                    else:
                        logger.debug('MQS-MQH-002 -         Not processing channel {a} of type {b}' .format(a=channel_name, b=chldeftype))
                        continue
                else:
                    logger.debug('MQS-MQH-002 -         Channel Defination Type = {a}' .format(a=chqdeftypeINT))
                    logger.info('MQS-MQH-002 -          Channel Defination Type = {a}' .format(a=chldeftypeINT))
                    logger.debug('MQS-MQH-002 -         Not processing channel {a} with unknown type {b}' .format(a=channel_name, b=str(chldeftypeINT)))
                    continue
###
### issue MQCMD_INQUIRE_CHANNEL_STATUS to get the ststus of the channel
###
                channel_stat_args = {pymqi.CMQCFC.MQCACH_CHANNEL_NAME: channel_name}
###                	      pymqi.CMQCFC.MQIACH_CHANNEL_INSTANCE_ATTRS: pymqi.CMQCFC.MQIACF_ALL}
                 	
                try:
                    channel_stat_response = pcf.MQCMD_INQUIRE_CHANNEL_STATUS(channel_stat_args)
                except pymqi.MQMIError as e:
                   if e.comp == (pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_UNKNOWN_OBJECT_NAME) or (e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQCFC.MQRCCF_CHL_STATUS_NOT_FOUND):
                       logger.info('MQS-MQH-002 - No channel status found. Channel = {a}. Status = {b}' .format(a=channel_name,b=e.reason))
                   else:
                       logger.error('MQS-MQH-002 - Unexpected Error. No channel status. Channel = {a}. Status = {b}' .format(a=channel_name,b=e.reason))
                       raise
                else:
                   logger.debug('MQS-MQH-002 - We got a response\n')
                   logger.debug('MQS-MQH-002 - Length of channel_stats_response = {a}' .format(a=len(channel_stat_response)))
                   logger.debug('MQS-MQH-002 - Contents of channel_stats_response = {a}' .format(a=channel_stat_response))
                   for channel_stats_info in channel_stat_response:
                      logger.info('MQS-MQH-002 - Channel Name = {a}' .format(a=channel_name))
###
### Pull the MQCA_REMOTE_Q_MGR_NAME from the response line
###
                      remotequeuemgrname = channel_stats_info[pymqi.CMQC.MQCA_REMOTE_Q_MGR_NAME].decode('utf-8').strip()
                      logger.info('MQS-MQH-002 -         Remote Queue Managr Name = {a}' .format(a=remotequeuemgrname))
###
### Pull the MQCACH_CONNECTION_NAME from the response line
###
                      connectionname = channel_stats_info[pymqi.CMQCFC.MQCACH_CONNECTION_NAME].decode('utf-8')
                      if ')' in connectionname:
                          list1=connectionname.split(')')
                          connectionname=list1[0] + ')'
                      logger.info('MQS-MQH-002 -         Connectionname = {a}' .format(a=connectionname))
###
### Pull the MQIACH_MSGS from the response line
###
                      msgs = channel_stats_info[pymqi.CMQCFC.MQIACH_MSGS] 
                      logger.info('MQS-MQH-002 -         Messages Sent = {a}' .format(a=msgs))
###
### Pull the MQIACH_BYTES_SENT from the response line
###
                      bytes = channel_stats_info[pymqi.CMQCFC.MQIACH_BYTES_SENT] 
                      logger.info('MQS-MQH-002 -         Bytes Sent = {a}' .format(a=bytes))
###
### Pull the MQIACH_NETWORK_TIME_INDICATOR from the response line. Could be missing!
###
                      if pymqi.CMQCFC.MQIACH_NETWORK_TIME_INDICATOR not in channel_stats_info:
                        logger.info('MQS-MQH-002 -         Nettime = None')	
                        nettimemin=0
                        nettimemax=0
                        nettimeavg=0
                      else:
                        nettime = channel_stats_info[pymqi.CMQCFC.MQIACH_NETWORK_TIME_INDICATOR]
                        logger.info('MQS-MQH-002 -         Nettime = {a}' .format(a=nettime))
                        nettimemin=nettime[0]
                        logger.info('MQS-MQH-002 -         nettimemin = {a}' .format(a=nettimemin))
                        nettimemax=nettime[1]
                        logger.info('MQS-MQH-002 -         nettimemax = {a}' .format(a=nettimemax))
                        nettimeavg=(nettimemin + nettimemax) / 2
                        logger.info('MQS-MQH-002 -         nettimeavg = {a}' .format(a=nettimeavg))
###
### Pull the MQIACH_BATCHES from the response line. COuld be missing!
###
                      fullbatches = channel_stats_info[pymqi.CMQCFC.MQIACH_BATCHES] 
                      logger.info('MQS-MQH-002 -         Full Batches = {a}' .format(a=fullbatches))
###
### Pull the MQIACH_BATCH_SIZE_INDICATOR from the response line. Two values returned.
###
                      list1 = channel_stats_info[pymqi.CMQCFC.MQIACH_BATCH_SIZE_INDICATOR]
                      incompletebatches = list1[1] 
                      logger.info('MQS-MQH-002 -         Incomplete Batches = {a}' .format(a=incompletebatches))
                      avgbatchsize = (list1[0] + list1[0]) / 2
                      logger.info('MQS-MQH-002 -         avgbatchsize = {a}' .format(a=avgbatchsize))
### Not Returned
                      putretries=0
                      dt = datetime.now()
                      
###
### Build and output the CSV report line from the captured data
###
                      outputL=channel_name + ',' + chldeftype + ',' + remotequeuemgrname + ',' + connectionname + ',' + str(msgs) + ',' + str(bytes) + ',' + str(nettimemin) + ',' + str(nettimemax) + ',' + str(nettimeavg) + ',' + str(fullbatches) + ',' + str(incompletebatches) + ',' + str(avgbatchsize) + ',' + str(putretries) + ',' + queueManager + ',' + str(dt) +"\n"
                      c.write(outputL)          
    c.close()
    return rc 

###############################################################################
###                                                                         ###
###                        connect_queue_manager()                          ###
### The function check to vrify there is trigger data present on the xmit   ###
### queue definations.                                                      ###
###                                                                         ###
###############################################################################
def connect_queue_manager(queueManager):
  rc=True
  try:
    logger.debug('MQS-MQH-000 - About to connect to queue manager = {a}' .format(a=queueManager))
###
### This method does a SERVIC connection to the QMGR. The PYMQI package has been compile
### as 'BUILD SERVICE'. To do a client build the PYMQI code as client 'BUILD CLIENT' and
### then implement the client connection connect_queue_manager found in GITHUB.
###
    qmgr = pymqi.connect(queueManager)
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
  ###                      collect_channel_stats()                            ###
  ###                       Collect Channel Stats                             ###
  ### This function will exercise the INQ Channel Stats command and parse     ###
  ### the result set to create a CSV output file for importing into EXCEL     ###
  ###############################################################################
  prefix = '*'
  print('Queue Manager = ',QueueMGR)
  qmgr = connect_queue_manager(QueueMGR)
  pcf = pymqi.PCFExecute(qmgr)
  logger.debug('MQS-MQH-002 -  Process the channel statistics')
  if collect_channel_stats(QueueMGR):
	  logger.debug('MQS-MQH-002 - Channel Statistics retrieve processes correctly')
  else:
	  logger.critical('MQS-MQH-002 - Channel statistics retrieveal failed')
  logger.debug('MQS-MQH-002 - Queue Manager Disconnect, {a}' .format(a=QueueMGR))  
  qmgr.disconnect()
