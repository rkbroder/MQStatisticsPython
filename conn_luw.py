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
import codecs
import string

try:
    from pymqi import pymqe # type: ignore
except ImportError:
    import pymqe # type: ignore # Backward compatibility
from pymqi import CMQCFC
from pymqi import CMQC, CMQXC, CMQZC


############################################################################
###                     get_config_dict(section)                         ###
### This function will process load the sections from the property       ###
### file into the script.                                                ###
###                                                                      ###
############################################################################

def get_config_dict(section):
    get_config_dict.config_dict = dict(config.items(section))
    return get_config_dict.config_dict
        
#############################################################################
###                                                                       ###
###                         mq_queue_manager_names()                      ###
###  Get a list of Queue Managers on this server. If the QmgrName section ###
###  apears in the proprties file we will use than. Otherwise, the script ###
###  will look for QMGRS on the server.                                   ###
###                                                                       ###
#############################################################################
def mq_queue_manager_names():
  if config.has_section('qmgrName'):
    # lets get the MQ Version from the property file
    config_details = get_config_dict('qmgrName')
    qmgrkey = config_details['key1']
    qmgrkey = qmgrkey.strip()
    QManagers.append(qmgrkey)
    logger.debug('MQS-MQLUW-000 - Queue Manager from Config File = {a}' .format(a=qmgrkey))
  else:
    # file and directory listing 
    returned_text = subprocess.check_output("dspmq", shell=True, universal_newlines=True) 
    logger.debug('MQS-MQLUW-000 - Queue Manager Listing {a}' .format(a=returned_text))
    returned_text_split = returned_text.split()
    logger.debug('MQS-MQLUW-000 - returned_text_split = {a}' .format(a=returned_text_split))
    for linex, elem in enumerate(returned_text_split):
      if 'QMNAME' in str(elem):
        if 'Running' in str(returned_text_split[linex+1]):
          result = str(elem)[str(elem).find('(')+1:str(elem).find(')')]
          QManagers.append(result)
          logger.debug('MQS-MQLUW-000 - Length of result = {a}' .format(a=len(result)))
          logger.debug('MQS-MQLUW-000 - result = {a}' .format(a=result))

  return True
  
###############################################################################
###                            MQS-MQLUW-002                                ###
###                             conn_check()                                ###
### The function retrieves all the connections and checks for the presence  ###
### of and LUW greater than the specified time period                       ###
###                                                                         ###
###############################################################################
def conn_check(queueManager):
    logger.debug('MQS-MQLUW-002 - Start collect_queue_stats\n')
    rc=True
    
    count=0
    tag=False
    a=True

#######
####### display conn(*) where(channel NE '') APPLTAG CHANNEL CONNAME CONNOPTS
#######

###
### Issure MQCMD_INQUIRE_CONNECTION on all connections WE are looking for a value in UOWLOGDA UOWLOGTI which
###        indicates there is a transaction under control
### all non-SYSTEM queus
###
###    null_byte = b'\'\''
    null_byte=b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    conn_args = {pymqi.CMQCFC.MQBACF_GENERIC_CONNECTION_ID: pymqi.ByteString('')}

### Filter Mapping
##    'less': CMQCFC.MQCFOP_LESS,
##    'equal': CMQCFC.MQCFOP_EQUAL,
##    'greater': CMQCFC.MQCFOP_GREATER,
##    'not_less': CMQCFC.MQCFOP_NOT_LESS,
##    'not_equal': CMQCFC.MQCFOP_NOT_EQUAL,
##    'not_greater': CMQCFC.MQCFOP_NOT_GREATER,
##    'like': CMQCFC.MQCFOP_LIKE,
##    'not_like': CMQCFC.MQCFOP_NOT_LIKE,
##    'contains': CMQCFC.MQCFOP_CONTAINS,
##    'contains_gen': CMQCFC.MQCFOP_CONTAINS_GEN,
##    'excludes_gen': CMQCFC.MQCFOP_EXCLUDES_GEN,
##    filter1 = pymqi.Filter(pymqi.CMQCFC.MQCACH_CHANNEL_NAME).not_equal(bytes(str, 'UTF-8'))
##    filter1 = pymqi.Filter(pymqi.CMQCFC.MQCACH_CHANNEL_NAME).not_equal('')

###
### Issure MQCMD_INQUIRE_CONNECTION
###
    try:
        conn_response=pcf.MQCMD_INQUIRE_CONNECTION({pymqi.CMQCFC.MQBACF_GENERIC_CONNECTION_ID:pymqi.ByteString(''),pymqi.CMQCFC.MQIACF_CONNECTION_ATTRS:pymqi.CMQCFC.MQIACF_ALL})	
    except pymqi.MQMIError as e:
       if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_UNKNOWN_OBJECT_NAME:
           logger.error('MQS-MQLUW-002 - No connections for MQCMD_INQUIRE_CONNECTION - 1')
           rc=False
           return rc
       else:
           logger.error('MQS-MQLUW-002 - No connections for MQCMD_INQUIRE_CONNECTION - 2')
           raise
    else:
        logger.debug('MQS-MQLUW-002 - We got a response\n')
        logger.debug('MQS-MLUWQ-002 - conn_response type = {a}' .format(a=type(conn_response)))
        logger.debug('MQS-MQLUW-002 - Length of conn_response = {a}' .format(a=len(conn_response)))
        for conn_info in conn_response:
            logger.debug('MQS-MQLUW-002 - Response Line = {a}' .format(a=conn_info))
            
            conn_log_start_date = conn_info[pymqi.CMQCFC.MQCACF_UOW_LOG_START_DATE].decode('utf-8').strip()
            if len(conn_log_start_date) != 0:
              conn_log_start_time = conn_info[pymqi.CMQCFC.MQCACF_UOW_START_TIME].decode('utf-8').strip()
              logger.debug('MQS-MQLUW-002 - Log Start Time = {a}' .format(a=conn_log_start_time))
              today = datetime.now()
              uow_datetime_combined = datetime.strptime(conn_log_start_date.replace("-", "/") + " " + conn_log_start_time.replace(".", ":"), '%Y/%m/%d %H:%M:%S')
              logger.debug('MQS-MQLUW-002 - Date Time Combined = {a}' .format(a=uow_datetime_combined))
              uow_time_diff = today - uow_datetime_combined
              uow_tsecs = uow_time_diff.total_seconds()
              logger.debug('MQS-MQLUW-002 - UOW seconds duratione = {a}' .format(a=uow_tsecs))
              logger.debug('MQS-MQLUW-002 - Log Start Date = {a}' .format(a=conn_log_start_date))
              if uow_tsecs > luwtollarance:
                conn_id = str(conn_info[pymqi.CMQCFC.MQBACF_CONNECTION_ID])
                conn_id = conn_id.rstrip("'")
                conn_id = conn_id.lstrip("b")
                conn_id = conn_id.lstrip("'")
                conn_channel_name = conn_info[pymqi.CMQCFC.MQCACH_CHANNEL_NAME].decode('utf-8').strip()
                logger.debug('MQS-MQLUW-002 - Channel Name = {a}' .format(a=conn_channel_name))
                logger.debug('MQS-MLUWQ-002 - conn_id = {a}' .format(a=conn_id))
                conn_appl_tag = conn_info[pymqi.CMQCFC.MQCACF_APPL_TAG].decode('utf-8').strip()
                conn_extent_name = conn_info[pymqi.CMQCFC.MQCACF_UOW_LOG_EXTENT_NAME].decode('utf-8').strip()
                conn_user_id = conn_info[pymqi.CMQCFC.MQCACF_USER_IDENTIFIER].decode('utf-8').strip()
                conn_process_id = conn_info[pymqi.CMQCFC.MQIACF_PROCESS_ID]
                logger.debug('MQS-MQLUW-002 - MQIACF_PROCESS_ID = {a}' .format(a=conn_process_id))
                outputL="Channel Name = " + conn_channel_name + "\n"
                qstatsreport.write(outputL)
                print(type(uow_tsecs))
                outputL="     Number of seconds UOW is out = %.2f \n" % uow_tsecs
                qstatsreport.write(outputL)
                outputL="     Start Date of UOW = " + conn_log_start_date + "\n"
                qstatsreport.write(outputL)
                outputL="     Start Time of UOW = " + conn_log_start_time + "\n"
                qstatsreport.write(outputL)
                outputL="     Conn ID = " + conn_id + "\n"
                qstatsreport.write(outputL)  
                outputL="     Application TAG = " + conn_appl_tag + "\n"
                qstatsreport.write(outputL)
                outputL="     Beginning Log Extent = " + conn_extent_name + "\n"
                qstatsreport.write(outputL)
                outputL="     Application UID = " + conn_user_id + "\n"
                qstatsreport.write(outputL)
                conn_process_id_str = "% s" % conn_process_id
                outputL="     Process ID = " + conn_process_id_str + "\n"
                qstatsreport.write(outputL)
                try:
###
###    args= []
###    args.append(pymqi.CFBS(Parameter=pymqi.CMQCFC.MQBACF_CONNECTION_ID,String=conn_id))
###    args.append(pymqi.CFIL(Parameter=pymqi.CMQCFC.MQIACF_CONNECTION_ATTRS,Values=[pymqi.CMQCFC.MQIACF_ALL]))
###    response = pcf.MQCMD_INQUIRE_CONNECTION(args)
###
###                  conn_handle_response=pcf.MQCMD_INQUIRE_CONNECTION({pymqi.CMQCFC.MQBACF_GENERIC_CONNECTION_ID:pymqi.ByteString(''),
                  conn_handle_response=pcf.MQCMD_INQUIRE_CONNECTION({pymqi.CMQCFC.MQBACF_GENERIC_CONNECTION_ID:pymqi.ByteString(''),
                  	pymqi.CMQCFC.MQIACF_CONN_INFO_TYPE:pymqi.CMQCFC.MQIACF_CONN_INFO_HANDLE,pymqi.CMQCFC.MQIACF_CONNECTION_ATTRS:pymqi.CMQCFC.MQIACF_ALL})
                except pymqi.MQMIError as e:
                  if e.comp == pymqi.CMQC.MQCC_FAILED and (e.reason == pymqi.CMQC.MQRC_UNKNOWN_OBJECT_NAME or e.reason == pymqi.CMQCFC.MQRCCF_NONE_FOUND):
                    logger.error('MQS-MQLUW-002 - No connections handle for MQCMD_INQUIRE_CONNECTION (handle) - 1')
                  else:
                    logger.error('MQS-MQLUW-002 - No connection handle for MQCMD_INQUIRE_CONNECTION (handle) - 2')
                    raise        
                else:
                  logger.debug('MQS-MQLUW-002 - We got a response\n')
                  logger.debug('MQS-MLUWQ-002 - conn_handle_response type = {a}' .format(a=type(conn_handle_response)))
                  logger.debug('MQS-MQLUW-002 - Length of conn_handle_response = {a}' .format(a=len(conn_handle_response)))
                  for conn_handle_info in conn_handle_response:	
                    logger.debug('MQS-MQLUW-002 - Response Line = {a}' .format(a=conn_handle_info))
                    conn_handle_id = str(conn_handle_info[pymqi.CMQCFC.MQBACF_CONNECTION_ID])
                    conn_handle_id = conn_handle_id.rstrip("'")
                    conn_handle_id = conn_handle_id.lstrip("b")
                    conn_handle_id = conn_handle_id.lstrip("'")
                    logger.debug('MQS-MLUWQ-002 - conn_handle_id = {a}' .format(a=conn_handle_id))
                    if conn_handle_id ==  conn_id:
                      logger.debug('MQS-MQLUW-002 - We got a MATCH\n')
                      conn_handle_obj_name = conn_handle_info[pymqi.CMQCFC.MQCACF_OBJECT_NAME].decode('utf-8').strip()  
                      outputL="     Object Names = " + conn_handle_obj_name + "\n"
                      qstatsreport.write(outputL)
                      queue_put_stats(queueManager, conn_handle_obj_name)
              
    qstatsreport.close()
    return rc 

###############################################################################
###                                                                         ###
###                        connect_queue_manager()                          ###
### Connect to the Queue manager non-Client                                 ###
###                                                                         ###
###############################################################################
def connect_queue_manager(queueManager):
  rc=True
  
  try:
    logger.debug('MQS-MQLUW-000 - About to connect to queue manager = {a}' .format(a=queueManager))
###
### This method does a SERVIC connection to the QMGR. The PYMQI package has been compile
### as 'BUILD SERVICE'. To do a client build the PYMQI code as client 'BUILD CLIENT' and
### then implement the client connection connect_queue_manager found in GITHUB.
###
    qmgr = pymqi.connect(queueManager)
  except pymqi.MQMIError as e:
    if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_HOST_NOT_AVAILABLE:
        logger.error('MQS-MQLUW-000 - Such a host `%s` does not exist.' % host)
        rc=False
    else:
    	rc=False
    	logger.critical('MQS-MQLUW-000 - Other Connect Error')
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
    logger.debug('MQS-MQLUW-002 - PUTQSTATS type = {a}' .format(a=type(putqstats)))

###
### process each response line looking for our Statistic
###
    for x in putqstats:
#        print('my x = ', x)
        logger.debug('/n')
        logger.debug('MQS-MQLUW-002 -        {a}' .format(a=x))
#
### MQPUT count        
        if 'MQPUT/MQPUT1 count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqputcount = list1[-1]
            logger.debug('MQS-MQLUW-002 -         MQPUT Count =  {a}' .format(a=mqputcount))
            outputL="               MQPUT Count = " + str(mqputcount) + "\n"
            qstatsreport.write(outputL)
            continue
#
### MQPUT non-persistent message count
#
        if 'MQPUT non-persistent message count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqputnpcount = list1[-1]
            logger.info('MQS-MQLUW-002 -         MQPUT Non-Persistent Count =  {a}' .format(a=mqputnpcount))
            outputL="               MQPUT Non-Persistent Count = " + mqputnpcount + "\n"
            qstatsreport.write(outputL)
            continue
#
### MQPUT persistent message count
#
        if 'MQPUT persistent message count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqputpcount = list1[-1]
            logger.info('MQS-MQLUW-002 -         MQPUT Persistent Count =  {a}' .format(a=mqputpcount))
            outputL="               MQPUT Persistent Count =  " + mqputpcount + "\n"
            qstatsreport.write(outputL)
            continue
#
### MQPUT1 non-persistent message count
#
        if 'MQPUT1 non-persistent message count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqput1npcount = list1[-1]
            logger.info('MQS-MQLUW-002 -         MQPUT1 Non-Persistent Count =  {a}' .format(a=mqput1npcount))
            outputL="               MQPUT1 Non-Persistent Count =  " + mqput1npcount + "\n"
            qstatsreport.write(outputL)
            count=count+1
            continue
#
### MQPUT1 persistent message count
#
        if 'MQPUT1 persistent message count' in x:
            x=x.strip()
            list1=x.split(" ");
            mqput1pcount = list1[-1]
            logger.info('MQS-MQLUW-002 -         MQPUT1 Persistent Count =  {a}' .format(a=mqput1pcount))
            outputL="               MQPUT1 Persistent Count =  " + mqput1pcount + "\n"
            qstatsreport.write(outputL)
            count=count+1
            continue
###
### Issure MQCMD_INQUIRE_Q on all Local Queues, then execute a MQCMD_RESET_Q_STATS against
### all non-SYSTEM queus
###
    queue_args = {pymqi.CMQC.MQCA_Q_NAME: qname,
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
            queue_depth = queue_info[pymqi.CMQC.MQIA_CURRENT_Q_DEPTH]
            logger.info('MQS-MQLUW-002 -         Currnt Queue Depth =  {a}' .format(a=str(queue_depth)))
            outputL="               Currnt Queue Depth =  " + str(queue_depth) + "\n"
            qstatsreport.write(outputL)
            ###
            q_open_in_count = queue_info[pymqi.CMQC.MQIA_OPEN_INPUT_COUNT]
            logger.info('MQS-MQLUW-002 -         Open Input Count =  {a}' .format(a=str(q_open_in_count)))
            outputL="               Open Input Count =  " + str(q_open_in_count) + "\n"
            qstatsreport.write(outputL)
            ###
            q_open_out_count = queue_info[pymqi.CMQC.MQIA_CURRENT_Q_DEPTH]
            logger.info('MQS-MQLUW-002 -         Open Output Count =  {a}' .format(a=str(q_open_out_count)))
            outputL="               Open Output Count =  " + str(q_open_out_count) + "\n"
            qstatsreport.write(outputL)
            
    return rc
                  
###############################################################################
###                                                                         ###
###                            Main Routines                                ###
###                                                                         ###
###############################################################################
#############################################################################
###                                                                       ###
### Static definations:                                                   ###
###                                                                       ###
#############################################################################
QManagers = []
prefix = '*'
current_month_name = calendar.month_name[date.today().month]

   
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
    
    logger.debug('MQS-MQLUW-000 - mq_logger_property = {a}' .format(a=mq_logger_property))
    logger.debug('MQS-MQLUW-000 - loggerconfigpath_Property = {a}' .format(a=loggerconfigpath_Property))
    logger.debug('MQS-MQLUW-000 - loggerName = {a}' .format(a=loggerName))
    logger.debug('MQS-MQLUW-000 - Length of ARGS = {a}' .format(a=len(sys.argv)))
    logger.debug('MQS-MQLUW-000 - Argument 1 = {a}' .format(a=sys.argv[1]))
    logger.debug('MQS-MQLUW-000 - Properties file found');
    logger.debug('MQS-MQLUW-000 - Properties path = {a}' .format(a=proppath));
    if config.has_section('MQConn'):
# lets get the LUW tolerance in seconds from the properties file
      config_details = get_config_dict('MQConn')
      luwtollarance = config_details['luwtollarance']
      luwtollarance = int(luwtollarance.strip())
      logger.debug('MQS-MQLUW-000 - LUW Time tollarance (seconds) = {a}' .format(a=str(luwtollarance)))
    else:	
###
### The property does not exist so lets set a default LUW tolanance
###
      luwtollarance = int(20)
      logger.debug('MQS-MQLUW-000 - LUW Time tollarance (seconds) = {a}' .format(a=str(luwtollarance)))

#  Debug Levels:
#  	logger.debug
#  	logger.info
#  	logger.warning
#  	logger.error
#  	logger.critical
else:
    print('MQS-MQLUW-000 - CRITICAL!! No Properties file');
    
###
### Set up report file name
###
hostname = subprocess.check_output("hostname", shell=True, universal_newlines=True)    
list0=hostname.split(".")                                                              
hostHLQ=list0[0].strip()                                                               
logger.debug('MQS-MQLUW-002 - Hostname = {a}' .format(a=hostHLQ))                      
name = hostHLQ + ".CONN_RPT_" + current_month_name
filename = "%s.csv" % name
if file_exists(filename):
   logger.debug('MQS-MQLUW-002 - Report file exists = {a}' .format(a=filename))
   qstatsreport = open(filename, "a")
else:
   logger.debug('MQS-MQLUW-002 - Report file DOES NOT exists = {a}' .format(a=filename))
   qstatsreport = open(filename, "w")
   outputL="Long Running Task Report for MQ" + "\n"
   qstatsreport.write(outputL) 

###############################################################################
###                                                                         ###
###                        Get Queue mnager names                           ###
###                                                                         ###
###############################################################################
if mq_queue_manager_names():
  logger.error('MQS-MQLUW-000 - Queue Manager Names retrieved ********** {a}' .format(a=QManagers))
else:
  logger.critical('MQS-MQLUW-000 - Queueu Manager Name FAULT **********')

for QueueMGR in QManagers:            
  ###############################################################################
  ###                            MQS-MQLUW-002                                  ###
  ###                       collect_queue_stats()                             ###
  ###                        Collect Queue Stats                              ###
  ###                                                                         ###
  ###############################################################################
  prefix = '*'
  print('Queue Manager = ',QueueMGR)
  
  qmgr = connect_queue_manager(QueueMGR)
  pcf = pymqi.PCFExecute(qmgr)
  logger.debug('MQS-MQLUW-002 -  Process the mqm Group Membership')
  if conn_check(QueueMGR):
	  logger.debug('MQS-MQLUW-002 - Queue Stats retrieve processes correctly')
  else:
	  logger.critical('MQS-MQLUW-002 - Queue stats retrieveal failed')
  qmgr.disconnect()	  


