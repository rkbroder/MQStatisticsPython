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

###############################################################################
###                                                                         ###
###                        Get System Statistics                            ###
###                                                                         ###
###############################################################################

###
### Get HOST NAME for output report file extension
###
hostname = subprocess.check_output("hostname", shell=True, universal_newlines=True)
list0=hostname.split(".")
hostHLQ=list0[0].strip()
logger.debug('MQS-MQH-002 - Hostname = {a}' .format(a=hostHLQ))

###
### Set up report file name
###
name = hostHLQ + ".SYSTEM_STATS_" + current_month_name
filename = "%s.csv" % name

###
### Open report file and write header
###
if file_exists(filename):
  report = open(filename, "a")
else:
  report = open(filename, "w")
  outputL="Host,Put Date,Put Time,Queue Manager,Queue Manager file system in Use,File System Free Space,System CPU Time,User CPU Time,RAM Total Bytes" + "\n"
  report.write(outputL)

count=0
tag=False
a=True

###
### Capture the System stats for each Queue Manager by executing amqsrua
### Each -c/-t has to be executed independantly because of the -n1 parm
###
for QueueMGR in QManagers:
  systemstats1 = subprocess.check_output(['amqsrua', '-m', QueueMGR, '-c', 'CPU', '-t', 'SystemSummary', '-n1'])
  logger.debug('MQS-MQH-001 - SYSTEMSTATS1 command response = {a}' .format(a=systemstats1))
  systemstats1 = str(systemstats1)
  systemstats1 = systemstats1.split('\\n')
  systemstats2 = subprocess.check_output(['amqsrua', '-m', QueueMGR, '-c', 'CPU', '-t', 'QMgrSummary', '-n1'])
  logger.debug('MQS-MQH-001 - SYSTEMSTATS2 command response = {a}' .format(a=systemstats2))
  systemstats2 = str(systemstats2)
  systemstats2 = systemstats2.split('\\n')  
  systemstats3 = subprocess.check_output(['amqsrua', '-m', QueueMGR, '-c', 'DISK', '-t', 'SystemSummary', '-n1'])
  logger.debug('MQS-MQH-001 - SYSTEMSTATS3 command response = {a}' .format(a=systemstats3))
  systemstats3 = str(systemstats3)
  systemstats3 = systemstats3.split('\\n')  
  systemstats4 = subprocess.check_output(['amqsrua', '-m', QueueMGR, '-c', 'DISK', '-t', 'QMgrSummary', '-n1'])
  logger.debug('MQS-MQH-001 - SYSTEMSTATS4 command response = {a}' .format(a=systemstats4))
  systemstats4 = str(systemstats4)
  systemstats4 = systemstats4.split('\\n')  
  
###
### Concatenate the individual responses
###
  systemstats = systemstats1 + systemstats2 + systemstats3 + systemstats4

  count=0
  logger.debug('MQS-MQH-001 - SYSTEMSTATS type = {a}' .format(a=type(systemstats)))
  logger.debug('MQS-MQH-001 - SYSTEMSTATS command response = {a}' .format(a=systemstats))
  tag=False
  
###
### Process each line in the response looking for what we need
###
  for x in systemstats:

###
### Grab the Date and Time from the first Publication timestamp
###
    logger.debug('MQS-MQH-001 - SYSTEMSTATS line response = {a}' .format(a=x))       
    if 'Publication received' in x:
      if not tag:
        tag=True
        list1=x.split(" ");
        list3=list1[2].split(":")
        putdate=list3[-1].strip()
        logger.debug('MQS-MQH-001 - PUT putdate = {a}' .format(a=putdate))
        list4=list1[3].split(":")
        puttime=list4[-1].strip()
        logger.debug('MQS-MQH-001 - PUT puttime = {a}' .format(a=puttime))
        print('PUT Time = ', puttime)
        continue
###
### Look for the File System Bytes in Use data
###
    if 'Queue Manager file system - bytes in use' in x:
      list1=x.split(" ");
      filesysteminuse = list1[-1].strip()
      logger.debug('MQS-MQH-001 - File System Bytes in Use (#1) = {a}' .format(a=filesysteminuse))
      count=count+1
      continue
###
### Look for the File System Free Space data
###
    if 'Queue Manager file system - free space' in x:
      list1=x.split(" ");
      filesystemfreespace = list1[-1].strip()
      logger.debug('MQS-MQH-001 - File System - free space (#2) = {a}' .format(a=filesystemfreespace))
      count=count+1
      continue
###
### Look for the QMGR CPU time percentage
###
    if 'System CPU time - percentage estimate for queue manager' in x:
      list1=x.split(" ");
      systemcputime= list1[-1].strip()
      logger.debug('MQS-MQH-001 - SYSTEM CPU (#3) = {a}' .format(a=systemcputime))
      count=count+1
      continue
###
### Look for the QMGR RAM Total Bytes
###
    if 'RAM total bytes -' in x:
      list1=x.split(" ");
      rambytes=list1[-1].strip()
      logger.debug('MQS-MQH-001 - RAM total bytes (#4) = {a}' .format(a=rambytes))
      print('RAM total bytes = ',rambytes)
      count=count+1
      continue
###
### If we have captured all 4 System stats, put the data to the CSV file
###
  if count == 4:
    outputL= hostHLQ + ',' + putdate + ',' + puttime + ',' + QueueMGR + ',' + filesysteminuse + ',' + filesystemfreespace + ',' + systemcputime + ',' + rambytes + "\n"
    report.write(outputL)
  else:
    logger.debug('MQS-MQH-003 - Did not receive all required parameters in response. Count = {a}' .format(a=str(count)))  	
  
report.close() 

