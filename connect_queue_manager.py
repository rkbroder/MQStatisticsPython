###############################################################################
###                                                                         ###
###                        connect_queue_manager()                          ###
### This function provides the code to connect to the QMGR via a client     ###
### connection. This is the default build for PYMQI. It requires key values ###
### in the config.property file. IT can also connect using SSL provided a   ###
### valid KEY DB is suplied.                                                ###
### Stanza:                                                                 ###
###    [MQConnection]                                                       ###
###    ssl (NO/YES)                                                         ###
###    host                                                                 ###
###    port                                                                 ###
###    channel                                                              ###                     
###    cipher                                                               ###
###    repos (path to KDB)                                                  ###                       
###                                                                         ###
###############################################################################
def connect_queue_manager(queueManager):
  rc=True
  mq_connection_property = get_config_dict('MQConnection')
  logger.debug('MQS-MQH-000 - Connection Property = {a}' .format(a=mq_connection_property))
  ssl = mq_connection_property.get("ssl")
  host = mq_connection_property.get("ip")
  port = mq_connection_property.get("port")
  channel = mq_connection_property.get("channel")

  ssl_asbytes=str.encode(ssl)
  host_asbytes=str.encode(host)
  port_asbytes=str.encode(port)
  channel_asbytes=str.encode(channel)
#  qmgr = pymqi.QueueManager(None)

  logger.debug('MQS-MQH-000 - MQ Connection Information /n Host = {a} /n Port = {b} /n Queue Manager = {c} /n Channel = {d}' .format(a=host, b=port, c=queueManager, d=channel))
#print('Connection Dictionary = ', mq_connection_property)
  if ssl == 'NO':
    conn_info = '%s(%s)' % (host, port)
    try:
      logger.debug('MQS-MQH-000 - About to connect to queue manager = {a}' .format(a=queueManager))
#      qmgr = pymqi.connect(queueManager, channel, conn_info)
      qmgr = pymqi.connect(queueManager)
    except pymqi.MQMIError as e:
      if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_HOST_NOT_AVAILABLE:
          logger.error('MQS-MQH-000 - Such a host `%s` does not exist.' % host)
          rc=False
      else:
      	rc=False
      	logger.critical('MQS-MQH-000 - Other Connect Error')
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
    except pymqi.MQMIError as e:
      if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_HOST_NOT_AVAILABLE:
          logger.error('MQS-MQH-000 - Such a host `%s` does not exist.' % host)
          rc=False
      else:
      	logger.critical('MQS-MQH-000 - Other Connect Error')
      	rc=False
      	raise

  if rc:
    return qmgr
  else:
  	return False

#
## End Connect to Queue Manager
#