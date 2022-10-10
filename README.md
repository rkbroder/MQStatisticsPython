# MQStatisticsPython
Several PYTHON scripts that use MQ features to capture various MQ statistical information.

The scripts are run as follows:
python3 <script name> <script config properties>

1: Queue Statistics - runs and captures MQ Queue Statistics for the Queue Manager identified in the config.properties file pass on the command line. It will produce a CSV file in the same directory as run which can be imported into EXCEL. A log report file is created in the directory configured in the log.config.property file pointed to by the config.property file.
2: Channel Statistics - runs and captures MQ Chanel Statistics for active channels on the Queue Manager identified in the config.properties file pass on the command line. It will produce a CSV file in the same directory as run which can be imported into EXCEL. A log report file is created in the directory configured in the log.config.property file pointed to by the config.property file.
3: SYSTEM Stats - runs and captures System Statistics (CPU, DISK, MEMORY) for the Queue Manager identified in the config.properties file pass on the command line. It will produce a CSV file in the same directory as run which can be imported into EXCEL. A log report file is created in the directory configured in the log.config.property file pointed to by the config.property file.

These scripts run using PYTHON 3.8 on Linux and Windows. They require PYMQI 1.12 or the most recent version.

The scripts use MQ base commands (INQ Channels Stats, INQ Queue Stats) and also exercises the MQ System sample program amqsrua.

The scripts show how to execute the commands and parse through the data. The end result will be an output CSV file that can
be imported into EXCEL.
