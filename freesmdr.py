#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Free SMDR daemon
by Gabriele Tozzi <gabriele@tozzi.eu>, 2010-2011

This software starts a TCP server and listens for a SMDR stream. The received
data is then written in raw format to a log file and also to a MySQL database.

Here is the SQL to create the table:
 CREATE TABLE `freesmdr` (
  `idfreesmdr` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `call_start` datetime DEFAULT NULL,
  `call_duration` time DEFAULT NULL,
  `ring_duration` time DEFAULT NULL,
  `caller` varchar(255) DEFAULT NULL,
  `direction` enum('I','O') DEFAULT NULL,
  `called_number` varchar(255) DEFAULT NULL,
  `dialled_number` varchar(255) DEFAULT NULL,
  `account` varchar(255) DEFAULT NULL,
  `is_internal` tinyint(1) DEFAULT NULL COMMENT '**BOOL**',
  `call_id` int(10) unsigned DEFAULT NULL,
  `continuation` tinyint(1) DEFAULT NULL COMMENT '**BOOL**',
  `paty1device` char(5) DEFAULT NULL,
  `party1name` varchar(255) DEFAULT NULL,
  `party2device` char(5) DEFAULT NULL,
  `party2name` varchar(255) DEFAULT NULL,
  `hold_time` time DEFAULT NULL,
  `park_time` time DEFAULT NULL,
  `authvalid` varchar(255) DEFAULT NULL,
  `authcode` varchar(255) DEFAULT NULL,
  `user_charged` varchar(255) DEFAULT NULL,
  `call_charge` varchar(255) DEFAULT NULL,
  `currency` varchar(255) DEFAULT NULL,
  `amount_change` varchar(255) DEFAULT NULL COMMENT 'Amount at last User Change',
  `call_units` varchar(255) DEFAULT NULL,
  `units_change` varchar(255) DEFAULT NULL COMMENT 'Units at last User Change',
  `cost_per_unit` varchar(255) DEFAULT NULL,
  `markup` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`idfreesmdr`),
  KEY `direction_idx` (`direction`),
  KEY `caller_idx` (`caller`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Freesmdr log table';

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

'''

from socketserver import TCPServer
from socketserver import BaseRequestHandler
import sys, os
from optparse import OptionParser
import traceback
import re, math
from datetime import datetime, time
import MySQLdb
import logging
import signal
import configparser

# Info
NAME = 'Free SMDR'
VERSION = '0.93'

# Classes
class ParserError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class RecvHandler(BaseRequestHandler):

    def handle(self):
        """ Handles established connection
    
        self.request is the socket
        """
        
        global server_running
        global MYSQL_DB
        log = logging.getLogger('req_handler')

        # Init parser
        #parser = re.compile('^(("(?:[^"]|"")*"|[^,]*)(,("(?:[^"]|"")*"|[^,]*))*)$')
        parser = re.compile(',')
        fieldlist = (
            ( "call_start", 'datetime', '%Y/%m/%d %H:%M:%S' ),
            ( "call_duration", 'time', '%H:%M:%S' ),
            ( "ring_duration", 'timeint' ), # In seconds, max 9999
            ( "caller", 'str', 255 ),
            ( "direction", 'enum', ['I','O'] ), #Inbound, Outbound
            ( "called_number", 'str', 255 ),
            ( "dialled_number", 'str', 255 ),
            ( "account", 'str', 255 ),
            ( "is_internal", 'bool' ), #0 or 1
            ( "call_id", 'int' ), #Internal avaya call ID
            ( "continuation", 'bool' ), #Tells if there is a further record for this callID
            ( "party1device", 'str', 5 ), #(E|T|V)xxx E=Extension, T=Trunk, V=voicemail
            ( "party1name", 'str', 255 ),
            ( "party2device", 'str', 5 ), #Like above
            ( "party2name", 'str', 255 ),
            ( "hold_time", 'timeint' ), #Seconds
            ( "park_time", 'timeint' ), #Seconds
            ( "authvalid", 'str', 255 ), #Undocumented from here
            ( "authcode", 'str', 255 ),
            ( "user_charged", 'str', 255 ),
            ( "call_charge", 'str', 255 ),
            ( "currency", 'str', 255 ),
            ( "amount_change", 'str', 255 ),
            ( "call_units", 'str', 255 ),
            ( "units_change", 'str', 255 ),
            ( "cost_per_unit", 'str', 255 ),
            ( "markup", 'str', 255 ),
        );

        peerinfo = self.request.getpeername()
        log.info('Got connection from ' + peerinfo[0] + ' (' + str(peerinfo[1]) + ')')
        
        #Init database
        conn = MySQLdb.connect(
            host = MYSQL_DB['host'],
            user = MYSQL_DB['user'],
            passwd = MYSQL_DB['passwd'],
            db = MYSQL_DB['db'],
        )
        conn.autocommit(True)

        #Receive data loop
        dbuffer = ""
        while server_running:
            data = self.request.recv(1024).decode()
            if not data:
                break

            # Append data to LOGFILE
            lgf = open(LOGFILE, 'a')
            lgf.write(data)
            lgf.close()

            # Process data
            line = data.strip(" \n\r\t")
            vals = parser.split(line)
            if len(vals) >= len(fieldlist):
                # Received a good line
                # Build a dictionary
                dictv = {}
                i = 0
                try:
                    for v in fieldlist:
                        if v[1] == 'datetime':
                            dictv[v[0]] = datetime.strptime(vals[i], v[2])
                        elif v[1] == 'time':
                            dictv[v[0]] = datetime.strptime(vals[i], v[2]).time()
                        elif v[1] == 'timeint':
                            z = int(vals[i])
                            h = int(math.floor( z / ( 60 ** 2 ) ))
                            m = int(math.floor( ( z - ( h * 60 ** 2 ) ) / 60 ** 1 ))
                            s = z - ( h * 60 ** 2 ) - ( m * 60 ** 1 )
                            dictv[v[0]] = time(h, m, s)
                        elif v[1] == 'int':
                            dictv[v[0]] = int(vals[i])
                        elif v[1] == 'str':
                            if len(vals[i]) > v[2]:
                                raise ParserError(v[0] + ': String too long')
                            dictv[v[0]] = str(vals[i])
                        elif v[1] == 'bool':
                            if vals[i] != '0' and vals[i] != '1':
                                raise ParserError(v[0] + ': Unvalid boolean')
                            dictv[v[0]] = bool(vals[i])
                        elif v[1] == 'enum':
                            if not vals[i] in v[2]:
                                raise ParserError(v[0] + ': Value out of range')
                            dictv[v[0]] = str(vals[i])
                        else:
                            raise ParserError(v[0] + ': Unknown field type ' + v[1])
                        i += 1
                
                except Exception as e:
                    # Unable to parse line
                    log.error("Parse error on line (" + str(v[0]) + str(vals[i]) + "): got exception " + e + " (" + str(line) + ")")
                
                else:
                    # Line parsed correctly
                    log.debug("Correctly parsed 1 line: " + str(dictv))
                    
                    #Prepare dictv for query
                    map(lambda v: MySQLdb.string_literal(v), dictv)
                    dictv['table'] = MYSQL_DB['table']
                    
                    # Put the data into the DB
                    cursor = conn.cursor()
                    q = """
                        INSERT INTO `%(table)s` SET
                            `call_start` = '%(call_start)s',
                            `call_duration` = '%(call_duration)s',
                            `ring_duration` = '%(ring_duration)s',
                            `caller` = '%(caller)s',
                            `direction` = '%(direction)s',
                            `called_number` = '%(called_number)s',
                            `dialled_number` = '%(dialled_number)s',
                            `account` = '%(account)s',
                            `is_internal` = %(is_internal)d,
                            `call_id` = %(call_id)d,
                            `continuation` = %(continuation)d,
                            `paty1device` = '%(party1device)s',
                            `party1name` = '%(party1name)s',
                            `party2device` = '%(party2device)s',
                            `party2name` = '%(party2name)s',
                            `hold_time` = '%(hold_time)s',
                            `park_time` = '%(park_time)s',
                            `authvalid` = '%(authvalid)s',
                            `authcode` = '%(authcode)s',
                            `user_charged` = '%(user_charged)s',
                            `call_charge` = '%(call_charge)s',
                            `currency` = '%(currency)s',
                            `amount_change` = '%(amount_change)s',
                            `call_units` = '%(call_units)s',
                            `units_change` = '%(units_change)s',
                            `cost_per_unit` = '%(cost_per_unit)s',
                            `markup` = '%(markup)s';
                    """ % dictv
                    log.debug("Query: " + q)
                    cursor.execute(q)
                    cursor.close()
            
            else:
                log.error("Parse error on line (len " + str(len(vals)) + " vs " + str(len(fieldlist)) + "): " + line)


        # Connection terminated
        log.info(peerinfo[0] + ' (' + str(peerinfo[1]) + ') disconnected')


def exitcleanup(signum):
    print("Signal %s received, exiting..." % signum)
    server.server_close()
    sys.exit(0)   

def sighandler(signum = None, frame = None):
    exitcleanup(signum)


# Parse command line
usage = "%prog [options] <config_file>"
parser = OptionParser(usage=usage, version=NAME + ' ' + VERSION)
parser.add_option("-f", "--foreground", dest="foreground",
            help="Don't daemonize", action="store_true")
parser.add_option("-c", "--config", dest="configpath",
            help="Config file location (defaults to current dir)", action="store", default="freesmdr.conf")

(options, args) = parser.parse_args()


# Read from ini file the settings
configfile=options.configpath
config = configparser.ConfigParser()
if os.path.isfile(configfile):
    config.read(configfile)
else:
    print("Config file {configfile} doesn't exist".format(configfile=configfile))
    sys.exit(1)
# DB settings
connparams = config.items('database')
MYSQL_DB = dict(connparams)
# Progam settings
progsettings = dict(config.items('program'))
HOST = progsettings['host']
PORT = int(progsettings['port'])
LOGFILE = progsettings['logfile']
LOGINFO = progsettings['loginfo']


# Gracefully process signals
signal.signal(signal.SIGTERM, sighandler)
signal.signal(signal.SIGINT, sighandler)

# Fork & go to background
if not options.foreground:
    pid = os.fork()
else:
    pid = 0
if pid == 0:
    # 1st child
    if not options.foreground:
        os.setsid()
        pid = os.fork()
    if pid == 0:
        # 2nd child
        # Set up file logging
        logging.basicConfig(
            level = logging.DEBUG,
            format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            datefmt = '%Y-%m-%d %H:%M:%S',
            filename = LOGINFO,
            filemode = 'a'
        )

        if options.foreground:
            # Set up console logging
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
            console.setFormatter(formatter)
            logging.getLogger('').addHandler(console)
        
        # Create logger
        log = logging.getLogger()

        # Start server
        server_running = True
        server = TCPServer((HOST, PORT), RecvHandler)
        try:
            server.serve_forever()
        except Exception as e:
            log.critical("Got exception, crashing...")
            log.critical(e)
            log.critical(traceback.format_exc())
            raise e
        server.server_close()
        sys.exit(0)
    else:
        os._exit(0)
else:
    os._exit(0)
