#!/usr/bin/env python
# -*- coding: utf-8 -*-

# agorrdtool
# Addon that log environment event on rrdtool database
# It also generates on the fly rrdtool graphs
# copyright (c) 2014 tang (tanguy.bonneau@gmail.com) 
 
import sys
import os
import agoclient
import threading
import time
import logging
import json
import base64
from qpid.datatypes import uuid4

client = None
server = None
units = {}
devices = {}
rooms = {}
EVENT_BLACKLIST = ['event.environment.timechanged']

from socket import socket

CARBON_SERVER = agoclient.get_config_option("graphite", "carbon_server", "127.0.0.1")
CARBON_PORT = agoclient.get_config_option("graphite", "carbon_port", "2003")
PATH_PREFIX = agoclient.get_config_option("graphite", "path_prefix", "home")
sock = socket()
try:
  sock.connect( (CARBON_SERVER,CARBON_PORT) )
except:
  print "Couldn't connect to %(server)s on port %(port)d, is carbon-agent.py running?" % { 'server':CARBON_SERVER, 'port':CARBON_PORT }
  sys.exit(1)

#logging.basicConfig(filename='/opt/agocontrol/agoscheduler.log', level=logging.INFO, format="%(asctime)s %(levelname)s : %(message)s")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s : %(message)s")

#=================================
#classes
#=================================


#=================================
#utils
#=================================
def quit(msg):
    """Exit application"""
    global client, server
    if client:
        del client
        client = None
    if server:
        #server.socket.close()
        server.stop()
        del server
        server = None
    logging.fatal(msg)
    sys.exit(0)

def getScenarioControllerUuid():
    """get scenariocontroller uuid"""
    global client, scenarioControllerUuid
    inventory = client.get_inventory()
    for uuid in inventory.content['devices']:
        if inventory.content['devices'][uuid]['devicetype']=='scenariocontroller':
            scenarioControllerUuid = uuid
            break
    if not scenarioControllerUuid:
        raise Exception('scenariocontroller uuid not found!')

#=================================
#functions
#=================================
def commandHandler(internalid, content):
    """command handler"""
    logging.info('commandHandler: %s, %s' % (internalid,content))
    global client
    command = None

    if content.has_key('command'):
        command = content['command']
    else:
        logging.error('No command specified')
        return None

    logging.warning('Unsupported command received: internalid=%s content=%s' % (internalid, content))
    return None

def makeToken(s):
    return s.replace(" ", "_").replace(".", "_").replace("\t", "_")

def getGraphitePath(event, device):
    try:
        name = device['uuid']
        if 'internalid' in device and device['internalid']:
            name = device['internalid']
        if 'name' in device and device['name']:
            name = device['name']
        valueName = event.split('.')[-1][:-len('changed')]
        return ".".join(map(makeToken, (PATH_PREFIX, "agocontrol", "v1", device["handled-by"], rooms[device["room"]]['name'], name, valueName)))
    except Exception as e:
        print "exception", e
        print device
        return None

def eventHandler(event, content):
    """ago event handler"""
    global client

    if event in EVENT_BLACKLIST: return
    #format event.environment.humiditychanged, {u'uuid': '506249e2-1852-4de7-8554-93f5b9354a20', u'unit': '', u'level': 49.8}
    if event == "event.device.announce":
        if content['uuid'] not in devices:
            devices[content['uuid']] = {'uuid': content['uuid']}
        devices[content['uuid']].update(content)
    else:
        if 'uuid' not in content or 'level' not in content:
            logging.info('eventHandler: %s, %s' % (event, content))
            return
        if content['uuid'] not in devices:
            logging.info('unknown device: ' + content['uuid'])
            return
        path = getGraphitePath(event, devices[content['uuid']])
        if not path: return
        carbon_message = "%s %s %d\n" % (path, content['level'], int(time.time()))
        print carbon_message
        sock.sendall(carbon_message)

#=================================
#main
#=================================
#init
try:
    #connect agoclient
    client = agoclient.AgoConnection('agographite')

    #get units
    inventory = client.get_inventory()
    rooms = inventory.content['rooms']
    rooms[u''] = {'location': '', 'name':u'UnknownRoom'}
    devices = inventory.content['devices']
    for uuid in devices:
        devices[uuid]['uuid'] = uuid
    for unit in inventory.content['schema']['units']:
        units[unit] = inventory.content['schema']['units'][unit]['label']

    #add client handlers
    client.add_handler(commandHandler)
    client.add_event_handler(eventHandler)

except Exception as e:
    #init failed
    logging.exception("Exception on init")
    quit('Init failed, exit now.')


#run agoclient
try:
    logging.info('Running agographite...')
    client.run()
except KeyboardInterrupt:
    #stopped by user
    quit('agographite stopped by user')
except Exception as e:
    logging.exception("Exception on main:")
    #stop everything
    quit('agographite stopped')

