name = "consuldisco"

import os
import time
import socket
import requests
import logging

logger = logging.getLogger(__name__)

class DiscoveryError(Exception):
    """Base class for exceptions in this module."""
    pass

def get_services(consul_host, consul_port):
   ''' Looks up available services in Consul.
   '''
   url = "http://%s:%d/v1/catalog/services" % (consul_host, int(consul_port))
   response = requests.get(url)
   response.raise_for_status()
   json = response.json()
   return list(json.keys())

def _get_consul(consul_host, consul_port):
   if consul_host is None:
      if 'CONSUL_HOST' in os.environ:
         consul_host = os.environ['CONSUL_HOST']
      else:
         raise DiscoveryError("Environment variable 'CONSUL_HOST' must be provided when 'consul_host' parameter is not provided")
   if consul_port is None:
      if 'CONSUL_PORT' in os.environ:
         consul_port = os.environ['CONSUL_PORT']
      else:
         raise DiscoveryError("Environment variable 'CONSUL_PORT' must be provided when 'consul_port' parameter is not provided")
   return consul_host, consul_port

def service_ready(service_name, consul_host=None, consul_port=None, wait=False, delay=0.1):
   '''Checks if service identified by 'service_name' is being advertised by Consul. If 'wait'
      is True, this function will block until it is ready, polling consul at an interval 
      given in 'delay'. If 'wait' is False (default), it will raise a DiscoveryError exception.

      Keyword arguments:
      service_name --the name of the service as advertised in Consul.
      consul_host -- the IP or host at which consul can be found. If not specified, this should be
                     specified in the environment variable 'CONSUL_HOST'. If not specified in either
                     location, a DiscoveryError will be raised.
      consul_port -- the port at which consul can be found. If not specified, this should be
                     specified in the environment variable 'CONSUL_PORT'. If not specified in either
                     location, a DiscoveryError will be raised.
      wait --        Specifies that if 'service_name' is not found in consul, to block until it is 
                     (default=False).
      delay --       The number of seconds to wait between rechecking consul for 'service_name' if it
                     was not found the first time, and 'wait' is True.
   ''' 
   consul_host, consul_port = _get_consul(consul_host, consul_port)
   service_ready = False
   while not service_ready:
      service_ready = service_name in get_services(consul_host, consul_port)
      if not service_ready:
         if wait:
            logger.info("Service {service_name} is not yet ready. Sleeping for {delay}s".format(service_name=service_name, delay=delay))
            time.sleep(delay)
         else:
            raise DiscoveryError("Service '%s' not available yet" % service_name)
      else:
         return True
      
def discover_endpoints(service_name, consul_host=None, consul_port=None, wait=False, delay=0.1):
   '''Looks up 'service_name' in Consul and returns an array of tuples describing
      that service's endpoints in the form (hostname, IP, port).

      Keyword arguments:
      service_name --the name of the service as advertised in Consul.
      consul_host -- the IP or host at which consul can be found. If not specified, this should be
                     specified in the environment variable 'CONSUL_HOST'. If not specified in either
                     location, a DiscoveryError will be raised.
      consul_port -- the port at which consul can be found. If not specified, this should be
                     specified in the environment variable 'CONSUL_PORT'. If not specified in either
                     location, a DiscoveryError will be raised.
      wait --        Specifies that if 'service_name' is not found in consul, to block until it is 
                     (default=False).
      delay --       The number of seconds to wait between rechecking consul for 'service_name' if it
                     was not found the first time, and 'wait' is True.
   ''' 
   consul_host, consul_port = _get_consul(consul_host, consul_port)
   
   service_ready(service_name, consul_host, consul_port, wait, delay)
   
   url = "http://%s:%d/v1/health/service/%s" % (consul_host, int(consul_port), service_name)
   response = requests.get(url)
   response.raise_for_status()
   endpoints = []
   json = response.json()
   for service in json:
      host = service['Service']['ID'].split(':')[1]
      IP = service['Service']['Address']
      port = service['Service']['Port']
      endpoints.append((host, IP, port))
   return endpoints


def discover_endpoint(service_name, consul_host=None, consul_port=None, wait=False, delay=0.1):
   ''' Returns the first endpoint matching the provided service name.

   Keyword arguments:
      service_name --the name of the service as advertised in Consul.
      consul_host -- the IP or host at which consul can be found. If not specified, this should be
                     specified in the environment variable 'CONSUL_HOST'. If not specified in either
                     location, a DiscoveryError will be raised.
      consul_port -- the port at which consul can be found. If not specified, this should be
                     specified in the environment variable 'CONSUL_PORT'. If not specified in either
                     location, a DiscoveryError will be raised.
      wait --        Specifies that if 'service_name' is not found in consul, to block until it is 
                     (default=False).
      delay --       The number of seconds to wait between rechecking consul for 'service_name' if it
                     was not found the first time, and 'wait' is True.
   '''
   return discover_endpoints(service_name, consul_host, consul_port, wait, delay)[0]
