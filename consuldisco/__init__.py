name = "consuldisco"

import os
import time
import socket
import requests
import logging
import collections

logger = logging.getLogger(__name__)

class DiscoveryError(Exception):
    """Base class for exceptions in this module."""
    pass

def get_services(consul_host=None, consul_port=None):
   ''' Looks up available services in Consul.
   '''
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
   
   url = "http://%s:%d/v1/agent/services" % (consul_host, int(consul_port))
   response = requests.get(url)
   response.raise_for_status()
   services = collections.defaultdict(list)
   for consul_service in response.json().values():
      host = consul_service['ID'].split(':')[1]
      IP = consul_service['Address']
      port = consul_service['Port']
      service_name = consul_service['Service']
      services[service_name].append((host, IP, port))
   return services


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
   services = get_services(consul_host, consul_port)
   while service_name not in services:
      if wait:
         logger.info("Service {service_name} is not yet ready. Sleeping for {delay}s".format(service_name=service_name, delay=delay))
         time.sleep(delay)
         services = get_services(consul_host, consul_port)
      else:
         raise DiscoveryError("Service '%s' not available yet" % service_name)
   return services[service_name]


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
   endpoints = discover_endpoints(service_name, consul_host, consul_port, wait, delay)
   if len(endpoints) > 0:
      return endpoints[0]
   else:
      raise DiscoveryError("Service '%s' is advertised but has no endpoints" % service_name)
