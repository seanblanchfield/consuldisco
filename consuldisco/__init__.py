name = "consuldisco"

import os
import socket
import requests

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


def discover_endpoints(service_name, consul_host=None, consul_port=None):
   ''' Looks up a named service in Consul and returns an array of tuples describing
       that service's endpoints in the form (hostname, IP, port).
   ''' 
   if consul_host is None and 'CONSUL_HOST' in os.environ:
      consul_host = os.environ['CONSUL_HOST']
   else:
      raise DiscoveryError("Environment variable 'CONSUL_HOST' must be provided when 'consul_host' parameter is not provided" % service_name)
   if consul_port is None and 'CONSUL_PORT' in os.environ:
      consul_port = os.environ['CONSUL_PORT']
   else:
      raise DiscoveryError("Environment variable 'CONSUL_PORT' must be provided when 'consul_port' parameter is not provided" % service_name)
   
   if service_name not in get_services(consul_host, consul_port):
      raise DiscoveryError("Service '%s' not known" % service_name)

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


def discover_endpoint(service_name, consul_host=None, consul_port=None):
   ''' Returns the first endpoint matching the provided service name.
   '''
   return discover_endpoints(service_name, consul_host, consul_port)
