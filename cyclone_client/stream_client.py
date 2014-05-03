import cPickle
import socket
import struct


class CycloneLineClient(object):
  def __init__(self, host, port):
    self.host = host
    self.port = port

  def write(self, key_to_datapoints):
    s = socket.create_connection((self.host, self.port))
    try:
      for key_name, datapoints in key_to_datapoints.iteritems():
        for timestamp, value in datapoints:
          s.sendall('%s %f %d\n' % (key_name, value, timestamp))
    finally:
      s.close()


class CyclonePickleClient(object):
  def __init__(self, host, port):
    self.host = host
    self.port = port

  def write(self, key_to_datapoints):
    data = [(k, (ts, v)) for k, d in key_to_datapoints.iteritems() for ts, v in d]
    payload = cPickle.dumps(data, protocol=cPickle.HIGHEST_PROTOCOL)
    s = socket.create_connection((self.host, self.port))
    try:
      s.sendall(struct.pack("!L", len(payload)) + payload)
    finally:
      s.close()
