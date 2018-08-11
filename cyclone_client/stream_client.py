import pickle
import socket
import struct


class CycloneLineClient(object):
  """Cyclone line receiver client.

  The line receiver interface serves only write queries. In most cases the
  pickle client should be used instead since it's a more efficient way to send
  multiple datapoints.

  """

  def __init__(self, host, port):
    self.host = host
    self.port = port
    self.socket = None

  def __del__(self):
    if self.socket is not None:
      self.socket.close()

  def write(self, key_to_datapoints):
    """Sends datapoints to Cyclone (or Graphite).

    key_to_datapoints is a dict of {key_name: [(timestamp, value), ...]}.

    """
    if self.socket is None:
      self.socket = socket.create_connection((self.host, self.port))
    for key_name, datapoints in key_to_datapoints.iteritems():
      for timestamp, value in datapoints:
        self.socket.sendall('%s %f %d\n' % (key_name, value, timestamp))


class CyclonePickleClient(object):
  """Cyclone pickle client.

  The pickle interface serves only write queries.

  """

  def __init__(self, host, port):
    self.host = host
    self.port = port

  def write(self, key_to_datapoints):
    """Sends datapoints to Cyclone (or Graphite).

    key_to_datapoints is a dict of {key_name: [(timestamp, value), ...]}.

    """
    data = [(k, (ts, v)) for k, d in key_to_datapoints.iteritems() for ts, v in d]
    payload = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
    s = socket.create_connection((self.host, self.port))
    try:
      s.sendall(struct.pack("!L", len(payload)) + payload)
    finally:
      s.close()
