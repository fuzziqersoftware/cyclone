import socket
import threading

import thrift.transport.TTransport
import thrift.transport.TSocket
import thrift.protocol.TBinaryProtocol

from .cyclone_if import Cyclone as cyclone_service
from .cyclone_if import ttypes as cyclone_types
from .common import KeyMetadata


def metadata_to_thrift(m):
  archive_args = [cyclone_types.ArchiveArg(precision=x[0], points=x[1])
                  for x in m.archive_args]
  return cyclone_types.SeriesMetadata(archive_args=archive_args,
      x_files_factor=m.x_files_factor, agg_method=m.agg_method)

def metadata_from_thrift(m):
  ret = KeyMetadata()
  ret.archive_args = [(a.precision, a.points) for a in m.archive_args]
  ret.x_files_factor = m.x_files_factor
  ret.agg_method = m.agg_method
  return ret


class CycloneThriftClient(threading.local):
  """Cyclone thrift client.

  The Thrift interface serves both read and write queries. Unlike the other
  clients in this module, the Thrift client maintains a persistent connection to
  the server and automatically reconnects if it's disconnected.

  """

  def __init__(self, host, port, timeout=None):
    self.host = host
    self.port = port
    self.timeout = timeout
    self.socket = None
    self.client = None
    self._connect()

  def _connect(self):
    if self.socket:
      self.socket.close()

    self.socket = thrift.transport.TSocket.TSocket(self.host, self.port)
    if self.timeout is not None:
      self.socket.setTimeout(self.timeout)
    trans = thrift.transport.TTransport.TFramedTransport(self.socket)
    proto = thrift.protocol.TBinaryProtocol.TBinaryProtocolAccelerated(trans)
    self.client = cyclone_service.Client(proto)
    trans.open()

  def _execute_command(self, k, *args):
    try:
      return getattr(self.client, k)(*args)

    except (thrift.transport.TTransport.TTransportException, socket.error) as e:
      # if the connection was broken, reconnect and retry
      if isinstance(e, socket.error) and (e.errno != errno.EPIPE):
        raise

      self._connect()
      return getattr(self.client, k)(*args)

  def update_metadata(self, key_name_to_metadata, create_new=True,
      skip_existing=False, truncate_existing=False, local_only=False):
    """Updates series metadata, optionally creating the series.

    Arguments:
    - key_name_to_metadata: a dict of {key_name: KeyMetadata}.
    - create_new: if True, series that don't exist are created.
    - skip_existing: if True, series that already exist are ignored.
    - truncate_existing: if True, series that already exist are truncated.
    skip_existing takes precedence over truncate_existing; if both are True,
    existing series are not modified.

    Returns a dict of {key_name: error_string}. An empty error string means that
    the operation succeeded for that key.

    """
    x = {k: metadata_to_thrift(v) for k, v in key_name_to_metadata.items()}
    return self._execute_command('update_metadata', x, create_new, skip_existing,
        truncate_existing, local_only)

  def delete_series(self, key_names, local_only=False):
    """Deletes entire series.

    Returns a dict of {key_name: error_string}. An empty error string means that
    the operation succeeded for that key.

    """
    return self._execute_command('delete_series', key_names, local_only)

  def read_metadata(self, key_names, local_only=False):
    """Reads the metadata for the given keys.

    Returns a dict of {key_name: KeyMetadata}. Raises RuntimeError on failure.

    """
    results = {}
    for k, v in self._execute_command('read_metadata', key_names, local_only).items():
      if v.error:
        raise RuntimeError(v)
      results[k] = metadata_from_thrift(v.metadata)
    return results

  def read(self, key_names, start_time, end_time, local_only=False):
    """Reads data from the given keys.

    Returns a dict of {key_name: ReadResult}. ReadResult objects have these
    attributes:
    - data: a list of (timestamp, value) pairs.
    - metadata: the key's metadata.
    - error: if not empty, the error that occurred during reading.

    """
    thrift_results = self._execute_command('read', key_names, start_time,
        end_time, local_only)

    results = {}
    for k, v in thrift_results.items():
      if v.error:
        raise RuntimeError(v)
      v.metadata = metadata_from_thrift(v.metadata)
      v.data = [(d.timestamp, d.value) for d in v.data]
      results[k] = v
    return results

  def write(self, key_name_to_datapoints, local_only=False):
    """Sends datapoints to Cyclone.

    key_to_datapoints is a dict of {key_name: [(timestamp, value), ...]}.

    """
    thrift_args = {}
    for key_name, datapoints in key_name_to_datapoints.items():
      thrift_args[key_name] = [cyclone_types.Datapoint(timestamp=x[0], value=x[1]) for x in datapoints]

    return self._execute_command('write', thrift_args, local_only)

  def find(self, patterns, local_only=False):
    """Searches for keys matching the given patterns.

    Patterns may include:
    - [abc] (character class; matches any single character from the [])
    - {ab,cd} (substring set; matches any string in the {})
    - * (wildcard; matches any number of characters of any type except '.')

    Returns a dict of {pattern: [key_name, ...]}. If a returned key_name ends
    with ".*", then it represents a directory that matched the pattern rather
    than a key.

    """
    results = {}
    for k, v in self._execute_command('find', patterns, local_only).items():
      if v.error:
        raise RuntimeError(v)
      results[k] = v.results
    return results
