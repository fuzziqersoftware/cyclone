import pickle
import json
import urllib
import threading


class ReadResult(object):
  def __init__(self, data, error=''):
    self.data = data
    self.metadata = None
    self.error = error


class CycloneHTTPClient(threading.local):
  """Cyclone HTTP interface client.

  The HTTP interface serves only read queries. This client can request data from
  the server in either pickle or JSON format; generally pickle is faster but
  harder to debug.

  """

  def __init__(self, host, port, format='pickle'):
    assert format in ('json', 'pickle', None)

    self.host = host
    self.port = port
    self.format = format

  def read(self, key_names, start_time=None, end_time=None):
    """Reads data from the given keys.

    Returns a dict of {key_name: ReadResult}. ReadResult objects have these
    attributes:
    - data: a list of (timestamp, value) pairs.
    - metadata: the key's metadata.
    - error: if not empty, the error that occurred during reading.

    """
    if end_time is None:
      end_time = time.time()
    if start_time is None:
      start_time = end_time - 60 * 60 * 24

    format = self.format or 'pickle'
    url = 'http://%s:%d/render/?format=%s&from=%d&until=%d' % (
      self.host, self.port, format, start_time, end_time)
    for key_name in key_names:
      url += '&target=%s' % key_name

    raw_data = urllib.urlopen(url).read()
    if format == 'json':
      return {d['target']: ReadResult(d['datapoints']) for d in json.loads(raw_data)}
    if format == 'pickle':
      return {d['pathExpression']: ReadResult(d['values']) for d in pickle.loads(raw_data)}
    assert False, 'unknown format: %s' % format

  def read_all(self, key_name):
    """Reads all datapoints from the given key."""
    url = 'http://%s:%d/y/read-all?target=%s' % (self.host, self.port, key_name,)
    raw_data = urllib.urlopen(url).read()
    result = json.loads(raw_data)
    if isinstance(result, list):
      return result
    assert False, result

  def find(self, patterns):
    """Searches for keys matching the given patterns.

    When communicating with a Graphite server, this method only works if the
    format is 'pickle'. For Cyclone servers, both formats work. Furthermore,
    only one pattern may be given if the format is 'pickle'.

    Patterns may include:
    - [abc] (character class; matches any single character from the [])
    - {ab,cd} (substring set; matches any string in the {})
    - * (wildcard; matches any number of characters of any type except '.')

    Returns a dict of {pattern: [key_name, ...]}. If a returned key_name ends
    with ".*", then it represents a directory that matched the pattern rather
    than a key.

    """
    format = self.format or 'json'
    url = 'http://%s:%d/metrics/find/?local=1&format=%s' % (
        self.host, self.port, format)
    for pattern in patterns:
      url += '&query=%s' % pattern

    raw_data = urllib.urlopen(url).read()

    if format == 'json':
      return json.loads(raw_data)

    if format == 'pickle':
      ret = {patterns[0]: []}
      ret_records = ret[patterns[0]]
      for item in pickle.loads(raw_data):
        if item['isLeaf']:
          ret_records.append(item['metric_path'])
        else:
          ret_records.append(item['metric_path'] + '.*')
      return ret

    assert False, 'unknown format: %s' % format
