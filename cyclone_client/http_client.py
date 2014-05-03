import cPickle
import json
import urllib
import threading


class ReadResult(object):
  def __init__(self, data, error=''):
    self.data = data
    self.metadata = None
    self.error = error


class CycloneHTTPClient(threading.local):
  def __init__(self, host, port, format='pickle'):
    assert format in ('json', 'pickle')

    self.host = host
    self.port = port
    self.format = format

  def read_metadata(self, key_names):
    results = {}
    for k, v in self.execute_command('read_metadata', key_names).iteritems():
      if v.error:
        raise RuntimeError(v)
      results[k] = metadata_from_thrift(v.metadata)
    return results

  def read(self, key_names, start_time=None, end_time=None):
    if end_time is None:
      end_time = time.time()
    if start_time is None:
      start_time = end_time - 60 * 60 * 24

    url = 'http://%s:%d/render/?format=%s&from=%d&until=%d' % (
      self.host, self.port, self.format, start_time, end_time)
    for key_name in key_names:
      url += '&target=%s' % key_name

    raw_data = urllib.urlopen(url).read()
    if self.format == 'json':
      return {d['target']: ReadResult(d['datapoints']) for d in json.loads(raw_data)}
    if self.format == 'pickle':
      return {d['pathExpression']: ReadResult(d['values']) for d in cPickle.loads(raw_data)}
    assert False, 'unknown format: %s' % self.format

  def find(self, patterns):
    # TODO
    raise NotImplementedError()
