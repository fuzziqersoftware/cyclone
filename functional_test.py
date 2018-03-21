import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

from cyclone_client.thrift_client import CycloneThriftClient
from cyclone_client.http_client import CycloneHTTPClient
from cyclone_client.stream_client import CycloneLineClient, CyclonePickleClient
from cyclone_client.common import KeyMetadata


def assert_eq(a, b):
  assert a == b, "%r != %r" % (a, b)


CONFIG_FILENAME = './cyclone-test.conf.json'
DATA_DIRECTORY = './cyclone-test-data'


def write_config(store_type='disk'):
  store_configs = {
    'disk': {
      'type': 'disk',
      'directory': DATA_DIRECTORY,
    },
    'cached_disk': {
      'type': 'cached_disk',
      'directory': DATA_DIRECTORY,
    },
    'buffered_cached_disk': {
      'type': 'write_buffer',
      'num_write_threads': 0,  # no auto-flush
      'batch_size': 5,
      'substore': {
        'type': 'cached_disk',
        'directory': DATA_DIRECTORY,
      },
    },
  }
  conf = {
    'http_listen': [5050],
    'line_stream_listen': [2003],
    'line_datagram_listen': [2003],
    'pickle_listen': [2004],
    'thrift_port': 2000,
    'http_threads': 1,
    'stream_threads': 1,
    'datagram_threads': 1,
    'thrift_threads': 1,
    'exit_check_usecs': 100000,
    'stats_report_usecs': 5000000,
    'store_config': store_configs[store_type],
    'autocreate_rules': [
      ["cyclone.**", "5:7d", 0, 1],
      ['test_autocreate.**.autokey', '60:90d', 0, 1],
      ["test_autocreate.*.autokey", "120:90d", 0, 1],
      ["**.autokey", "1800:90d", 0, 1],
    ],
  }
  with open(CONFIG_FILENAME, 'w') as f:
    json.dump(conf, f)


def run_functional_test(store_type):
  is_buffer = store_type.startswith('buffer')

  write_config(store_type=store_type)
  if not os.path.exists(DATA_DIRECTORY):
    os.mkdir(DATA_DIRECTORY)

  server = subprocess.Popen(['./cyclone', CONFIG_FILENAME])
  try:
    time.sleep(1)  # wait for startup

    now = time.time()

    print('-- [%s] connecting thrift' % store_type)
    cyclone_thrift = CycloneThriftClient('localhost', 2000)
    cyclone_http_json = CycloneHTTPClient('localhost', 5050, format='json')
    cyclone_http_pickle = CycloneHTTPClient('localhost', 5050, format='pickle')
    cyclone_stream_line = CycloneLineClient('localhost', 2003)
    cyclone_stream_pickle = CyclonePickleClient('localhost', 2004)
    assert_eq(cyclone_thrift.find(['*']), {'*': []})


    print('-- [%s] thrift update_metadata' % store_type)

    m = KeyMetadata()
    m.archive_args = [(60, 60 * 24 * 30), (60 * 60, 24 * 365)]
    m.x_files_factor = 0.5
    m.agg_method = 1

    ret = cyclone_thrift.update_metadata({'test.dir1.subdir1.key1': m},
        create_new=False, skip_existing=False, truncate_existing=False);
    assert_eq(ret, {'test.dir1.subdir1.key1': 'ignored' if not is_buffer else ''})
    assert_eq(cyclone_thrift.find(['*']), {'*': []})
    assert not os.path.exists(os.path.join(DATA_DIRECTORY, 'test'))

    ret = cyclone_thrift.update_metadata({'test.dir1.subdir1.key1': m},
        create_new=False, skip_existing=False, truncate_existing=True);
    assert_eq(ret, {'test.dir1.subdir1.key1': 'ignored' if not is_buffer else ''})
    assert_eq(cyclone_thrift.find(['*']), {'*': []})
    assert not os.path.exists(os.path.join(DATA_DIRECTORY, 'test'))

    # these should be 'ignored' even for buffer stores because they should merge
    # requests with the queue and notice that there's already a create request
    ret = cyclone_thrift.update_metadata({'test.dir1.subdir1.key1': m},
        create_new=False, skip_existing=True, truncate_existing=False);
    assert_eq(ret, {'test.dir1.subdir1.key1': 'ignored'})
    assert_eq(cyclone_thrift.find(['*']), {'*': []})
    assert not os.path.exists(os.path.join(DATA_DIRECTORY, 'test'))

    ret = cyclone_thrift.update_metadata({'test.dir1.subdir1.key1': m},
        create_new=False, skip_existing=True, truncate_existing=True);
    assert_eq(ret, {'test.dir1.subdir1.key1': 'ignored'})
    assert_eq(cyclone_thrift.find(['*']), {'*': []})
    assert not os.path.exists(os.path.join(DATA_DIRECTORY, 'test'))

    ret = cyclone_thrift.update_metadata({'test.dir1.subdir1.key1': m});
    assert_eq(ret, {'test.dir1.subdir1.key1': ''})
    assert_eq(cyclone_thrift.find(['*']), {'*': ['test.*']})
    assert_eq(cyclone_thrift.find(['test.*']), {'test.*': ['test.dir1.*']})
    assert_eq(cyclone_thrift.find(['test.dir1.*']),
        {'test.dir1.*': ['test.dir1.subdir1.*']})
    assert_eq(cyclone_thrift.find(['test.dir1.subdir1.*']),
        {'test.dir1.subdir1.*': ['test.dir1.subdir1.key1']})

    if not is_buffer:
      assert os.path.isfile(os.path.join(DATA_DIRECTORY,
          'test', 'dir1', 'subdir1', 'key1.wsp'))

    ret = cyclone_thrift.update_metadata({'test.dir1.subdir1.key1': m},
        create_new=False, skip_existing=True, truncate_existing=False);
    assert_eq(ret, {'test.dir1.subdir1.key1': 'ignored'})
    ret = cyclone_thrift.update_metadata({'test.dir1.subdir1.key1': m},
        create_new=False, skip_existing=True, truncate_existing=True);
    assert_eq(ret, {'test.dir1.subdir1.key1': 'ignored'})
    ret = cyclone_thrift.update_metadata({'test.dir1.subdir1.key1': m},
        create_new=True, skip_existing=True, truncate_existing=False);
    assert_eq(ret, {'test.dir1.subdir1.key1': 'ignored'})
    ret = cyclone_thrift.update_metadata({'test.dir1.subdir1.key1': m},
        create_new=True, skip_existing=True, truncate_existing=True);
    assert_eq(ret, {'test.dir1.subdir1.key1': 'ignored'})
    ret = cyclone_thrift.update_metadata({'test.dir1.subdir1.key1': m},
        create_new=False, skip_existing=False, truncate_existing=True);
    assert_eq(ret, {'test.dir1.subdir1.key1': ''})


    print('-- [%s] thrift read_metadata and thrift/http read (empty series)' % store_type)

    assert_eq(cyclone_thrift.read_metadata(['test.dir1.subdir1.key1']),
        {'test.dir1.subdir1.key1': m})
    for c in (cyclone_thrift, cyclone_http_json, cyclone_http_pickle):
      read_result = c.read(['test.dir1.subdir1.key1'], now - 60 * 10, now)
      assert_eq(read_result['test.dir1.subdir1.key1'].error, '')
      assert_eq(read_result['test.dir1.subdir1.key1'].data, [])
      if c is cyclone_thrift:  # non-thrift clients don't return metadata
        assert_eq(read_result['test.dir1.subdir1.key1'].metadata, m)


    print('-- [%s] thrift write and thrift/http read' % store_type)

    assert_eq(cyclone_thrift.write({'test.dir1.subdir1.key1': [(now, 2.0)]}),
        {'test.dir1.subdir1.key1': ''})
    expected_data = [((now // 60) * 60, 2.0)]
    for c in (cyclone_thrift, cyclone_http_json, cyclone_http_pickle):
      read_result = cyclone_thrift.read(['test.dir1.subdir1.key1'], now - 60 * 10, now)
      assert_eq(read_result['test.dir1.subdir1.key1'].error, '')
      assert_eq(read_result['test.dir1.subdir1.key1'].data, expected_data)
      if c is cyclone_thrift:  # non-thrift clients don't return metadata
        assert_eq(read_result['test.dir1.subdir1.key1'].metadata, m)


    print('-- [%s] stream (line) write and thrift/http read' % store_type)

    # unlike thrift writes, these writes are asynchronous, so we have to wait a
    # bit after sending to be sure the data gets written
    cyclone_stream_line.write({'test.dir1.subdir1.key1': [(now, 3.0)]})
    time.sleep(1)

    expected_data = [((now // 60) * 60, 3.0)]
    for c in (cyclone_thrift, cyclone_http_json, cyclone_http_pickle):
      read_result = cyclone_thrift.read(['test.dir1.subdir1.key1'], now - 60 * 10, now)
      assert_eq(read_result['test.dir1.subdir1.key1'].error, '')
      assert_eq(read_result['test.dir1.subdir1.key1'].data, expected_data)
      if c is cyclone_thrift:  # non-thrift clients don't return metadata
        assert_eq(read_result['test.dir1.subdir1.key1'].metadata, m)


    print('-- [%s] stream (pickle) write and thrift/http read' % store_type)

    # unlike thrift writes, these writes are asynchronous, so we have to wait a
    # bit after sending to be sure the data gets written
    cyclone_stream_pickle.write({'test.dir1.subdir1.key1': [(now, 4.0)]})
    time.sleep(1)

    expected_data = [((now // 60) * 60, 4.0)]
    for c in (cyclone_thrift, cyclone_http_json, cyclone_http_pickle):
      read_result = cyclone_thrift.read(['test.dir1.subdir1.key1'], now - 60 * 10, now)
      assert_eq(read_result['test.dir1.subdir1.key1'].error, '')
      assert_eq(read_result['test.dir1.subdir1.key1'].data, expected_data)
      if c is cyclone_thrift:  # non-thrift clients don't return metadata
        assert_eq(read_result['test.dir1.subdir1.key1'].metadata, m)


    print('-- [%s] thrift update_metadata on existing series, then read' % store_type)

    assert_eq(cyclone_thrift.read_metadata(['test.dir1.subdir1.key1']),
        {'test.dir1.subdir1.key1': m})
    m.x_files_factor = 1.0
    assert_eq(cyclone_thrift.update_metadata({'test.dir1.subdir1.key1': m}),
        {'test.dir1.subdir1.key1': ''})
    assert_eq(cyclone_thrift.read_metadata(['test.dir1.subdir1.key1']),
        {'test.dir1.subdir1.key1': m})

    read_result = cyclone_thrift.read(['test.dir1.subdir1.key1'], now - 60 * 10, now)
    assert_eq(read_result['test.dir1.subdir1.key1'].error, '')
    assert_eq(read_result['test.dir1.subdir1.key1'].data, expected_data)
    assert_eq(read_result['test.dir1.subdir1.key1'].metadata, m)


    print('-- [%s] thrift truncate series, then read' % store_type)

    assert_eq(cyclone_thrift.update_metadata({'test.dir1.subdir1.key1': m},
        truncate_existing=True), {'test.dir1.subdir1.key1': ''})
    assert_eq(cyclone_thrift.read_metadata(['test.dir1.subdir1.key1']),
        {'test.dir1.subdir1.key1': m})
    read_result = cyclone_thrift.read(['test.dir1.subdir1.key1'], now - 60 * 10, now)
    assert_eq(read_result['test.dir1.subdir1.key1'].error, '')
    assert_eq(read_result['test.dir1.subdir1.key1'].data, [])
    assert_eq(read_result['test.dir1.subdir1.key1'].metadata, m)


    print('-- [%s] thrift delete series' % store_type)

    assert_eq(cyclone_thrift.find(['*']), {'*': ['test.*']})
    assert_eq(cyclone_thrift.find(['test.*']), {'test.*': ['test.dir1.*']})
    assert_eq(cyclone_thrift.find(['test.dir1.*']),
        {'test.dir1.*': ['test.dir1.subdir1.*']})
    assert_eq(cyclone_thrift.find(['test.dir1.subdir1.*']),
        {'test.dir1.subdir1.*': ['test.dir1.subdir1.key1']})
    assert_eq(cyclone_thrift.delete_series(['test.dir1.subdir1.key1']),
        {'test.dir1.subdir1.key1': ''})
    assert_eq(cyclone_thrift.find(['*']), {'*': []})
    assert_eq(cyclone_thrift.find(['test.*']), {'test.*': []})
    assert_eq(cyclone_thrift.find(['test.dir1.*']), {'test.dir1.*': []})
    assert_eq(cyclone_thrift.find(['test.dir1.subdir1.*']),
        {'test.dir1.subdir1.*': []})

    print('-- [%s] check for stats' % store_type)
    if is_buffer:
      server.send_signal(signal.SIGUSR1)  # flush the buffer store
    time.sleep(3)  # must have been at least 5 seconds from server start
    if is_buffer:
      server.send_signal(signal.SIGUSR1)  # flush the buffer store again
      time.sleep(2)
    hostname = socket.gethostname()
    assert_eq(cyclone_thrift.find(['*']), {'*': ['cyclone.*']})
    stats_keys = cyclone_thrift.find(['cyclone.%s.*' % hostname])['cyclone.%s.*' % hostname]
    stats_read_result = cyclone_thrift.read(stats_keys, time.time() - 60, time.time())
    stats_data = {k.split('.')[-1]: int(r.data[0][1]) for k, r in stats_read_result.iteritems()}

    # the buffer store should do merges in memory, so some stats will show
    # different values (e.g. directory creates/deletes)
    assert(5000000 <= stats_data['duration'])
    assert_eq(0 if is_buffer else 3,  stats_data['directory_creates'])
    assert_eq(0 if is_buffer else 3,  stats_data['directory_deletes'])
    assert_eq(0,  stats_data['find_errors'])
    assert_eq(17, stats_data['find_patterns'])
    assert_eq(17, stats_data['find_requests'])
    assert_eq(0 if is_buffer else 8,  stats_data['find_results'])
    assert_eq(0 if is_buffer else 10, stats_data['read_datapoints'])
    assert_eq(0,  stats_data['read_errors'])
    assert_eq(18, stats_data['read_requests'])
    assert_eq(18, stats_data['read_series'])
    assert_eq(0,  stats_data['series_autocreates'])
    assert_eq(0 if is_buffer else 1,  stats_data['series_creates'])
    assert_eq(0 if is_buffer else 1,  stats_data['series_deletes'])
    assert_eq(0 if is_buffer else 2,  stats_data['series_truncates'])
    assert_eq(0 if is_buffer else 1,  stats_data['series_update_metadatas'])
    assert_eq(0 if is_buffer else 3,  stats_data['write_datapoints'])
    assert_eq(0,  stats_data['write_errors'])
    assert_eq(0 if is_buffer else 3,  stats_data['write_requests'])
    assert_eq(0 if is_buffer else 3,  stats_data['write_series'])

    assert not os.path.exists(os.path.join(DATA_DIRECTORY, 'test'))
    assert os.path.exists(DATA_DIRECTORY)

  finally:
    server.terminate()

    kill_time = time.time() + 10
    while server.poll() is None and time.time() < kill_time:
      time.sleep(1)
    if server.poll() is None:
      server.kill()

    shutil.rmtree(DATA_DIRECTORY)
    os.remove(CONFIG_FILENAME)


if __name__ == '__main__':
  run_functional_test(store_type='disk')
  run_functional_test(store_type='cached_disk')
  run_functional_test(store_type='buffered_cached_disk')
  print('all tests passed')
