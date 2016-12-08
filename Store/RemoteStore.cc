#include "RemoteStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <stdexcept>
#include <string>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <vector>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace std;


RemoteStore::RemoteStore(const string& hostname, int port) : Store(),
    hostname(hostname), port(port) { }

unordered_map<string, string> RemoteStore::update_metadata(
    const SeriesMetadataMap& metadata_map, bool create_new,
    UpdateMetadataBehavior update_behavior) {
  unordered_map<string, string> ret;
  this->get_client()->update_metadata(ret, metadata_map, create_new,
      (update_behavior == UpdateMetadataBehavior::Ignore),
      (update_behavior == UpdateMetadataBehavior::Recreate));
  return ret;
}

unordered_map<string, string> RemoteStore::delete_series(
    const vector<string>& key_names) {
  unordered_map<string, string> ret;
  this->get_client()->delete_series(ret, key_names);
  return ret;
}

unordered_map<string, ReadResult> RemoteStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time) {
  unordered_map<string, ReadResult> ret;
  this->get_client()->read(ret, key_names, start_time, end_time);
  return ret;
}

unordered_map<string, string> RemoteStore::write(
    const unordered_map<string, Series>& data) {
  unordered_map<string, string> ret;
  this->get_client()->write(ret, data);
  return ret;
}

unordered_map<string, FindResult> RemoteStore::find(
    const vector<string>& patterns) {
  unordered_map<string, FindResult> ret;
  this->get_client()->find(ret, patterns);
  return ret;
}

unordered_map<string, int64_t> RemoteStore::get_stats(bool rotate) {
  // TODO: add connection/request stats, timing, etc.
  return unordered_map<string, int64_t>();
}

int64_t RemoteStore::delete_from_cache(const std::string& path) {
  return this->get_client()->delete_from_cache(path);
}

int64_t RemoteStore::delete_pending_writes(const std::string& pattern) {
  return this->get_client()->delete_pending_writes(pattern);
}

shared_ptr<CycloneClient> RemoteStore::get_client() {
  if (!this->client) {
    boost::shared_ptr<TSocket> socket(new TSocket(this->hostname, this->port));
    boost::shared_ptr<TTransport> trans(new TFramedTransport(socket));
    boost::shared_ptr<TProtocol> proto(new TBinaryProtocol(trans));
    trans->open();
    this->client = shared_ptr<CycloneClient>(new CycloneClient(proto));
  }
  return this->client;
}
