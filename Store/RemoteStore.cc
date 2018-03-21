#include "RemoteStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <phosg/Strings.hh>
#include <stdexcept>
#include <string>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <vector>

#ifdef _THRIFT_STDCXX_H_
#define thrift_ptr apache::thrift::stdcxx::shared_ptr
#else
#define thrift_ptr boost::shared_ptr
#endif

using namespace std;


RemoteStore::RemoteStore(const string& hostname, int port,
    size_t connection_cache_count) : Store(), clients_lock(), clients(),
    hostname(hostname), port(port),
    connection_cache_count(connection_cache_count) { }

unordered_map<string, string> RemoteStore::update_metadata(
    const SeriesMetadataMap& metadata_map, bool create_new,
    UpdateMetadataBehavior update_behavior) {
  unordered_map<string, string> ret;

  auto c = this->get_client();
  c->update_metadata(ret, metadata_map, create_new,
      (update_behavior == UpdateMetadataBehavior::Ignore),
      (update_behavior == UpdateMetadataBehavior::Recreate));
  this->return_client(c);
  return ret;
}

unordered_map<string, string> RemoteStore::delete_series(
    const vector<string>& key_names) {
  unordered_map<string, string> ret;
  auto c = this->get_client();
  c->delete_series(ret, key_names);
  this->return_client(c);
  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> RemoteStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time) {
  unordered_map<string, unordered_map<string, ReadResult>> ret;
  auto c = this->get_client();
  c->read(ret, key_names, start_time, end_time);
  this->return_client(c);
  return ret;
}

unordered_map<string, string> RemoteStore::write(
    const unordered_map<string, Series>& data) {
  unordered_map<string, string> ret;
  auto c = this->get_client();
  this->get_client()->write(ret, data);
  this->return_client(c);
  return ret;
}

unordered_map<string, FindResult> RemoteStore::find(
    const vector<string>& patterns) {
  unordered_map<string, FindResult> ret;
  auto c = this->get_client();
  this->get_client()->find(ret, patterns);
  this->return_client(c);
  return ret;
}

unordered_map<string, int64_t> RemoteStore::get_stats(bool rotate) {
  // TODO: add connection/request stats, timing, etc.
  return unordered_map<string, int64_t>();
}

int64_t RemoteStore::delete_from_cache(const std::string& path) {
  auto c = this->get_client();
  int64_t ret = c->delete_from_cache(path);
  this->return_client(c);
  return ret;
}

int64_t RemoteStore::delete_pending_writes(const std::string& pattern) {
  auto c = this->get_client();
  int64_t ret = c->delete_pending_writes(pattern);
  this->return_client(c);
  return ret;
}

string RemoteStore::str() const {
  return string_printf("RemoteStore(hostname=%s, port=%d, connection_cache_count=%zu)",
      this->hostname.c_str(), this->port, this->connection_cache_count);
}

shared_ptr<CycloneClient> RemoteStore::get_client() {
  {
    lock_guard<mutex> g(this->clients_lock);
    auto it = this->clients.begin();
    if (it != this->clients.end()) {
      auto ret = *it;
      this->clients.erase(it);
      return ret;
    }
  }

  // there are no clients; make a new one
  thrift_ptr<apache::thrift::transport::TSocket> socket(
      new apache::thrift::transport::TSocket(this->hostname, this->port));
  thrift_ptr<apache::thrift::transport::TTransport> trans(
      new apache::thrift::transport::TFramedTransport(socket));
  thrift_ptr<apache::thrift::protocol::TProtocol> proto(
      new apache::thrift::protocol::TBinaryProtocol(trans));
  trans->open();
  return shared_ptr<CycloneClient>(new CycloneClient(proto));
}

void RemoteStore::return_client(shared_ptr<CycloneClient> client) {
  lock_guard<mutex> g(this->clients_lock);
  if (this->connection_cache_count &&
      (this->clients.size() >= this->connection_cache_count)) {
    return;
  }
  this->clients.insert(client);
}
