#include "RemoteStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>
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
    size_t connection_cache_count) : Store(), netloc_token(now()),
    hostname(hostname), port(port),
    connection_cache_count(connection_cache_count) { }

size_t RemoteStore::get_connection_cache_count() const {
  return this->connection_cache_count;
}

int RemoteStore::get_port() const {
  return this->port;
}

std::string RemoteStore::get_hostname() const {
  return this->hostname;
}

void RemoteStore::set_connection_cache_count(size_t new_value) {
  this->connection_cache_count = new_value;
}

void RemoteStore::set_netloc(const std::string& new_hostname, int new_port) {
  lock_guard<mutex> g(this->clients_lock);
  this->hostname = new_hostname;
  this->port = new_port;
  this->netloc_token = now();
}

unordered_map<string, string> RemoteStore::update_metadata(
    const SeriesMetadataMap& metadata_map, bool create_new,
    UpdateMetadataBehavior update_behavior, bool local_only) {
  unordered_map<string, string> ret;
  if (local_only) {
    return ret;
  }

  auto c = this->get_client();
  try {
    c->client->update_metadata(ret, metadata_map, create_new,
        (update_behavior == UpdateMetadataBehavior::Ignore),
        (update_behavior == UpdateMetadataBehavior::Recreate), true);
    this->return_client(move(c));
  } catch (const apache::thrift::transport::TTransportException& e) {
    for (const auto& it : metadata_map) {
      ret.emplace(it.first, e.what());
    }
  }
  return ret;
}

unordered_map<string, int64_t> RemoteStore::delete_series(
    const vector<string>& patterns, bool local_only) {
  unordered_map<string, int64_t> ret;
  if (local_only) {
    return ret;
  }

  auto c = this->get_client();
  try {
    c->client->delete_series(ret, patterns, true);
    this->return_client(move(c));
  } catch (const apache::thrift::transport::TTransportException& e) {
    for (const auto& it : patterns) {
      ret.emplace(it, 0);
    }
  }
  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> RemoteStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time,
    bool local_only) {
  unordered_map<string, unordered_map<string, ReadResult>> ret;
  if (key_names.empty()) {
    return ret;
  }
  if (local_only) {
    return ret;
  }

  auto c = this->get_client();
  try {
    c->client->read(ret, key_names, start_time, end_time, true);
    this->return_client(move(c));
  } catch (const apache::thrift::transport::TTransportException& e) {
    for (const auto& it : key_names) {
      ret[it][it].error = e.what();
    }
  }
  return ret;
}

unordered_map<string, string> RemoteStore::write(
    const unordered_map<string, Series>& data, bool local_only) {
  unordered_map<string, string> ret;
  if (data.empty()) {
    return ret;
  }
  if (local_only) {
    return ret;
  }

  auto c = this->get_client();
  try {
    c->client->write(ret, data, true);
    this->return_client(move(c));
  } catch (const apache::thrift::transport::TTransportException& e) {
    for (const auto& it : data) {
      ret.emplace(it.first, e.what());
    }
  }
  return ret;
}

unordered_map<string, FindResult> RemoteStore::find(
    const vector<string>& patterns, bool local_only) {
  unordered_map<string, FindResult> ret;
  if (local_only) {
    return ret;
  }

  auto c = this->get_client();
  try {
    c->client->find(ret, patterns, true);
    this->return_client(move(c));
  } catch (const apache::thrift::transport::TTransportException& e) {
    for (const auto& it : patterns) {
      ret[it].error = e.what();
    }
  }
  return ret;
}

unordered_map<string, int64_t> RemoteStore::get_stats(bool rotate) {
  // TODO: add connection/request stats, timing, etc.
  return unordered_map<string, int64_t>();
}

int64_t RemoteStore::delete_from_cache(const std::string& path, bool local_only) {
  if (local_only) {
    return 0;
  }

  auto c = this->get_client();
  int64_t ret;
  try {
    ret = c->client->delete_from_cache(path, true);
    this->return_client(move(c));
  } catch (const apache::thrift::transport::TTransportException& e) {
    ret = 0;
  }
  return ret;
}

int64_t RemoteStore::delete_pending_writes(const std::string& pattern, bool local_only) {
  if (local_only) {
    return 0;
  }

  auto c = this->get_client();
  int64_t ret;
  try {
    ret = c->client->delete_pending_writes(pattern, true);
    this->return_client(move(c));
  } catch (const apache::thrift::transport::TTransportException& e) {
    ret = 0;
  }
  return ret;
}

string RemoteStore::str() const {
  return string_printf("RemoteStore(hostname=%s, port=%d, connection_cache_count=%zu)",
      this->hostname.c_str(), this->port, this->connection_cache_count.load());
}

std::unique_ptr<RemoteStore::Client> RemoteStore::get_client() {
  std::string hostname;
  int port;
  {
    lock_guard<mutex> g(this->clients_lock);
    if (!this->clients.empty()) {
      auto ret = move(this->clients.front());
      this->clients.pop_front();
      return ret;
    }

    // we have to do this while holding the lock because set_netloc can change
    // these values, and that's not thread-safe when using std::strings
    hostname = this->hostname;
    port = this->port;
  }

  // there are no clients; make a new one
  unique_ptr<Client> c(new Client());
  thrift_ptr<apache::thrift::transport::TSocket> socket(
      new apache::thrift::transport::TSocket(hostname, port));
  thrift_ptr<apache::thrift::transport::TTransport> trans(
      new apache::thrift::transport::TFramedTransport(socket));
  thrift_ptr<apache::thrift::protocol::TProtocol> proto(
      new apache::thrift::protocol::TBinaryProtocol(trans));
  trans->open();
  c->client = shared_ptr<CycloneClient>(new CycloneClient(proto));
  c->netloc_token = this->netloc_token;
  return c;
}

void RemoteStore::return_client(unique_ptr<RemoteStore::Client>&& client) {
  lock_guard<mutex> g(this->clients_lock);
  if (this->connection_cache_count &&
      (this->clients.size() >= this->connection_cache_count)) {
    return;
  }
  this->clients.emplace_back(move(client));
}
