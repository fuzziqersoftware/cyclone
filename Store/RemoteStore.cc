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
    size_t connection_limit) : Store(), netloc_token(now()),
    hostname(hostname), port(port),
    connection_limit(connection_limit), stats(3) { }

size_t RemoteStore::get_connection_limit() const {
  return this->connection_limit;
}

int RemoteStore::get_port() const {
  return this->port;
}

std::string RemoteStore::get_hostname() const {
  return this->hostname;
}

void RemoteStore::set_connection_limit(size_t new_value) {
  this->connection_limit = new_value;
}

void RemoteStore::set_netloc(const std::string& new_hostname, int new_port) {
  lock_guard<mutex> g(this->clients_lock);
  this->hostname = new_hostname;
  this->port = new_port;
  this->netloc_token = now();
}

unordered_map<string, string> RemoteStore::update_metadata(
    const SeriesMetadataMap& metadata_map, bool create_new,
    UpdateMetadataBehavior update_behavior, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  unordered_map<string, string> ret;
  if (local_only) {
    profiler->add_metadata("local_only", "true");
    return ret;
  }

  try {
    auto c = this->get_client();
    profiler->checkpoint("get_client");
    c->client->update_metadata(ret, metadata_map, create_new,
        (update_behavior == UpdateMetadataBehavior::Ignore),
        (update_behavior == UpdateMetadataBehavior::Recreate), skip_buffering,
        true);
    profiler->checkpoint("remote_call");
    this->return_client(move(c));

  } catch (const exception& e) {
    profiler->checkpoint("remote_call");
    profiler->add_metadata("remote_error", e.what());
    this->stats[0].server_disconnects++;
    for (const auto& it : metadata_map) {
      ret.emplace(it.first, e.what());
    }
  }
  this->stats[0].update_metadata_commands++;
  return ret;
}

unordered_map<string, int64_t> RemoteStore::delete_series(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, int64_t> ret;
  if (local_only) {
    profiler->add_metadata("local_only", "true");
    return ret;
  }

  try {
    auto c = this->get_client();
    profiler->checkpoint("get_client");
    c->client->delete_series(ret, patterns, true);
    profiler->checkpoint("remote_call");
    this->return_client(move(c));

  } catch (const exception& e) {
    profiler->checkpoint("remote_call");
    profiler->add_metadata("remote_error", e.what());
    this->stats[0].server_disconnects++;
    for (const auto& it : patterns) {
      ret.emplace(it, 0);
    }
  }
  this->stats[0].delete_series_commands++;
  return ret;
}

unordered_map<string, string> RemoteStore::rename_series(
    const unordered_map<string, string>& renames, bool local_only,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, string> ret;
  if (local_only) {
    profiler->add_metadata("local_only", "true");
    return ret;
  }

  try {
    auto c = this->get_client();
    profiler->checkpoint("get_client");
    c->client->rename_series(ret, renames, true);
    profiler->checkpoint("remote_call");
    this->return_client(move(c));

  } catch (const exception& e) {
    profiler->checkpoint("remote_call");
    profiler->add_metadata("remote_error", e.what());
    this->stats[0].server_disconnects++;
    for (const auto& it : renames) {
      ret.emplace(it.first, e.what());
    }
  }
  this->stats[0].rename_series_commands++;
  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> RemoteStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time,
    bool local_only, BaseFunctionProfiler* profiler) {
  unordered_map<string, unordered_map<string, ReadResult>> ret;
  if (key_names.empty()) {
    return ret;
  }
  if (local_only) {
    profiler->add_metadata("local_only", "true");
    return ret;
  }

  try {
    auto c = this->get_client();
    profiler->checkpoint("get_client");
    c->client->read(ret, key_names, start_time, end_time, true);
    profiler->checkpoint("remote_call");
    this->return_client(move(c));

  } catch (const exception& e) {
    profiler->checkpoint("remote_call");
    profiler->add_metadata("remote_error", e.what());
    this->stats[0].server_disconnects++;
    for (const auto& it : key_names) {
      ret[it][it].error = e.what();
    }
  }
  this->stats[0].read_commands++;
  return ret;
}

ReadAllResult RemoteStore::read_all(const string& key_name, bool local_only,
    BaseFunctionProfiler* profiler) {
  ReadAllResult ret;
  if (local_only) {
    profiler->add_metadata("local_only", "true");
    return ret;
  }

  try {
    auto c = this->get_client();
    profiler->checkpoint("get_client");
    c->client->read_all(ret, key_name, true);
    profiler->checkpoint("remote_call");
    this->return_client(move(c));

  } catch (const exception& e) {
    profiler->checkpoint("remote_call");
    profiler->add_metadata("remote_error", e.what());
    this->stats[0].server_disconnects++;
    ret.error = e.what();
  }
  this->stats[0].read_all_commands++;
  return ret;
}

unordered_map<string, string> RemoteStore::write(
    const unordered_map<string, Series>& data, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  unordered_map<string, string> ret;
  if (data.empty()) {
    return ret;
  }
  if (local_only) {
    profiler->add_metadata("local_only", "true");
    return ret;
  }

  try {
    auto c = this->get_client();
    profiler->checkpoint("get_client");
    c->client->write(ret, data, skip_buffering, true);
    profiler->checkpoint("remote_call");
    this->return_client(move(c));

  } catch (const exception& e) {
    profiler->checkpoint("remote_call");
    profiler->add_metadata("remote_error", e.what());
    this->stats[0].server_disconnects++;
    for (const auto& it : data) {
      ret.emplace(it.first, e.what());
    }
  }
  this->stats[0].write_commands++;
  return ret;
}

unordered_map<string, FindResult> RemoteStore::find(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, FindResult> ret;
  if (local_only) {
    profiler->add_metadata("local_only", "true");
    return ret;
  }

  try {
    auto c = this->get_client();
    profiler->checkpoint("get_client");
    c->client->find(ret, patterns, true);
    profiler->checkpoint("remote_call");
    this->return_client(move(c));

  } catch (const exception& e) {
    profiler->checkpoint("remote_call");
    profiler->add_metadata("remote_error", e.what());
    this->stats[0].server_disconnects++;
    for (const auto& it : patterns) {
      ret[it].error = e.what();
    }
  }
  this->stats[0].find_commands++;
  return ret;
}

unordered_map<string, int64_t> RemoteStore::get_stats(bool rotate) {
  const Stats& current_stats = this->stats[0];

  if (rotate) {
    this->stats.rotate();

    uint64_t n = now();
    this->stats[0].start_time = n;
    this->stats[1].duration = n - this->stats[1].start_time;
  }

  size_t client_count;
  {
    lock_guard<mutex> g(this->clients_lock);
    client_count = this->clients.size();
  }

  auto ret = current_stats.to_map();
  ret.emplace("connection_count", client_count);
  ret.emplace("connection_limit", this->connection_limit.load());
  return ret;
}

int64_t RemoteStore::delete_from_cache(const std::string& path, bool local_only) {
  if (local_only) {
    return 0;
  }

  int64_t ret;
  try {
    auto c = this->get_client();
    ret = c->client->delete_from_cache(path, true);
    this->return_client(move(c));
  } catch (const exception& e) {
    this->stats[0].server_disconnects++;
    ret = 0;
  }
  this->stats[0].delete_from_cache_commands++;
  return ret;
}

int64_t RemoteStore::delete_pending_writes(const std::string& pattern, bool local_only) {
  if (local_only) {
    return 0;
  }

  int64_t ret;
  try {
    auto c = this->get_client();
    ret = c->client->delete_pending_writes(pattern, true);
    this->return_client(move(c));
  } catch (const exception& e) {
    this->stats[0].server_disconnects++;
    ret = 0;
  }
  this->stats[0].delete_pending_writes_commands++;
  return ret;
}

string RemoteStore::str() const {
  return string_printf("RemoteStore(hostname=%s, port=%d, connection_limit=%zu)",
      this->hostname.c_str(), this->port, this->connection_limit.load());
}

std::unique_ptr<RemoteStore::Client> RemoteStore::get_client() {
  std::string hostname;
  int port;
  {
    lock_guard<mutex> g(this->clients_lock);
    while (!this->clients.empty()) {
      auto ret = move(this->clients.front());
      this->clients.pop_front();

      // don't return any clients that were created before the netloc changed
      if (ret->netloc_token == this->netloc_token) {
        return ret;
      }
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
  this->stats[0].connects++;
  return c;
}

void RemoteStore::return_client(unique_ptr<RemoteStore::Client>&& client) {
  // don't return a client that was created before the netloc changed
  if (client->netloc_token != this->netloc_token) {
    this->stats[0].client_disconnects++;
    return;
  }

  lock_guard<mutex> g(this->clients_lock);
  if (this->connection_limit &&
      (this->clients.size() >= this->connection_limit)) {
    this->stats[0].client_disconnects++;
    return;
  }

  this->clients.emplace_back(move(client));
}

RemoteStore::Stats::Stats() : Store::Stats::Stats(),
    connects(0),
    server_disconnects(0),
    client_disconnects(0),
    update_metadata_commands(0),
    delete_series_commands(0),
    rename_series_commands(0),
    read_commands(0),
    read_all_commands(0),
    write_commands(0),
    find_commands(0),
    restore_series_commands(0),
    serialize_series_commands(0),
    delete_from_cache_commands(0),
    delete_pending_writes_commands(0) { }


// TODO: figure out if we can make this less dumb
RemoteStore::Stats& RemoteStore::Stats::operator=(const Stats& other) {
  this->Store::Stats::operator=(other);
  this->connects = other.connects.load();
  this->server_disconnects = other.server_disconnects.load();
  this->client_disconnects = other.client_disconnects.load();
  this->update_metadata_commands = other.update_metadata_commands.load();
  this->delete_series_commands = other.delete_series_commands.load();
  this->rename_series_commands = other.rename_series_commands.load();
  this->read_commands = other.read_commands.load();
  this->read_all_commands = other.read_all_commands.load();
  this->write_commands = other.write_commands.load();
  this->find_commands = other.find_commands.load();
  this->restore_series_commands = other.restore_series_commands.load();
  this->serialize_series_commands = other.serialize_series_commands.load();
  this->delete_from_cache_commands = other.delete_from_cache_commands.load();
  this->delete_pending_writes_commands = other.delete_pending_writes_commands.load();
  return *this;
}

unordered_map<string, int64_t> RemoteStore::Stats::to_map() const {
  unordered_map<string, int64_t> ret = this->Store::Stats::to_map();
  ret.emplace("connects", this->connects.load());
  ret.emplace("server_disconnects", this->server_disconnects.load());
  ret.emplace("client_disconnects", this->client_disconnects.load());
  ret.emplace("remote_update_metadata_commands", this->update_metadata_commands.load());
  ret.emplace("remote_delete_series_commands", this->delete_series_commands.load());
  ret.emplace("remote_rename_series_commands", this->rename_series_commands.load());
  ret.emplace("remote_read_commands", this->read_commands.load());
  ret.emplace("remote_read_all_commands", this->read_all_commands.load());
  ret.emplace("remote_write_commands", this->write_commands.load());
  ret.emplace("remote_find_commands", this->find_commands.load());
  ret.emplace("remote_restore_series_commands", this->restore_series_commands.load());
  ret.emplace("remote_serialize_series_commands", this->serialize_series_commands.load());
  ret.emplace("remote_delete_from_cache_commands", this->delete_from_cache_commands.load());
  ret.emplace("remote_delete_pending_writes_commands", this->delete_pending_writes_commands.load());
  return ret;
}
