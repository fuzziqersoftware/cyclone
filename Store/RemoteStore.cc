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

#include "Utils/Errors.hh"

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



class RemoteStore::RemoteStoreUpdateMetadataTask : public Store::UpdateMetadataTask {
public:
  virtual ~RemoteStoreUpdateMetadataTask() = default;
  RemoteStoreUpdateMetadataTask(StoreTaskManager* m, RemoteStore* s,
      shared_ptr<const UpdateMetadataArguments> args,
      BaseFunctionProfiler* profiler) : UpdateMetadataTask(m, args, profiler),
      store(s) {
    try {
      this->client = this->store->get_client();
      this->profiler->checkpoint("get_client");
      this->client->client->send_update_metadata(this->args->metadata,
          this->args->create_new,
          (this->args->behavior == UpdateMetadataBehavior::Ignore),
          (this->args->behavior == UpdateMetadataBehavior::Recreate),
          this->args->skip_buffering, true);
      this->read_suspend(this->client->socket->getSocketFD(), bind(
          &RemoteStoreUpdateMetadataTask::on_socket_readable, this));

    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
  }

  void on_socket_readable() {
    try {
      this->client->client->recv_update_metadata(this->return_value);
      this->set_complete();
      this->store->return_client(move(this->client));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
    this->store->stats[0].update_metadata_commands++;
  }

  void set_value_from_error(const exception& e) {
    this->profiler->checkpoint("remote_call");
    this->profiler->add_metadata("remote_error", e.what());
    this->store->stats[0].server_disconnects++;
    for (const auto& it : this->args->metadata) {
      this->return_value.emplace(it.first, make_error(e.what(), true));
    }
    this->set_complete();
  }

private:
  RemoteStore* store;
  unique_ptr<RemoteStore::Client> client;
};

shared_ptr<Store::UpdateMetadataTask> RemoteStore::update_metadata(StoreTaskManager* m,
    shared_ptr<const UpdateMetadataArguments> args, BaseFunctionProfiler* profiler) {
  static shared_ptr<UpdateMetadataTask> empty_result_task(new UpdateMetadataTask({}));
  if (args->metadata.empty()) {
    return empty_result_task;
  }
  if (args->local_only) {
    profiler->add_metadata("local_only", "true");
    return empty_result_task;
  }
  return shared_ptr<UpdateMetadataTask>(new RemoteStoreUpdateMetadataTask(m,
      this, args, profiler));
}



class RemoteStore::RemoteStoreDeleteSeriesTask : public Store::DeleteSeriesTask {
public:
  virtual ~RemoteStoreDeleteSeriesTask() = default;
  RemoteStoreDeleteSeriesTask(StoreTaskManager* m, RemoteStore* s,
      shared_ptr<const DeleteSeriesArguments> args,
      BaseFunctionProfiler* profiler) : DeleteSeriesTask(m, args, profiler),
      store(s) {
    try {
      this->client = this->store->get_client();
      this->profiler->checkpoint("get_client");
      this->client->client->send_delete_series(this->args->patterns,
          this->args->deferred, true);
      this->read_suspend(this->client->socket->getSocketFD(), bind(
          &RemoteStoreDeleteSeriesTask::on_socket_readable, this));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
  }

  void on_socket_readable() {
    try {
      this->client->client->recv_delete_series(this->return_value);
      this->set_complete();
      this->store->return_client(move(this->client));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
    this->store->stats[0].delete_series_commands++;
  }

  void set_value_from_error(const exception& e) {
    this->profiler->checkpoint("remote_call");
    this->profiler->add_metadata("remote_error", e.what());
    this->store->stats[0].server_disconnects++;
    for (const auto& it : this->args->patterns) {
      auto& res = this->return_value[it];
      res.disk_series_deleted = 0;
      res.buffer_series_deleted = 0;
      res.error = make_error(e.what());
    }
    this->set_complete();
  }

private:
  RemoteStore* store;
  unique_ptr<RemoteStore::Client> client;
};

shared_ptr<Store::DeleteSeriesTask> RemoteStore::delete_series(StoreTaskManager* m,
    shared_ptr<const DeleteSeriesArguments> args, BaseFunctionProfiler* profiler) {
  static shared_ptr<DeleteSeriesTask> empty_result_task(new DeleteSeriesTask({}));
  if (args->patterns.empty()) {
    return empty_result_task;
  }
  if (args->local_only) {
    profiler->add_metadata("local_only", "true");
    return empty_result_task;
  }
  return shared_ptr<DeleteSeriesTask>(new RemoteStoreDeleteSeriesTask(m, this,
      args, profiler));
}



class RemoteStore::RemoteStoreRenameSeriesTask : public Store::RenameSeriesTask {
public:
  virtual ~RemoteStoreRenameSeriesTask() = default;
  RemoteStoreRenameSeriesTask(StoreTaskManager* m, RemoteStore* s,
      shared_ptr<const RenameSeriesArguments> args,
      BaseFunctionProfiler* profiler) : RenameSeriesTask(m, args, profiler),
      store(s) {
    try {
      this->client = this->store->get_client();
      this->profiler->checkpoint("get_client");
      this->client->client->send_rename_series(this->args->renames,
          this->args->merge, true);
      this->read_suspend(this->client->socket->getSocketFD(), bind(
          &RemoteStoreRenameSeriesTask::on_socket_readable, this));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
  }

  void on_socket_readable() {
    try {
      this->client->client->recv_rename_series(this->return_value);
      this->set_complete();
      this->store->return_client(move(this->client));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
    this->store->stats[0].rename_series_commands++;
  }

  void set_value_from_error(const exception& e) {
    this->profiler->checkpoint("remote_call");
    this->profiler->add_metadata("remote_error", e.what());
    this->store->stats[0].server_disconnects++;
    for (const auto& it : this->args->renames) {
      this->return_value.emplace(it.first, make_error(e.what(), true));
    }
    this->set_complete();
  }

private:
  RemoteStore* store;
  unique_ptr<RemoteStore::Client> client;
};

shared_ptr<Store::RenameSeriesTask> RemoteStore::rename_series(StoreTaskManager* m,
    shared_ptr<const RenameSeriesArguments> args, BaseFunctionProfiler* profiler) {
  static shared_ptr<RenameSeriesTask> empty_result_task(new RenameSeriesTask({}));
  if (args->renames.empty()) {
    return empty_result_task;
  }
  if (args->local_only) {
    profiler->add_metadata("local_only", "true");
    return empty_result_task;
  }
  return shared_ptr<RenameSeriesTask>(new RemoteStoreRenameSeriesTask(m, this,
      args, profiler));
}



class RemoteStore::RemoteStoreReadTask : public Store::ReadTask {
public:
  virtual ~RemoteStoreReadTask() = default;
  RemoteStoreReadTask(StoreTaskManager* m, RemoteStore* s,
      shared_ptr<const ReadArguments> args, BaseFunctionProfiler* profiler) :
      ReadTask(m, args, profiler), store(s) {
    try {
      this->client = this->store->get_client();
      this->profiler->checkpoint("get_client");
      this->client->client->send_read(this->args->key_names,
          this->args->start_time, this->args->end_time, true);
      this->read_suspend(this->client->socket->getSocketFD(), bind(
          &RemoteStoreReadTask::on_socket_readable, this));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
  }

  void on_socket_readable() {
    try {
      this->client->client->recv_read(this->return_value);
      this->set_complete();
      this->store->return_client(move(this->client));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
    this->store->stats[0].read_commands++;
  }

  void set_value_from_error(const exception& e) {
    this->profiler->checkpoint("remote_call");
    this->profiler->add_metadata("remote_error", e.what());
    this->store->stats[0].server_disconnects++;
    for (const auto& it : this->args->key_names) {
      this->return_value[it][it].error = make_error(e.what(), true);
    }
    this->set_complete();
  }

private:
  RemoteStore* store;
  unique_ptr<RemoteStore::Client> client;
};

shared_ptr<Store::ReadTask> RemoteStore::read(StoreTaskManager* m,
    shared_ptr<const ReadArguments> args, BaseFunctionProfiler* profiler) {
  static shared_ptr<ReadTask> empty_result_task(new ReadTask({}));
  if (args->key_names.empty()) {
    return empty_result_task;
  }
  if (args->local_only) {
    profiler->add_metadata("local_only", "true");
    return empty_result_task;
  }
  return shared_ptr<RemoteStoreReadTask>(new RemoteStoreReadTask(m, this,
      args, profiler));
}



class RemoteStore::RemoteStoreReadAllTask : public Store::ReadAllTask {
public:
  virtual ~RemoteStoreReadAllTask() = default;
  RemoteStoreReadAllTask(StoreTaskManager* m, RemoteStore* s,
      shared_ptr<const ReadAllArguments> args, BaseFunctionProfiler* profiler) :
      ReadAllTask(m, args, profiler), store(s) {
    try {
      this->client = this->store->get_client();
      this->profiler->checkpoint("get_client");
      this->client->client->send_read_all(this->args->key_name, true);
      this->read_suspend(this->client->socket->getSocketFD(), bind(
          &RemoteStoreReadAllTask::on_socket_readable, this));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
  }

  void on_socket_readable() {
    try {
      this->client->client->recv_read_all(this->return_value);
      this->set_complete();
      this->store->return_client(move(this->client));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
    this->store->stats[0].read_all_commands++;
  }

  void set_value_from_error(const exception& e) {
    this->profiler->checkpoint("remote_call");
    this->profiler->add_metadata("remote_error", e.what());
    this->store->stats[0].server_disconnects++;
    this->return_value.error = make_error(e.what(), true);
    this->set_complete();
  }

private:
  RemoteStore* store;
  unique_ptr<RemoteStore::Client> client;
};

shared_ptr<Store::ReadAllTask> RemoteStore::read_all(StoreTaskManager* m,
    shared_ptr<const ReadAllArguments> args, BaseFunctionProfiler* profiler) {
  static shared_ptr<ReadAllTask> empty_result_task(new ReadAllTask(ReadAllResult()));
  if (args->local_only) {
    profiler->add_metadata("local_only", "true");
    return empty_result_task;
  }
  return shared_ptr<ReadAllTask>(new RemoteStoreReadAllTask(m, this, args,
      profiler));
}



class RemoteStore::RemoteStoreWriteTask : public Store::WriteTask {
public:
  virtual ~RemoteStoreWriteTask() = default;
  RemoteStoreWriteTask(StoreTaskManager* m, RemoteStore* s,
      shared_ptr<const WriteArguments> args, BaseFunctionProfiler* profiler) :
      WriteTask(m, args, profiler), store(s) {
    try {
      this->client = this->store->get_client();
      this->profiler->checkpoint("get_client");
      this->client->client->send_write(this->args->data,
          this->args->skip_buffering, true);
      this->read_suspend(this->client->socket->getSocketFD(), bind(
          &RemoteStoreWriteTask::on_socket_readable, this));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
  }

  void on_socket_readable() {
    try {
      this->client->client->recv_write(this->return_value);
      this->set_complete();
      this->store->return_client(move(this->client));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
    this->store->stats[0].write_commands++;
  }

  void set_value_from_error(const exception& e) {
    this->profiler->checkpoint("remote_call");
    this->profiler->add_metadata("remote_error", e.what());
    this->store->stats[0].server_disconnects++;
    for (const auto& it : this->args->data) {
      this->return_value.emplace(it.first, make_error(e.what(), true));
    }
    this->set_complete();
  }

private:
  RemoteStore* store;
  unique_ptr<RemoteStore::Client> client;
};

shared_ptr<Store::WriteTask> RemoteStore::write(StoreTaskManager* m,
    shared_ptr<const WriteArguments> args, BaseFunctionProfiler* profiler) {
  static shared_ptr<WriteTask> empty_result_task(new WriteTask({}));
  if (args->data.empty()) {
    return empty_result_task;
  }
  if (args->local_only) {
    profiler->add_metadata("local_only", "true");
    return empty_result_task;
  }
  return shared_ptr<RemoteStoreWriteTask>(new RemoteStoreWriteTask(m, this,
      args, profiler));
}



class RemoteStore::RemoteStoreFindTask : public Store::FindTask {
public:
  virtual ~RemoteStoreFindTask() = default;
  RemoteStoreFindTask(StoreTaskManager* m, RemoteStore* s,
      shared_ptr<const FindArguments> args, BaseFunctionProfiler* profiler) :
      FindTask(m, args, profiler), store(s) {
    try {
      this->client = this->store->get_client();
      this->profiler->checkpoint("get_client");
      this->client->client->send_find(this->args->patterns, true);
      this->read_suspend(this->client->socket->getSocketFD(), bind(
          &RemoteStoreFindTask::on_socket_readable, this));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
  }

  void on_socket_readable() {
    try {
      this->client->client->recv_find(this->return_value);
      this->set_complete();
      this->store->return_client(move(this->client));
    } catch (const exception& e) {
      this->set_value_from_error(e);
    }
    this->store->stats[0].find_commands++;
  }

  void set_value_from_error(const exception& e) {
    this->profiler->checkpoint("remote_call");
    this->profiler->add_metadata("remote_error", e.what());
    this->store->stats[0].server_disconnects++;
    for (const auto& it : this->args->patterns) {
      this->return_value[it].error = make_error(e.what(), true);
    }
    this->set_complete();
  }

private:
  RemoteStore* store;
  unique_ptr<RemoteStore::Client> client;
};

shared_ptr<Store::FindTask> RemoteStore::find(StoreTaskManager* m,
    shared_ptr<const FindArguments> args, BaseFunctionProfiler* profiler) {
  static shared_ptr<FindTask> empty_result_task(new FindTask({}));
  if (args->patterns.empty()) {
    return empty_result_task;
  }
  if (args->local_only) {
    profiler->add_metadata("local_only", "true");
    return empty_result_task;
  }
  return shared_ptr<FindTask>(new RemoteStoreFindTask(m, this, args, profiler));
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
  c->socket.reset(new apache::thrift::transport::TSocket(hostname, port));
  thrift_ptr<apache::thrift::transport::TTransport> trans(
      new apache::thrift::transport::TFramedTransport(c->socket));
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
  ret.emplace("remote_delete_pending_writes_commands", this->delete_pending_writes_commands.load());
  return ret;
}
