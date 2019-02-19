#include "ThriftServer.hh"

#include <atomic>
#include <iostream>
#include <phosg/Strings.hh>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#ifdef _THRIFT_STDCXX_H_
#include <thrift/transport/TNonblockingServerSocket.h>
#endif

#include "../gen-cpp/Cyclone.h"

#ifdef _THRIFT_STDCXX_H_
#define thrift_ptr apache::thrift::stdcxx::shared_ptr
#else
#define thrift_ptr boost::shared_ptr
#endif

using namespace std;


class CycloneHandler : virtual public CycloneIf {
public:
  CycloneHandler(shared_ptr<Store> store, size_t num_threads,
      const vector<shared_ptr<ConsistentHashMultiStore>>& hash_stores,
      atomic<size_t>* idle_thread_count,
      const vector<shared_ptr<Server>>* all_servers) : store(store),
      hash_stores(hash_stores), idle_thread_count(idle_thread_count),
      all_servers(all_servers) { }

  ProfilerGuard create_profiler(const string& function_name) {
    string thread_name = string_printf("ThriftServer::serve (thread_id=%zu)",
        this_thread::get_id());
    return ProfilerGuard(::create_profiler(thread_name, function_name));
  }

  void update_metadata(unordered_map<string, Error>& _return,
      const SeriesMetadataMap& metadata, bool create_new,
      bool skip_existing_series, bool truncate_existing_series,
      bool skip_buffering, bool local_only) {
    BusyThreadGuard g(this->idle_thread_count);

    Store::UpdateMetadataBehavior update_behavior;
    if (skip_existing_series) {
      update_behavior = Store::UpdateMetadataBehavior::Ignore;
    } else {
      if (truncate_existing_series) {
        update_behavior = Store::UpdateMetadataBehavior::Recreate;
      } else {
        update_behavior = Store::UpdateMetadataBehavior::Update;
      }
    }

    auto pg = this->create_profiler(Store::string_for_update_metadata(
        metadata, create_new, update_behavior, skip_buffering, local_only));
    auto task = this->store->update_metadata(NULL, metadata,
        update_behavior, create_new, skip_buffering, local_only,
        pg.profiler.get());
    _return = task->value();
  }

  void delete_series(unordered_map<string, DeleteResult>& _return,
      const vector<string>& key_names, bool deferred, bool local_only) {
    BusyThreadGuard g(this->idle_thread_count);

    auto pg = this->create_profiler(Store::string_for_delete_series(
        key_names, deferred, local_only));
    auto task = this->store->delete_series(NULL, key_names, deferred,
        local_only, pg.profiler.get());
    _return = task->value();
  }

  void rename_series(unordered_map<string, Error>& _return,
      const unordered_map<string, string>& renames, bool merge, bool local_only) {
    BusyThreadGuard g(this->idle_thread_count);

    auto pg = this->create_profiler(Store::string_for_rename_series(renames,
        merge, local_only));
    auto task = this->store->rename_series(NULL, renames, merge, local_only,
        pg.profiler.get());
    _return = task->value();
  }

  void read(
      unordered_map<string, unordered_map<string, ReadResult>>& _return,
      const vector<string>& targets, const int64_t start_time,
      const int64_t end_time, bool local_only) {
    BusyThreadGuard g(this->idle_thread_count);

    auto pg = this->create_profiler(Store::string_for_read(targets,
        start_time, end_time, local_only));
    auto task = this->store->read(NULL, targets, start_time, end_time,
        local_only, pg.profiler.get());
    _return = task->value();
  }

  void read_all(ReadAllResult& _return, const string& key_name,
      bool local_only) {
    BusyThreadGuard g(this->idle_thread_count);

    auto pg = this->create_profiler(Store::string_for_read_all(key_name,
        local_only));
    auto task = this->store->read_all(NULL, key_name, local_only,
        pg.profiler.get());
    _return = task->value();
  }

  void write(unordered_map<string, Error>& _return,
      const unordered_map<string, Series>& data, bool skip_buffering,
      bool local_only) {
    BusyThreadGuard g(this->idle_thread_count);

    auto pg = this->create_profiler(Store::string_for_write(data,
        skip_buffering, local_only));
    auto task = this->store->write(NULL, data, skip_buffering,
        local_only, pg.profiler.get());
    _return = task->value();
  }

  void find(unordered_map<string, FindResult>& _return,
      const vector<string>& patterns, bool local_only) {
    BusyThreadGuard g(this->idle_thread_count);

    auto pg = this->create_profiler(Store::string_for_find(patterns,
        local_only));
    auto task = this->store->find(NULL, patterns, local_only,
        pg.profiler.get());
    _return = task->value();
  }

  void stats(unordered_map<string, int64_t>& _return) {
    BusyThreadGuard g(this->idle_thread_count);
    _return = gather_stats(this->store, *this->all_servers);
  }

  void get_verify_status(unordered_map<string, int64_t>& _return) {
    BusyThreadGuard g(this->idle_thread_count);

    if (this->hash_stores.size() != 1) {
      return;
    }

    const auto& progress = this->hash_stores[0]->get_verify_progress();
    _return.emplace("keys_examined", progress.keys_examined.load());
    _return.emplace("keys_moved", progress.keys_moved.load());
    _return.emplace("read_all_errors", progress.read_all_errors.load());
    _return.emplace("update_metadata_errors", progress.update_metadata_errors.load());
    _return.emplace("write_errors", progress.write_errors.load());
    _return.emplace("delete_errors", progress.delete_errors.load());
    _return.emplace("find_queries_executed", progress.find_queries_executed.load());
    _return.emplace("start_time", progress.start_time.load());
    _return.emplace("end_time", progress.end_time.load());
    _return.emplace("repair", progress.repair.load());
    _return.emplace("cancelled", progress.cancelled.load());
  }

  bool start_verify(bool repair) {
    BusyThreadGuard g(this->idle_thread_count);

    if (this->hash_stores.size() == 1) {
      return this->hash_stores[0]->start_verify(repair);
    }
    return false;
  }

  bool cancel_verify() {
    BusyThreadGuard g(this->idle_thread_count);

    if (this->hash_stores.size() == 1) {
      return this->hash_stores[0]->cancel_verify();
    }
    return false;
  }

  bool get_read_from_all() {
    BusyThreadGuard g(this->idle_thread_count);

    if (this->hash_stores.size() == 1) {
      return this->hash_stores[0]->get_read_from_all();
    }
    return false;
  }

  bool set_read_from_all(bool read_from_all) {
    BusyThreadGuard g(this->idle_thread_count);

    if (this->hash_stores.size() == 1) {
      return this->hash_stores[0]->set_read_from_all(read_from_all);
    }
    return false;
  }

private:
  shared_ptr<Store> store;
  vector<shared_ptr<ConsistentHashMultiStore>> hash_stores;
  atomic<size_t>* idle_thread_count;
  const vector<shared_ptr<Server>>* all_servers;
};



ThriftServer::ThriftServer(shared_ptr<Store> store,
    const vector<shared_ptr<ConsistentHashMultiStore>>& hash_stores, int port,
    size_t num_threads) : Server("thrift_server", num_threads), store(store),
    hash_stores(hash_stores), port(port), num_threads(num_threads), t() { }

void ThriftServer::start() {
  log(INFO, "[ThriftServer] listening on tcp port %d with %lu threads", this->port, this->num_threads);
  this->t = thread(&ThriftServer::serve_thread_routine, this);
}

void ThriftServer::schedule_stop() {
  // TODO: does this block?
  log(INFO, "[ThriftServer] scheduling exit");
  this->server->stop();
}

void ThriftServer::wait_for_stop() {
  log(INFO, "[ThriftServer] waiting for threads to terminate");
  this->t.join();
  log(INFO, "[ThriftServer] shutdown complete");
}

unordered_map<string, int64_t> ThriftServer::get_stats() {
  auto stats = this->Server::get_stats();
  if (this->server) {
    stats.emplace(this->stats_prefix + ".num_connections", this->server->getNumConnections());
    stats.emplace(this->stats_prefix + ".num_active_connections", this->server->getNumActiveConnections());
    stats.emplace(this->stats_prefix + ".num_idle_connections", this->server->getNumIdleConnections());
  }
  return stats;
}

void ThriftServer::serve_thread_routine() {
  if (this->num_threads == 0) {
    this->num_threads = thread::hardware_concurrency();
  }

  // oh god the namespaces... dat 80-char limit whyyyy

  thrift_ptr<apache::thrift::concurrency::ThreadManager> thread_manager =
      apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(num_threads);
  thrift_ptr<apache::thrift::concurrency::PosixThreadFactory> thread_factory(
      new apache::thrift::concurrency::PosixThreadFactory());
  thread_manager->threadFactory(thread_factory);
  thread_manager->start();

  thrift_ptr<apache::thrift::protocol::TProtocolFactory> protocol_factory(
      new apache::thrift::protocol::TBinaryProtocolFactory());

  thrift_ptr<CycloneHandler> handler(new CycloneHandler(this->store,
      this->num_threads, this->hash_stores, &this->idle_thread_count,
      &this->all_servers));
  thrift_ptr<CycloneProcessor::TProcessor> processor(new CycloneProcessor(handler));

  // TODO: unify these implementations
#ifdef _THRIFT_STDCXX_H_
  shared_ptr<apache::thrift::transport::TNonblockingServerSocket> socket(
      new apache::thrift::transport::TNonblockingServerSocket(this->port));
  this->server.reset(new apache::thrift::server::TNonblockingServer(
      processor, protocol_factory, socket, thread_manager));
#else
  this->server.reset(new apache::thrift::server::TNonblockingServer(
      processor, protocol_factory, port, thread_manager));
#endif
  this->server->setNumIOThreads(num_threads);
  this->server->serve();
}
