#include "ThriftServer.hh"

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
  CycloneHandler(shared_ptr<Store> store) : store(store) { }

  void update_metadata(unordered_map<string, string>& _return,
      const SeriesMetadataMap& metadata, bool create_new,
      bool skip_existing_series, bool truncate_existing_series, bool local_only) {

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
    _return = this->store->update_metadata(metadata, create_new, update_behavior, local_only);
  }

  void delete_series(unordered_map<string, int64_t>& _return,
      const vector<string>& key_names, bool local_only) {
    _return = this->store->delete_series(key_names, local_only);
  }

  void read(
      unordered_map<string, unordered_map<string, ReadResult>>& _return,
      const vector<string>& targets, const int64_t start_time,
      const int64_t end_time, bool local_only) {
    _return = this->store->read(targets, start_time, end_time, local_only);
  }

  void write(unordered_map<string, string>& _return,
      const unordered_map<string, Series>& data, bool local_only) {
    _return = this->store->write(data, local_only);
  }

  void find(unordered_map<string, FindResult>& _return,
      const vector<string>& patterns, bool local_only) {
    _return = this->store->find(patterns, local_only);
  }

  void stats(unordered_map<string, int64_t>& _return) {
    _return = this->store->get_stats();
  }

  int64_t delete_from_cache(const std::string& path, bool local_only) {
    return this->store->delete_from_cache(path, local_only);
  }

  int64_t delete_pending_writes(const std::string& pattern, bool local_only) {
    return this->store->delete_pending_writes(pattern, local_only);
  }

private:
  shared_ptr<Store> store;
};

ThriftServer::ThriftServer(shared_ptr<Store> store, int port,
    size_t num_threads) : store(store), port(port), num_threads(num_threads),
    t() { }

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

  thrift_ptr<CycloneHandler> handler(new CycloneHandler(this->store));
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
