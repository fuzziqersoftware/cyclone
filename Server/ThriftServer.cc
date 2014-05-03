#include "ThriftServer.hh"

#include <iostream>
#include <phosg/Strings.hh>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include "../gen-cpp/Cyclone.h"

using namespace std;


class CycloneHandler : virtual public CycloneIf {
public:
  CycloneHandler(shared_ptr<Store> store) : store(store) { }

  void update_metadata(unordered_map<string, string>& _return,
      const SeriesMetadataMap& metadata, bool create_new,
      bool skip_existing_series, bool truncate_existing_series) {
 
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
    _return = this->store->update_metadata(metadata, create_new, update_behavior);
  }

  void delete_series(unordered_map<string, string>& _return,
      const vector<string>& key_names) {
    _return = this->store->delete_series(key_names);
  }

  void read_metadata(unordered_map<string, ReadResult>& _return,
      const vector<string>& targets) {
    _return = this->store->read(targets, 0, 0);
  }

  void read(unordered_map<string, ReadResult>& _return,
      const vector<string>& targets, const int64_t start_time,
      const int64_t end_time) {
    _return = this->store->read(targets, start_time, end_time);
  }

  void write(unordered_map<string, string>& _return,
      const unordered_map<string, Series>& data) {
    _return = this->store->write(data);
  }

  void find(unordered_map<string, FindResult>& _return, const vector<string>& patterns) {
    _return = this->store->find(patterns);
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

  boost::shared_ptr<apache::thrift::concurrency::ThreadManager> thread_manager =
      apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(num_threads);
  boost::shared_ptr<apache::thrift::concurrency::PosixThreadFactory> thread_factory(
      new apache::thrift::concurrency::PosixThreadFactory());
  thread_manager->threadFactory(thread_factory);
  thread_manager->start();

  boost::shared_ptr<apache::thrift::protocol::TProtocolFactory> protocol_factory(
      new apache::thrift::protocol::TBinaryProtocolFactory());

  boost::shared_ptr<CycloneHandler> handler(new CycloneHandler(this->store));
  boost::shared_ptr<CycloneProcessor::TProcessor> processor(new CycloneProcessor(handler));

  this->server.reset(new apache::thrift::server::TNonblockingServer(
      processor, protocol_factory, port, thread_manager));
  this->server->setNumIOThreads(num_threads);
  this->server->serve();
}
