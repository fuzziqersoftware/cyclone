#pragma once

#include <memory>
#include <thread>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include "../gen-cpp/Cyclone.h"
#include "Server.hh"
#include "../Store/Store.hh"


class ThriftServer : public Server {
public:
  ThriftServer() = delete;
  ThriftServer(const ThriftServer&) = delete;
  ThriftServer(ThriftServer&&) = delete;
  ThriftServer(std::shared_ptr<Store> store, int port, size_t num_threads);
  virtual ~ThriftServer() = default;

  virtual void start();
  virtual void schedule_stop();
  virtual void wait_for_stop();

private:
  std::shared_ptr<Store> store;

  int port;

  size_t num_threads;
  std::thread t;
  std::shared_ptr<apache::thrift::server::TNonblockingServer> server;

  void serve_thread_routine();
};
