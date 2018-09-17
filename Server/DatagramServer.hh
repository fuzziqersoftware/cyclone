#pragma once

#include <event2/event.h>

#include <atomic>
#include <memory>
#include <phosg/Concurrency.hh>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "Server.hh"
#include "../Store/Store.hh"

class DatagramServer : public Server {
private:
  struct WorkerThread {
    DatagramServer* server;
    int worker_num;
    std::unique_ptr<struct event_base, void(*)(struct event_base*)> base;
    std::unordered_map<int, std::unique_ptr<struct event, void(*)(struct event*)>> fd_to_event;
    std::thread t;

    WorkerThread(DatagramServer* server, int worker_num);

    void check_for_thread_exit(int fd, short events);
  };

  std::atomic<bool> should_exit;
  std::vector<WorkerThread> threads;
  std::unordered_set<int> fds;
  std::shared_ptr<Store> store;

  static void dispatch_on_client_input(int fd, short int events, void *ctx);
  static void dispatch_check_for_thread_exit(evutil_socket_t fd, short what, void* ctx);
  void on_client_input(int fd, short int events);

  void run_thread(int thread_id);

public:
  DatagramServer() = delete;
  DatagramServer(std::shared_ptr<Store> store, size_t num_threads);
  ~DatagramServer() = default;

  void listen(const std::string& socket_path);
  void listen(const std::string& addr, int port);
  void listen(int port);
  void add_socket(int fd);

  void start();
  void schedule_stop();
  void wait_for_stop();
};
