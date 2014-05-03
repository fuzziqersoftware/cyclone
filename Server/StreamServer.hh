#pragma once

#include <event2/event.h>

#include <atomic>
#include <memory>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "Server.hh"
#include "../Store/Store.hh"


class StreamServer : public Server {
private:
  struct Client {
    struct sockaddr remote_addr;
    int fd;
    bool is_pickle;

    Client(int fd, const struct sockaddr& remote_addr, bool is_pickle);
  };

  struct WorkerThread {
    StreamServer* server;
    int worker_num;
    std::unique_ptr<struct event_base, void(*)(struct event_base*)> base;
    std::unordered_set<std::unique_ptr<struct evconnlistener, void(*)(struct evconnlistener*)>> listeners;
    std::unordered_map<struct bufferevent*, Client> bev_to_client;
    std::thread t;

    WorkerThread(StreamServer* server, int worker_num);

    void disconnect_client(struct bufferevent* bev);

    static void dispatch_on_listen_accept(struct evconnlistener* listener,
        evutil_socket_t fd, struct sockaddr *address, int socklen, void* ctx);
    static void dispatch_on_listen_error(struct evconnlistener* listener, void* ctx);
    static void dispatch_on_client_input(struct bufferevent* bev, void* ctx);
    static void dispatch_on_client_error(struct bufferevent* bev, short events,
        void* ctx);
    static void dispatch_check_for_thread_exit(evutil_socket_t fd, short what, void* ctx);
  };

  std::atomic<bool> should_exit;
  uint64_t exit_check_usecs;
  std::vector<WorkerThread> threads;
  std::unordered_map<int, bool> listen_fd_to_is_pickle;
  std::shared_ptr<Store> store;

  void on_listen_accept(WorkerThread& wt, struct evconnlistener *listener,
      evutil_socket_t fd, struct sockaddr *address, int socklen);
  void on_listen_error(WorkerThread& wt, struct evconnlistener *listener);
  void on_client_input(WorkerThread& wt, struct bufferevent *bev);
  void on_client_error(WorkerThread& wt, struct bufferevent *bev, short events);
  void check_for_thread_exit(WorkerThread& wt, evutil_socket_t fd, short what);

  void run_thread(int thread_id);

public:
  StreamServer() = delete;
  StreamServer(const StreamServer&) = delete;
  StreamServer(StreamServer&&) = delete;
  StreamServer(std::shared_ptr<Store> store, size_t num_threads,
      uint64_t exit_check_usecs);
  virtual ~StreamServer() = default;

  void listen(const std::string& socket_path, bool is_pickle);
  void listen(const std::string& addr, int port, bool is_pickle);
  void listen(int port, bool is_pickle);
  void add_socket(int fd, bool is_pickle);

  virtual void start();
  virtual void schedule_stop();
  virtual void wait_for_stop();
};
