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
#include "../Store/ConsistentHashMultiStore.hh"


class StreamServer : public Server {
public:
  enum class Protocol {
    Line = 0,
    Pickle,
    Shell,
  };

private:
  struct Client {
    struct sockaddr remote_addr;
    int fd;
    Protocol protocol;

    Client(int fd, const struct sockaddr& remote_addr, Protocol protocol);
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
  std::unordered_map<int, Protocol> listen_fd_to_protocol;
  std::shared_ptr<Store> store;
  std::vector<std::shared_ptr<ConsistentHashMultiStore>> hash_stores;

  void on_listen_accept(WorkerThread& wt, struct evconnlistener *listener,
      evutil_socket_t fd, struct sockaddr *address, int socklen);
  void on_listen_error(WorkerThread& wt, struct evconnlistener *listener);
  void on_client_input(WorkerThread& wt, struct bufferevent *bev);
  void on_client_error(WorkerThread& wt, struct bufferevent *bev, short events);
  void check_for_thread_exit(WorkerThread& wt, evutil_socket_t fd, short what);

  void execute_shell_command(const char* command, struct evbuffer* out_buffer);

  void run_thread(int thread_id);

public:
  // note: StreamServer needs to know about the hash stores because it supports
  // a shell command to start and check on verify procedures
  StreamServer() = delete;
  StreamServer(const StreamServer&) = delete;
  StreamServer(StreamServer&&) = delete;
  StreamServer(std::shared_ptr<Store> store,
      const std::vector<std::shared_ptr<ConsistentHashMultiStore>>& hash_stores,
      size_t num_threads, uint64_t exit_check_usecs);
  virtual ~StreamServer() = default;

  void listen(const std::string& socket_path, Protocol protocol);
  void listen(const std::string& addr, int port, Protocol protocol);
  void listen(int port, Protocol protocol);
  void add_socket(int fd, Protocol protocol);

  virtual void start();
  virtual void schedule_stop();
  virtual void wait_for_stop();
};
