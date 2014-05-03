#pragma once

#include <event2/event.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "Server.hh"


class HTTPServer : public Server {
public:
  HTTPServer() = delete;
  HTTPServer(const HTTPServer&) = delete;
  HTTPServer(HTTPServer&&) = delete;
  HTTPServer(size_t num_threads, uint64_t exit_check_usecs);
  virtual ~HTTPServer() = default;

  void listen(const std::string& path);
  void listen(const std::string& addr, int port);
  void add_socket(int fd);

  virtual void start();
  virtual void schedule_stop();
  virtual void wait_for_stop();

  static void send_response(struct evhttp_request* req, int code,
      const char* content_type, struct evbuffer* b);
  static void send_response(struct evhttp_request* req, int code,
      const char* content_type, const char* fmt, ...);

  static std::unordered_multimap<std::string, std::string> parse_url_params(
      const std::string& query);

  static const std::string& get_url_param(
      const std::unordered_multimap<std::string, std::string>& params,
      const std::string& key, const std::string* _default = NULL);

  static const std::unordered_map<int, const char*> explanation_for_response_code;

protected:
  class http_error : public std::runtime_error {
  public:
    http_error(int code, const std::string& what);
    int code;
  };

  struct Thread {
    HTTPServer* server;
    std::unique_ptr<struct event_base, void(*)(struct event_base*)> base;
    std::unique_ptr<struct event, void(*)(struct event*)> exit_check_event;
    std::unique_ptr<struct evhttp, void(*)(struct evhttp*)> http;
    std::thread t;

    Thread() = delete;
    Thread(const Thread& s) = delete;
    explicit Thread(HTTPServer* s);
    Thread(Thread&& s) = default;
    ~Thread();

    Thread& operator=(const Thread& other) = delete;

    static void dispatch_exit_check(evutil_socket_t fd, short what, void* ctx);
  };

  std::unordered_set<int> listen_fds;

  std::atomic<bool> should_exit;
  uint64_t exit_check_usecs;
  size_t num_threads;
  std::vector<Thread> threads;

  static void dispatch_handle_request(struct evhttp_request* req, void* ctx);
  virtual void handle_request(struct Thread& t, struct evhttp_request* req) = 0;

  void worker_thread_routine(HTTPServer::Thread* t);
};
