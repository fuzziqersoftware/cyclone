#pragma once

#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "../Store/Store.hh"



class Server {
public:
  Server(const Server&) = delete;
  Server(Server&&) = delete;
  virtual ~Server() = default;

  virtual void start() = 0;
  virtual void schedule_stop() = 0;
  virtual void wait_for_stop() = 0;

  virtual std::unordered_map<std::string, int64_t> get_stats();
  void set_servers_list(const std::vector<std::shared_ptr<Server>>& all_servers);

protected:
  Server(const std::string& stats_prefix, size_t thread_count);

  std::string stats_prefix;
  std::atomic<size_t> thread_count;
  std::atomic<size_t> idle_thread_count;

  std::vector<std::shared_ptr<Server>> all_servers;
};

class BusyThreadGuard {
public:
  explicit BusyThreadGuard(std::atomic<size_t>* idle_thread_count);
  ~BusyThreadGuard();

  BusyThreadGuard(const BusyThreadGuard&) = delete;
  BusyThreadGuard(BusyThreadGuard&&) = delete;
  BusyThreadGuard& operator=(const BusyThreadGuard&) = delete;
  BusyThreadGuard& operator=(BusyThreadGuard&&) = delete;

protected:
  std::atomic<size_t>* idle_thread_count;
};



// these are used by multiple servers and I don't know where else to put them
int64_t parse_relative_time(const std::string& s);
std::unordered_map<std::string, int64_t> gather_stats(
    std::shared_ptr<Store> store,
    const std::vector<std::shared_ptr<Server>>& all_servers);
