#pragma once

#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <string>
#include <unordered_map>



class Server {
public:
  Server(const Server&) = delete;
  Server(Server&&) = delete;
  virtual ~Server() = default;

  virtual void start() = 0;
  virtual void schedule_stop() = 0;
  virtual void wait_for_stop() = 0;

  virtual std::unordered_map<std::string, int64_t> get_stats();

protected:
  Server(const std::string& stats_prefix, size_t thread_count);

  std::string stats_prefix;
  std::atomic<size_t> thread_count;
  std::atomic<size_t> idle_thread_count;
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
