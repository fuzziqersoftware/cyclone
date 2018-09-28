#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <deque>

#include "../gen-cpp/Cyclone.h"
#include "Store.hh"
#include "FixedAtomicRotator.hh"


class RemoteStore : public Store {
public:
  RemoteStore() = delete;
  RemoteStore(const RemoteStore& rhs) = delete;
  RemoteStore(const std::string& hostname, int port, size_t connection_limit);
  virtual ~RemoteStore() = default;
  const RemoteStore& operator=(const RemoteStore& rhs) = delete;

  size_t get_connection_limit() const;
  int get_port() const;
  std::string get_hostname() const;
  void set_connection_limit(size_t new_value);
  void set_netloc(const std::string& new_hostname, int new_port);

  virtual std::unordered_map<std::string, std::string> update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler);
  virtual std::unordered_map<std::string, int64_t> delete_series(
      const std::vector<std::string>& patterns, bool local_only,
      BaseFunctionProfiler* profiler);
  virtual std::unordered_map<std::string, std::string> rename_series(
      const std::unordered_map<std::string, std::string>& renames,
      bool local_only, BaseFunctionProfiler* profiler);

  virtual std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>> read(
      const std::vector<std::string>& key_names, int64_t start_time,
      int64_t end_time, bool local_only, BaseFunctionProfiler* profiler);
  virtual ReadAllResult read_all(const std::string& key_name, bool local_only,
      BaseFunctionProfiler* profiler);
  virtual std::unordered_map<std::string, std::string> write(
      const std::unordered_map<std::string, Series>& data, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler);

  virtual std::unordered_map<std::string, FindResult> find(
      const std::vector<std::string>& patterns, bool local_only,
      BaseFunctionProfiler* profiler);

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);

  virtual int64_t delete_from_cache(const std::string& path, bool local_only);
  virtual int64_t delete_pending_writes(const std::string& pattern, bool local_only);

  virtual std::string str() const;

private:
  struct Client {
    std::shared_ptr<CycloneClient> client;
    int64_t netloc_token;
  };

  std::mutex clients_lock;
  std::deque<std::unique_ptr<Client>> clients;
  std::atomic<int64_t> netloc_token;

  std::unique_ptr<Client> get_client();
  void return_client(std::unique_ptr<Client>&& client);

  std::string hostname;
  int port;
  std::atomic<size_t> connection_limit;

  struct Stats : public Store::Stats {
    std::atomic<size_t> connects;
    std::atomic<size_t> server_disconnects;
    std::atomic<size_t> client_disconnects;
    std::atomic<size_t> update_metadata_commands;
    std::atomic<size_t> delete_series_commands;
    std::atomic<size_t> rename_series_commands;
    std::atomic<size_t> read_commands;
    std::atomic<size_t> read_all_commands;
    std::atomic<size_t> write_commands;
    std::atomic<size_t> find_commands;
    std::atomic<size_t> restore_series_commands;
    std::atomic<size_t> serialize_series_commands;
    std::atomic<size_t> delete_from_cache_commands;
    std::atomic<size_t> delete_pending_writes_commands;

    Stats();
    Stats& operator=(const Stats& other);

    std::unordered_map<std::string, int64_t> to_map() const;
  };

  FixedAtomicRotator<Stats> stats;
};
