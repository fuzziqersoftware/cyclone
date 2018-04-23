#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <deque>

#include "../gen-cpp/Cyclone.h"
#include "Store.hh"


class RemoteStore : public Store {
public:
  RemoteStore() = delete;
  RemoteStore(const RemoteStore& rhs) = delete;
  RemoteStore(const std::string& hostname, int port, size_t connection_cache_count);
  virtual ~RemoteStore() = default;
  const RemoteStore& operator=(const RemoteStore& rhs) = delete;

  size_t get_connection_cache_count() const;
  int get_port() const;
  std::string get_hostname() const;
  void set_connection_cache_count(size_t new_value);
  void set_netloc(const std::string& new_hostname, int new_port);

  virtual std::unordered_map<std::string, std::string> update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior, bool local_only);
  virtual std::unordered_map<std::string, int64_t> delete_series(
      const std::vector<std::string>& patterns, bool local_only);

  virtual std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>> read(
      const std::vector<std::string>& key_names, int64_t start_time,
      int64_t end_time, bool local_only);
  virtual std::unordered_map<std::string, std::string> write(
      const std::unordered_map<std::string, Series>& data, bool local_only);

  virtual std::unordered_map<std::string, FindResult> find(
      const std::vector<std::string>& patterns, bool local_only);

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
  std::atomic<size_t> connection_cache_count;
};
