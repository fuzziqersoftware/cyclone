#pragma once

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "../gen-cpp/Cyclone.h"
#include "Store.hh"


class RemoteStore : public Store {
public:
  RemoteStore() = delete;
  RemoteStore(const RemoteStore& rhs) = delete;
  RemoteStore(const std::string& hostname, int port, size_t connection_cache_count);
  virtual ~RemoteStore() = default;

  const RemoteStore& operator=(const RemoteStore& rhs) = delete;

  virtual std::unordered_map<std::string, std::string> update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior);
  virtual std::unordered_map<std::string, std::string> delete_series(
      const std::vector<std::string>& key_names);

  virtual std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>> read(
      const std::vector<std::string>& key_names, int64_t start_time,
      int64_t end_time);
  virtual std::unordered_map<std::string, std::string> write(
      const std::unordered_map<std::string, Series>& data);

  virtual std::unordered_map<std::string, FindResult> find(
      const std::vector<std::string>& patterns);

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);

  virtual int64_t delete_from_cache(const std::string& path);
  virtual int64_t delete_pending_writes(const std::string& pattern);

  virtual std::string str() const;

private:
  std::mutex clients_lock;
  std::unordered_set<std::shared_ptr<CycloneClient>> clients;

  std::shared_ptr<CycloneClient> get_client();
  void return_client(std::shared_ptr<CycloneClient> client);

  std::string hostname;
  int port;
  size_t connection_cache_count;
};
