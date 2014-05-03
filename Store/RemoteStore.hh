#pragma once

#include <string>
#include <vector>
#include <unordered_map>

#include "../gen-cpp/Cyclone.h"
#include "Store.hh"


class RemoteStore : public Store {
public:
  RemoteStore() = delete;
  RemoteStore(const RemoteStore& rhs) = delete;
  RemoteStore(const std::string& hostname, int port);
  virtual ~RemoteStore() = default;

  const RemoteStore& operator=(const RemoteStore& rhs) = delete;

  virtual std::unordered_map<std::string, std::string> update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior);
  virtual std::unordered_map<std::string, std::string> delete_series(
      const std::vector<std::string>& key_names);

  virtual std::unordered_map<std::string, ReadResult> read(
      const std::vector<std::string>& key_names, int64_t start_time,
      int64_t end_time);
  virtual std::unordered_map<std::string, std::string> write(
      const std::unordered_map<std::string, Series>& data);

  virtual std::unordered_map<std::string, FindResult> find(
      const std::vector<std::string>& patterns);

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);

private:
  std::shared_ptr<CycloneClient> client;
  std::shared_ptr<CycloneClient> get_client();

  std::string hostname;
  int port;
};
