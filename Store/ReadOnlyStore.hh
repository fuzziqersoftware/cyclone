#pragma once

#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "Store.hh"


class ReadOnlyStore : public Store {
public:
  ReadOnlyStore() = delete;
  ReadOnlyStore(const ReadOnlyStore& rhs) = delete;
  ReadOnlyStore(std::shared_ptr<Store> store);
  virtual ~ReadOnlyStore() = default;

  const ReadOnlyStore& operator=(const ReadOnlyStore& rhs) = delete;

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

  virtual int64_t delete_from_cache(const std::string& path);

private:
  std::shared_ptr<Store> store;
};
