#pragma once

#include <memory>
#include <unordered_map>

#include "ConsistentHashRing.hh"
#include "Store.hh"


class MultiStore : public Store {
public:
  MultiStore() = delete;
  MultiStore(const MultiStore& rhs) = delete;
  MultiStore(const std::unordered_map<std::string, std::shared_ptr<Store>>& stores);
  virtual ~MultiStore() = default;

  const MultiStore& operator=(const MultiStore& rhs) = delete;

  virtual void set_autocreate_rules(
      const std::vector<std::pair<std::string, SeriesMetadata>> autocreate_rules);

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
  std::shared_ptr<ConsistentHashRing> ring;
  std::unordered_map<std::string, std::shared_ptr<Store>> stores;
};
