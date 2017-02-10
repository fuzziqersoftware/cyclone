#pragma once

#include <memory>
#include <phosg/ConsistentHashRing.hh>
#include <unordered_map>

#include "Store.hh"
#include "MultiStore.hh"


class ConsistentHashMultiStore : public MultiStore {
public:
  ConsistentHashMultiStore() = delete;
  ConsistentHashMultiStore(const ConsistentHashMultiStore& rhs) = delete;
  ConsistentHashMultiStore(
      const std::unordered_map<std::string, std::shared_ptr<Store>>& stores);
  virtual ~ConsistentHashMultiStore() = default;

  const ConsistentHashMultiStore& operator=(const ConsistentHashMultiStore& rhs) = delete;

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

private:
  std::shared_ptr<ConsistentHashRing> ring;
};
