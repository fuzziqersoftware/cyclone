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
      const std::unordered_map<std::string, std::shared_ptr<Store>>& stores,
      int64_t precision);
  virtual ~ConsistentHashMultiStore() = default;
  const ConsistentHashMultiStore& operator=(const ConsistentHashMultiStore& rhs) = delete;

  int64_t get_precision() const;
  void set_precision(int64_t new_precision);

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

private:
  std::shared_ptr<ConsistentHashRing> ring;
  int64_t precision;

  void create_ring();
};
