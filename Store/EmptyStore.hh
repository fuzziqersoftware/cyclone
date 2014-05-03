#pragma once

#include <memory>
#include <unordered_map>

#include "Store.hh"


class EmptyStore : public Store {
public:
  EmptyStore() = default;
  EmptyStore(const EmptyStore& rhs) = delete;
  virtual ~EmptyStore() = default;

  const EmptyStore& operator=(const EmptyStore& rhs) = delete;

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
};
