#pragma once

#include <memory>
#include <unordered_map>

#include "Store.hh"


class QueryStore : public Store {
public:
  QueryStore() = delete;
  QueryStore(const QueryStore& rhs) = delete;
  QueryStore(std::shared_ptr<Store> stores);
  virtual ~QueryStore() = default;

  const QueryStore& operator=(const QueryStore& rhs) = delete;

  virtual void set_autocreate_rules(
      const std::vector<std::pair<std::string, SeriesMetadata>> autocreate_rules);

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

protected:
  std::shared_ptr<Store> store;
};
