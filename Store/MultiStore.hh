#pragma once

#include <memory>
#include <unordered_map>

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
  std::unordered_map<std::string, std::shared_ptr<Store>> stores;

  template <typename K, typename V>
  static void merge_maps(std::unordered_map<K, V>& dest,
      std::unordered_map<K, V>&& src, void (*merge_value)(const K&, V&, V&)) {
    for (auto& it : src) {
      if (!dest.emplace(it.first, std::move(it.second)).second) {
        V& existing_v = dest.at(it.first);
        merge_value(it.first, existing_v, it.second);
      }
    }
  }

  static void combine_error_strings(const std::string& k, std::string& v1,
      std::string& v2);
  static void combine_read_results(const std::string& k, ReadResult& v1,
      ReadResult& v2);
  static void combine_read_pattern_maps(const std::string& k,
      std::unordered_map<std::string, ReadResult>& v1,
      std::unordered_map<std::string, ReadResult>& v2);
  static void combine_find_results(const std::string& k, FindResult& v1,
      FindResult& v2);
};
