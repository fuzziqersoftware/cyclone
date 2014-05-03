#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "../gen-cpp/Cyclone.h"


class Store {
public:
  Store(const Store& rhs) = delete;
  virtual ~Store() = default;

  const Store& operator=(const Store& rhs) = delete;

  // sync with UpdateMetadataBehavior in cyclone_if.thrift
  enum UpdateMetadataBehavior {
    // values passed to update_metadata to determine behavior on existing series
    Update = 0, // update existing series, preserving data
    Ignore = 1, // make no changes to existing series
    Recreate = 2, // update existing series, discarding data
  };

  virtual void set_autocreate_rules(
      const std::vector<std::pair<std::string, SeriesMetadata>> autocreate_rules);

  virtual std::unordered_map<std::string, std::string> update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior) = 0;
  virtual std::unordered_map<std::string, std::string> delete_series(
      const std::vector<std::string>& key_names) = 0;

  virtual std::unordered_map<std::string, ReadResult> read(
      const std::vector<std::string>& key_names, int64_t start_time,
      int64_t end_time) = 0;
  virtual std::unordered_map<std::string, std::string> write(
      const std::unordered_map<std::string, Series>& data) = 0;

  virtual std::unordered_map<std::string, FindResult> find(
      const std::vector<std::string>& patterns) = 0;

  virtual void flush();

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);

  static bool token_is_pattern(const std::string& token);
  static bool pattern_is_basename(const std::string& token);
  static bool pattern_is_indeterminate(const std::string& pattern);
  static bool name_matches_pattern(const std::string& name,
      const std::string& pattern, size_t name_offset = 0,
      size_t pattern_offset = 0);

protected:
  Store() = default;
};
