#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <phosg/Concurrency.hh>
#include <vector>

#include "../gen-cpp/Cyclone.h"
#include "Utils/FunctionProfiler.hh"


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
      const std::vector<std::pair<std::string, SeriesMetadata>>& autocreate_rules);

  virtual std::unordered_map<std::string, Error> update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler) = 0;
  virtual std::unordered_map<std::string, DeleteResult> delete_series(
      const std::vector<std::string>& patterns, bool local_only,
      BaseFunctionProfiler* profiler) = 0;
  virtual std::unordered_map<std::string, Error> rename_series(
      const std::unordered_map<std::string, std::string>& renames,
      bool merge, bool local_only, BaseFunctionProfiler* profiler) = 0;

  virtual std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>> read(
      const std::vector<std::string>& key_names, int64_t start_time,
      int64_t end_time, bool local_only, BaseFunctionProfiler* profiler) = 0;
  virtual ReadAllResult read_all(const std::string& key_name, bool local_only,
      BaseFunctionProfiler* profiler) = 0;
  virtual std::unordered_map<std::string, Error> write(
      const std::unordered_map<std::string, Series>& data, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler) = 0;

  virtual std::unordered_map<std::string, FindResult> find(
      const std::vector<std::string>& patterns, bool local_only,
      BaseFunctionProfiler* profiler) = 0;

  virtual void flush();

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);

  virtual int64_t delete_from_cache(const std::string& paths, bool local_only);
  virtual int64_t delete_pending_writes(const std::string& paths, bool local_only);



  static std::string string_for_update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior, bool skip_buffering,
      bool local_only);
  static std::string string_for_delete_series(
      const std::vector<std::string>& patterns, bool local_only);
  static std::string string_for_rename_series(
      const std::unordered_map<std::string, std::string>& renames,
      bool merge, bool local_only);
  static std::string string_for_read(const std::vector<std::string>& key_names,
      int64_t start_time, int64_t end_time, bool local_only);
  static std::string string_for_read_all(const std::string& key_name,
      bool local_only);
  static std::string string_for_write(
      const std::unordered_map<std::string, Series>& data, bool skip_buffering,
      bool local_only);
  static std::string string_for_find(const std::vector<std::string>& patterns,
      bool local_only);



  virtual std::string str() const = 0;

  static bool token_is_pattern(const std::string& token);
  static bool pattern_is_basename(const std::string& token);
  static bool pattern_is_indeterminate(const std::string& pattern);
  static bool name_matches_pattern(const std::string& name,
      const std::string& pattern, size_t name_offset = 0,
      size_t pattern_offset = 0);

  static bool key_char_is_valid(char ch);
  static bool key_name_is_valid(const std::string& key_name);

protected:
  Store() = default;

  std::vector<std::pair<std::string, SeriesMetadata>> autocreate_rules;
  rw_lock autocreate_rules_lock;

  static void validate_autocreate_rules(
      const std::vector<std::pair<std::string, SeriesMetadata>> autocreate_rules);
  SeriesMetadata get_autocreate_metadata_for_key(const std::string& key_name);

  std::unordered_map<std::string, std::vector<std::string>> resolve_patterns(
      const std::vector<std::string>& key_names, bool local_only,
      BaseFunctionProfiler* profiler);

  static Error emulate_rename_series(Store* from_store,
      const std::string& from_key_name, Store* to_store,
      const std::string& to_key_name, bool merge,
      BaseFunctionProfiler* profiler);

  struct Stats {
    std::atomic<uint64_t> start_time;
    std::atomic<uint64_t> duration;

    Stats();
    Stats& operator=(const Stats& other);

    std::unordered_map<std::string, int64_t> to_map() const;
  };

  void combine_simple_results(
      std::unordered_map<std::string, Error>& into,
      std::unordered_map<std::string, Error>&& from);
  void combine_delete_results(
      std::unordered_map<std::string, DeleteResult>& into,
      std::unordered_map<std::string, DeleteResult>&& from);
  void combine_read_results(
      std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>& into,
      std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>&& from);
  void combine_find_results(std::unordered_map<std::string, FindResult>& into,
      std::unordered_map<std::string, FindResult>&& from);
};
