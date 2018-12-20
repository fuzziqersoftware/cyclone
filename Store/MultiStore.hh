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

  std::unordered_map<std::string, std::shared_ptr<Store>> get_substores() const;

  virtual void set_autocreate_rules(
      const std::vector<std::pair<std::string, SeriesMetadata>>& autocreate_rules);

  virtual std::unordered_map<std::string, Error> update_metadata(
      const SeriesMetadataMap& metadata, bool create_new,
      UpdateMetadataBehavior update_behavior, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler);
  virtual std::unordered_map<std::string, DeleteResult> delete_series(
      const std::vector<std::string>& patterns, bool deferred, bool local_only,
      BaseFunctionProfiler* profiler);
  virtual std::unordered_map<std::string, Error> rename_series(
      const std::unordered_map<std::string, std::string>& renames, bool merge,
      bool local_only, BaseFunctionProfiler* profiler);

  virtual std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>> read(
      const std::vector<std::string>& key_names, int64_t start_time,
      int64_t end_time, bool local_only, BaseFunctionProfiler* profiler);
  virtual ReadAllResult read_all(const std::string& key_name, bool local_only,
      BaseFunctionProfiler* profiler);
  virtual std::unordered_map<std::string, Error> write(
      const std::unordered_map<std::string, Series>& data, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler);

  virtual std::unordered_map<std::string, FindResult> find(
      const std::vector<std::string>& patterns, bool local_only,
      BaseFunctionProfiler* profiler);

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);

  virtual std::string str() const;

protected:
  std::unordered_map<std::string, std::shared_ptr<Store>> stores;
};
