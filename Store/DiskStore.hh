#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

#include <phosg/Concurrency.hh>

#include "FixedAtomicRotator.hh"
#include "Store.hh"
#include "Whisper.hh"


class DiskStore : public Store {
public:
  DiskStore() = delete;
  DiskStore(const DiskStore& rhs) = delete;
  explicit DiskStore(const std::string& directory);
  virtual ~DiskStore() = default;
  const DiskStore& operator=(const DiskStore& rhs) = delete;

  // note: CachedDiskStore overrides set_directory so it can clear the cache
  std::string get_directory() const;
  virtual void set_directory(const std::string& new_value);

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

  virtual std::unordered_map<std::string, FindResult> find(
      const std::vector<std::string>& patterns, bool local_only);

  virtual std::unordered_map<std::string, int64_t> get_stats(
      bool rotate = false);

  virtual std::string restore_series(const std::string& key_name,
      const std::string& data, bool combine_from_existing, bool local_only);
  virtual std::string serialize_series(const std::string& key_name,
      bool local_only);

  virtual std::string str() const;

protected:
  void find_recursive(std::vector<std::string>& ret,
      const std::string& current_path_prefix,
      const std::string& current_key_prefix, size_t part_index,
      const std::vector<std::string>& pattern_parts);

  std::string filename_for_key(const std::string& key_name, bool is_file = true);

  std::string directory;

  struct Stats : public Store::Stats {
    std::atomic<size_t> directory_creates;
    std::atomic<size_t> directory_deletes;
    std::atomic<size_t> series_creates;
    std::atomic<size_t> series_truncates;
    std::atomic<size_t> series_update_metadatas;
    std::atomic<size_t> series_autocreates;
    std::atomic<size_t> series_deletes;
    std::atomic<size_t> read_requests;
    std::atomic<size_t> read_series;
    std::atomic<size_t> read_datapoints;
    std::atomic<size_t> read_errors;
    std::atomic<size_t> write_requests;
    std::atomic<size_t> write_series;
    std::atomic<size_t> write_datapoints;
    std::atomic<size_t> write_errors;
    std::atomic<size_t> find_requests;
    std::atomic<size_t> find_patterns;
    std::atomic<size_t> find_results;
    std::atomic<size_t> find_errors;

    Stats();
    Stats& operator=(const Stats& other);

    std::unordered_map<std::string, int64_t> to_map() const;

    void report_directory_delete(size_t directories, size_t files);
    void report_read_request(
        const std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>>& ret);
    void report_write_request(
        const std::unordered_map<std::string, std::string>& ret,
        const std::unordered_map<std::string, Series>& data);
    void report_find_request(
        const std::unordered_map<std::string, FindResult>& ret);
  };

private:
  // presumably subclasses will want to extend Stats, so don't make them inherit
  // this
  FixedAtomicRotator<Stats> stats;
};
