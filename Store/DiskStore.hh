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
  explicit DiskStore(const std::string& root_directory);
  virtual ~DiskStore() = default;

  const DiskStore& operator=(const DiskStore& rhs) = delete;

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

protected:
  static SeriesMetadata convert_metadata_to_thrift(
      const WhisperArchive::Metadata& m);

  void find_recursive(std::vector<std::string>& ret,
      const std::string& current_path_prefix,
      const std::string& current_key_prefix, size_t part_index,
      const std::vector<std::string>& pattern_parts);

  std::string filename_for_key(const std::string& key_name, bool is_file = true);

  SeriesMetadata get_autocreate_metadata_for_key(const std::string& key_name);

  std::string root_directory;

  std::vector<std::pair<std::string, SeriesMetadata>> autocreate_rules;
  rw_lock autocreate_rules_lock;

  struct Stats {
    std::atomic<uint64_t> start_time;
    std::atomic<uint64_t> duration;

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
        const std::unordered_map<std::string, ReadResult>& ret);
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
