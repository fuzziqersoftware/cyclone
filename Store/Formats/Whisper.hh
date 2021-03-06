#pragma once

#include <stdint.h>

#include <memory>
#include <mutex>
#include <phosg/Filesystem.hh>
#include <phosg/LRUSet.hh>
#include <string>
#include <unordered_map>
#include <vector>

#include <phosg/Concurrency.hh>
#include <phosg/FileCache.hh>

#include "../../gen-cpp/Cyclone.h"


enum AggregationMethod {
  Average = 1,
  Sum = 2,
  Last = 3,
  Min = 4,
  Max = 5,
};


class WhisperArchive {
public:
  struct ArchiveMetadata {
    uint32_t offset;
    uint32_t seconds_per_point;
    uint32_t points;
  };
  struct Metadata {
    AggregationMethod aggregation_method;
    uint32_t max_retention;
    float x_files_factor;
    uint32_t num_archives;
    ArchiveMetadata archives[0];
  };
  struct ReadResult {
    Series data;
    uint64_t start_time;
    uint64_t end_time;
    uint64_t step;

    ReadResult();
    ~ReadResult() = default;

    bool operator==(const ReadResult& other) const;
    bool operator!=(const ReadResult& other) const;

    std::string str() const;
  };

  WhisperArchive() = delete;
  WhisperArchive(const WhisperArchive& rhs) = delete;
  WhisperArchive(WhisperArchive&& rhs);
  const WhisperArchive& operator=(const WhisperArchive& rhs) = delete;

  // open constructor (doesn't create a new file)
  WhisperArchive(const std::string& filename);

  // create constructor (always creates a new file)
  WhisperArchive(const std::string& filename,
      const std::vector<ArchiveArg>& archive_args, float x_files_factor,
      uint32_t agg_method);
  WhisperArchive(const std::string& filename, const std::string& archive_args,
      float x_files_factor, uint32_t agg_method);

  virtual ~WhisperArchive();

  const std::string& get_filename() const;
  std::shared_ptr<const Metadata> get_metadata() const;

  void print(FILE* stream, bool print_data = false);
  ReadResult read(uint64_t start_time, uint64_t end_time);
  Series read_all();
  void write(const Series& data);

  void truncate();
  void update_metadata(const std::vector<ArchiveArg>& archive_args,
      float x_files_factor, uint32_t agg_method, bool truncate = false);

  size_t get_file_size() const;

  static std::vector<ArchiveArg> parse_archive_args(const std::string& s);
  static void validate_archive_args(const std::vector<ArchiveArg>& args);

  static size_t get_files_lru_size();
  static void set_files_lru_max_size(size_t max);
  static void clear_files_lru();

private:

  struct FileHeader {
    uint32_t aggregation_type;
    uint32_t max_retention;
    uint32_t x_files_factor; // actually a float, but byteswapped
    uint32_t archive_count;
  } __attribute__((packed));

  struct FileArchiveHeader {
    uint32_t offset;
    uint32_t seconds_per_point;
    uint32_t points;
  } __attribute__((packed));

  struct FilePoint {
    uint32_t time;
    uint64_t value; // actually a double, but byteswapped
  } __attribute__((packed));

  void create_file_locked(int fd);
  void write_header_locked(int fd);

  size_t get_file_size_locked() const;
  uint32_t get_base_interval_locked(int fd, uint32_t archive_index);
  void write_sorted_locked(const Series& data, int64_t t);
  void write_archive_locked(int fd, uint32_t archive_index, const Series& data,
      uint32_t start_index, uint32_t end_index);
  bool propagate_write_locked(int fd, uint64_t interval, uint32_t archive_index,
      uint32_t target_archive_index);
  double aggregate_locked(uint64_t interval_start, uint64_t interval_step,
      const FilePoint* pts, uint32_t num_pts) const;

  const std::string filename;
  mutable std::vector<int64_t> base_intervals; // -1 = not present
  std::shared_ptr<Metadata> metadata;

  // this lock protects both the metadata in memory and the data on disk
  mutable rw_lock lock;

  static FileCache file_cache;
};
