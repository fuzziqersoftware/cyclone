cpp_include '<unordered_map>'

struct Error {
  1: string description;
  2: bool recoverable;
  3: bool ignored;
}

struct Datapoint {
  1: i64 timestamp;
  2: double value;
}

struct ArchiveArg {
  1: i32 precision;
  2: i32 points;
}

struct SeriesMetadata {
  1: list<ArchiveArg> archive_args;
  2: double x_files_factor;
  3: i32 agg_method;
}

struct ReadResult {
  1: Error error;
  2: Series data;
  4: i64 start_time;
  5: i64 end_time;
  6: i64 step;
}

struct ReadAllResult {
  1: Error error;
  2: SeriesMetadata metadata;
  3: Series data;
}

struct FindResult {
  1: Error error;
  2: list<string> results;
}

struct DeleteResult {
  1: Error error;
  2: i64 disk_series_deleted;
  3: i64 buffer_series_deleted;
}

typedef list<Datapoint> Series;
typedef map cpp_type 'std::unordered_map<std::string, Series>'
    <string, Series> SeriesMap;
typedef map cpp_type 'std::unordered_map<std::string, class SeriesMetadata>'
    <string, SeriesMetadata> SeriesMetadataMap;

typedef map cpp_type 'std::unordered_map<std::string, std::string>'
    <string, string> RenameSeriesMap;

typedef map cpp_type 'std::unordered_map<std::string, std::unordered_map<std::string, class ReadResult>>'
    <string, map cpp_type 'std::unordered_map<std::string, class ReadResult>' <string, ReadResult>> ReadResultMap;

typedef map cpp_type 'std::unordered_map<std::string, class Error>'
    <string, Error> WriteResultMap;

typedef map cpp_type 'std::unordered_map<std::string, class DeleteResult>'
    <string, DeleteResult> DeleteResultMap;

typedef map cpp_type 'std::unordered_map<std::string, class FindResult>'
    <string, FindResult> FindResultMap;

typedef map cpp_type 'std::unordered_map<std::string, int64_t>'
    <string, i64> StatsResultMap;

service Cyclone {



  // write commands

  // updates metadata and/or creates series. returns error strings for each
  // series; an empty error string signifies success.
  WriteResultMap update_metadata(1: SeriesMetadataMap metadata,
      2: bool create_new = true, 3: bool skip_existing_series = false,
      4: bool truncate_existing_series = false, 5: bool skip_buffering = false,
      6: bool local_only = false);

  // deletes series. returns the number of series deleted. for each pattern.
  DeleteResultMap delete_series(1: list<string> patterns,
      2: bool local_only = false);

  // renames series. returns an error string for each series.
  WriteResultMap rename_series(1: RenameSeriesMap renames,
      3: bool merge, 2: bool local_only = false);

  // writes or deletes datapoints in a series. to delete datapoints, pass NaN as
  // the value. returns an error string for each series.
  WriteResultMap write(1: SeriesMap data, 2: bool skip_buffering = false,
      3: bool local_only = false);



  // read commands

  // reads datapoints from multiple series.
  ReadResultMap read(1: list<string> targets, 2:i64 start_time,
      3: i64 end_time, 4: bool local_only = false);

  // reads all datapoints and metadata from a series.
  ReadAllResult read_all(1: string series, 2: bool local_only = false);

  // searches for directory and key names matching the given patterns. if a
  // result ends with '.*', it's a directory; otherwise it's a key.
  FindResultMap find(1: list<string> patterns, 2: bool local_only = false);

  // returns the current server stats
  StatsResultMap stats();



  // administration commands

  // returns the status of the current verify operation.
  StatsResultMap get_verify_status();

  // starts or cancels a verify operation. returns true if the operation was
  // performed (a verify was started or cancelled).
  bool start_verify(1: bool repair);
  bool cancel_verify();

  // gets or sets the read-from-all state. this state should be enabled on all
  // nodes in a cluster before a verify+repair starts and disabled after it
  // ends.
  bool get_read_from_all();
  bool set_read_from_all(1: bool read_from_all);



  // debugging / internal commands

  // if the server has a cache store, deletes the given path from the cache. if
  // the path is blank or "*", deletes everything in the cache.
  i64 delete_from_cache(1: string path, 2: bool local_only = false);

  // if the server has a write buffer store, deletes everything matching the
  // given pattern from the write buffer. if the pattern is blank, deletes
  // everything in the write buffer.
  i64 delete_pending_writes(1: string pattern, 2: bool local_only = false);
}
