cpp_include '<unordered_map>'

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
  1: string error;
  2: Series data;
  4: i64 start_time;
  5: i64 end_time;
  6: i64 step;
}

struct FindResult {
  1: string error;
  2: list<string> results;
}

typedef list<Datapoint> Series;
typedef map cpp_type 'std::unordered_map<std::string, Series>'
    <string, Series> SeriesMap;
typedef map cpp_type 'std::unordered_map<std::string, class SeriesMetadata>'
    <string, SeriesMetadata> SeriesMetadataMap;

typedef map cpp_type 'std::unordered_map<std::string, std::unordered_map<std::string, class ReadResult>>'
    <string, map cpp_type 'std::unordered_map<std::string, class ReadResult>' <string, ReadResult>> ReadResultMap;

typedef map cpp_type 'std::unordered_map<std::string, std::string>'
    <string, string> WriteResultMap;

typedef map cpp_type 'std::unordered_map<std::string, int64_t>'
    <string, i64> DeleteResultMap;

typedef map cpp_type 'std::unordered_map<std::string, class FindResult>'
    <string, FindResult> FindResultMap;

typedef map cpp_type 'std::unordered_map<std::string, int64_t>'
    <string, i64> StatsResultMap;

service Cyclone {



  // write commands

  // updates metadata and/or creates series. returns the number of series
  // modified (including creates and updates).
  WriteResultMap update_metadata(1: SeriesMetadataMap metadata,
      2: bool create_new = true, 3: bool skip_existing_series = false,
      4: bool truncate_existing_series = false, 5: bool local_only = false);

  // deletes series. returns the number of series deleted.
  DeleteResultMap delete_series(1: list<string> patterns,
      2: bool local_only = false);

  // writes or deletes datapoints in a series. to delete datapoints, pass NaN as
  // the value.
  WriteResultMap write(1: SeriesMap data, 2: bool local_only = false);

  // load a serialized series. if the series exists and combine_from_existing is
  // true, read all data in the most recent archive of the existing series, and
  // write it to the restored series before restoring it.
  string restore_series(1: string key_name, 2: binary data,
      3: bool combine_from_existing, 4: bool local_only = false);



  // read commands

  // reads datapoints from multiple series.
  ReadResultMap read(1: list<string> targets, 2:i64 start_time,
      3: i64 end_time, 4: bool local_only = false);

  // return a serialized version of the given series' schema and data, which can
  // be loaded again with restore_series.
  binary serialize_series(1: string key_name, 2: bool local_only = false);

  // searches for directory and key names matching the given patterns. if a
  // result ends with '.*', it's a directory; otherwise it's a key.
  FindResultMap find(1: list<string> patterns, 2: bool local_only = false);

  // returns the current server stats
  StatsResultMap stats();



  // debugging / internal commands

  // if the server has a cache store, deletes the given path from the cache. if
  // the path is blank or "*", deletes everything in the cache.
  i64 delete_from_cache(1: string path, 2: bool local_only = false);

  // if the server has a write buffer store, deletes everything matching the
  // given pattern from the write buffer. if the pattern is blank, deletes
  // everything in the write buffer.
  i64 delete_pending_writes(1: string pattern, 2: bool local_only = false);
}
