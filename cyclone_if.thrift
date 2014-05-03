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
  3: SeriesMetadata metadata;
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

typedef map cpp_type 'std::unordered_map<std::string, class ReadResult>'
    <string, ReadResult> ReadResultMap;

typedef map cpp_type 'std::unordered_map<std::string, std::string>'
    <string, string> WriteResultMap;

typedef map cpp_type 'std::unordered_map<std::string, class FindResult>'
    <string, FindResult> FindResultMap;

service Cyclone {
  // updates metadata and/or creates series. returns the number of series
  // modified (including creates and updates).
  WriteResultMap update_metadata(1: SeriesMetadataMap metadata,
      2: bool create_new = true, 3: bool skip_existing_series = false,
      4: bool truncate_existing_series = false);

  // deletes series. returns the number of series deleted.
  WriteResultMap delete_series(1: list<string> key_names);

  // reads metadata and/or datapoints from multiple series.
  ReadResultMap read_metadata(1: list<string> targets);
  ReadResultMap read(1: list<string> targets, 2:i64 start_time,
      3: i64 end_time);

  // writes or deletes datapoints in a series. to delete datapoints, pass NaN as
  // the value.
  WriteResultMap write(1: SeriesMap data);

  // TODO
  // SeriesMap execute_query(1: list<string> targets, 2:i64 start_time,
  //     3: i64 end_time);

  // searches for directory and key names matching the given patterns. if a
  // result ends with '.*', it's a directory; otherwise it's a key.
  FindResultMap find(1: list<string> patterns);
}
