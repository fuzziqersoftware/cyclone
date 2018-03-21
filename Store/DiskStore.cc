#include "DiskStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <phosg/Filesystem.hh>
#include <phosg/JSON.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>
#include <stdexcept>
#include <string>
#include <vector>

#include "Whisper.hh"

using namespace std;


SeriesMetadata DiskStore::convert_metadata_to_thrift(
    const WhisperArchive::Metadata& m) {
  SeriesMetadata sm;
  for (size_t x = 0; x < m.num_archives; x++) {
    sm.archive_args.emplace_back();
    auto& arg = sm.archive_args.back();
    arg.precision = m.archives[x].seconds_per_point;
    arg.points = m.archives[x].points;
  }
  sm.x_files_factor = m.x_files_factor;
  sm.agg_method = m.aggregation_method;
  return sm;
}


DiskStore::DiskStore(const string& root_directory) :
    root_directory(root_directory), stats(3) { }

void DiskStore::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>> autocreate_rules) {
  this->validate_autocreate_rules(autocreate_rules);
  auto new_rules = autocreate_rules;
  {
    rw_guard g(this->autocreate_rules_lock, true);
    this->autocreate_rules.swap(new_rules);
  }
}

unordered_map<string, string> DiskStore::update_metadata(
    const SeriesMetadataMap& m, bool create_new,
    UpdateMetadataBehavior update_behavior) {

  unordered_map<string, string> ret;
  for (auto& it : m) {
    auto& key_name = it.first;
    auto& metadata = it.second;

    try {
      string filename = this->filename_for_key(key_name);

      // create directories if we need to
      if (create_new) {
        string dirname = filename;
        for (size_t p = dirname.find('/'); p != string::npos; p = dirname.find('/', p + 1)) {
          if (p == 0) {
            continue; // don't try to create the root directory
          }

          dirname[p] = 0;
          if (mkdir(dirname.c_str(), 0755) == -1) {
            if (errno != EEXIST) {
              throw runtime_error("can\'t create directory " + dirname);
            }
          } else {
            this->stats[0].directory_creates++;
          }
          dirname[p] = '/';
        }
      }

      // create or update the series
      if (isfile(filename)) {
        if (update_behavior == UpdateMetadataBehavior::Ignore) {
          ret.emplace(key_name, "ignored");

        } else if (update_behavior == UpdateMetadataBehavior::Update) {
          WhisperArchive(filename).update_metadata(metadata.archive_args,
              metadata.x_files_factor, metadata.agg_method);
          ret.emplace(key_name, "");
          this->stats[0].series_update_metadatas++;

        } else if (update_behavior == UpdateMetadataBehavior::Recreate) {
          WhisperArchive(filename, metadata.archive_args, metadata.x_files_factor,
              metadata.agg_method);
          ret.emplace(key_name, "");
          this->stats[0].series_truncates++;
        }

      } else {
        if (create_new) {
          WhisperArchive(filename, metadata.archive_args, metadata.x_files_factor,
              metadata.agg_method);
          ret.emplace(key_name, "");
          this->stats[0].series_creates++;

        } else {
          ret.emplace(key_name, "ignored");
        }
      }

    } catch (const exception& e) {
      ret.emplace(key_name, e.what());
    }
  }

  return ret;
}

unordered_map<string, string> DiskStore::delete_series(const vector<string>& key_names) {
  unordered_map<string, string> ret;
  for (const auto& key_name : key_names) {
    try {
      // delete the file
      // note: we don't need to explicitly close the fd in WhisperArchive's file
      // cache; there shouldn't be an open file for this series because it was
      // closed in the WhisperArchive destructor
      string filename = this->filename_for_key(key_name);
      unlink(filename);
      this->stats[0].series_deletes++;

      // then delete any empty directories in which the file resided. stop when
      // we reach the data directory or any directory isn't empty
      while (filename.size() > this->root_directory.size()) {
        size_t slash_pos = filename.rfind('/');
        if (slash_pos == string::npos) {
          break;
        }
        filename.resize(slash_pos);

        if (filename.size() <= this->root_directory.size()) {
          break;
        }
        if (rmdir(filename.c_str())) {
          if (errno != EBUSY && errno != ENOENT && errno != ENOTEMPTY) {
            throw runtime_error("can\'t delete directory: " + filename);
          }
          break;
        }
        this->stats[0].directory_deletes++;
      }

      ret.emplace(key_name, "");
    } catch (const exception& e) {
      ret.emplace(key_name, e.what());
    }
  }
  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> DiskStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time) {

  auto key_to_pattern = this->resolve_patterns(key_names);

  unordered_map<string, unordered_map<string, ReadResult>> ret;
  for (const auto& it : key_to_pattern) {
    const string& key_name = it.first;
    const string& pattern = it.second;

    unordered_map<string, ReadResult>& read_results = ret[pattern];
    ReadResult& r = read_results[key_name];

    try {
      WhisperArchive d(this->filename_for_key(key_name));
      if (start_time && end_time) {
        r.data = d.read(start_time, end_time);
      }
      r.metadata = this->convert_metadata_to_thrift(*d.get_metadata());

    } catch (const cannot_open_file& e) {
      if (e.error == ENOENT) {
        r.error = "series does not exist";
      } else {
        r.error = e.what();
      }

    } catch (const exception& e) {
      r.error = e.what();
    }
  }

  this->stats[0].report_read_request(ret);
  return ret;
}

unordered_map<string, string> DiskStore::write(
    const unordered_map<string, Series>& data) {
  unordered_map<string, string> ret;
  for (const auto& it : data) {
    try {
      string filename = this->filename_for_key(it.first);
      try {
        WhisperArchive d(filename);
        d.write(it.second);
        ret.emplace(it.first, "");

      } catch (const cannot_open_file& e) {
        // the file doesn't exist - check if it can be autocreated
        auto m = this->get_autocreate_metadata_for_key(it.first);
        if (m.archive_args.empty()) {
          ret.emplace(it.first, "key does not exist");

        } else {
          auto update_metadata_ret = this->update_metadata({{it.first, m}}, true, UpdateMetadataBehavior::Ignore);
          auto series_ret = update_metadata_ret.at(it.first);

          if (series_ret.empty() || (series_ret == "ignored")) {
            WhisperArchive d(filename);
            d.write(it.second);
            ret.emplace(it.first, "");
            this->stats[0].series_autocreates++;

          } else {
            ret.emplace(it.first, series_ret);
          }
        }
      }

    } catch (const exception& e) {
      ret.emplace(it.first, e.what());
    }
  }

  this->stats[0].report_write_request(ret, data);
  return ret;
}

void DiskStore::find_recursive(vector<string>& ret,
    const string& current_path_prefix, const string& current_key_prefix,
    size_t part_index, const vector<string>& pattern_parts) {

  if (part_index >= pattern_parts.size()) {
    ret.push_back(current_key_prefix + "*");
    return;
  }

  const string& this_part = pattern_parts[part_index];
  if (this_part.empty()) {
    return; // nothing matches an empty pattern
  }

  for (auto filename : list_directory(current_path_prefix)) {
    string full_path = current_path_prefix + filename;
    if (isdir(full_path)) {
      if (!name_matches_pattern(filename, this_part)) {
        continue;
      }
      this->find_recursive(ret, full_path + "/",
          current_key_prefix + filename + ".", part_index + 1, pattern_parts);

    } else {
      if (part_index != pattern_parts.size() - 1) {
        continue; // expected directory here, got file
      }

      if (!filename.compare(filename.size() - 4, 4, ".wsp")) {
        filename.resize(filename.size() - 4);
      }

      if (!this->name_matches_pattern(filename, this_part)) {
        continue;
      }

      ret.push_back(current_key_prefix + filename);
    }
  }
}

unordered_map<string, FindResult> DiskStore::find(const vector<string>& patterns) {
  unordered_map<string, FindResult> ret;
  for (const auto& pattern : patterns) {
    FindResult& r = ret[pattern];
    if (this->pattern_is_indeterminate(pattern)) {
      r.error = "pattern is indeterminate";
      continue;
    }
    try {
      vector<string> pattern_parts = split(pattern, '.');
      this->find_recursive(r.results, this->root_directory + "/", "", 0, pattern_parts);
    } catch (const exception& e) {
      r.error = e.what();
    }
  }

  this->stats[0].report_find_request(ret);
  return ret;
}

unordered_map<string, int64_t> DiskStore::get_stats(bool rotate) {
  const Stats& current_stats = this->stats[0];

  if (rotate) {
    this->stats.rotate();

    uint64_t n = now();
    this->stats[0].start_time = n;
    this->stats[1].duration = n - this->stats[1].start_time;
  }

  return current_stats.to_map();
}

string DiskStore::str() const {
  return "DiskStore(" + this->root_directory + ")";
}

string DiskStore::filename_for_key(const string& key_name, bool is_file) {
  // input: a.b.c.d
  // output: /root/directory/a/b/c/d.wsp
  // normally we'd just use string_printf but we need to modify key_name too

  string fname;
  fname.reserve(key_name.size() + this->root_directory.size() + (is_file ? 5 : 1));

  fname += this->root_directory;
  fname += '/';

  // replace periods in key_name with slashes
  for (auto ch : key_name) {
    fname += ((ch == '.') ? '/' : ch);
  }

  if (is_file) {
    fname += ".wsp";
  }
  return fname;
}

SeriesMetadata DiskStore::get_autocreate_metadata_for_key(const string& key_name) {
  {
    rw_guard g(this->autocreate_rules_lock, false);
    for (const auto& rule : this->autocreate_rules) {
      if (this->name_matches_pattern(key_name, rule.first)) {
        return rule.second;
      }
    }
  }

  return SeriesMetadata();
}

DiskStore::Stats::Stats() : start_time(now()), duration(0),
    directory_creates(0),
    directory_deletes(0),
    series_creates(0),
    series_truncates(0),
    series_update_metadatas(0),
    series_autocreates(0),
    series_deletes(0),
    read_requests(0),
    read_series(0),
    read_datapoints(0),
    read_errors(0),
    write_requests(0),
    write_series(0),
    write_datapoints(0),
    write_errors(0),
    find_requests(0),
    find_patterns(0),
    find_results(0),
    find_errors(0) { }

// TODO: figure out if we can make this less dumb
DiskStore::Stats& DiskStore::Stats::operator=(const Stats& other) {
  this->start_time = other.start_time.load();
  this->duration = other.duration.load();
  this->directory_creates = other.directory_creates.load();
  this->directory_deletes = other.directory_deletes.load();
  this->series_creates = other.series_creates.load();
  this->series_truncates = other.series_truncates.load();
  this->series_update_metadatas = other.series_update_metadatas.load();
  this->series_autocreates = other.series_autocreates.load();
  this->series_deletes = other.series_deletes.load();
  this->read_requests = other.read_requests.load();
  this->read_series = other.read_series.load();
  this->read_datapoints = other.read_datapoints.load();
  this->read_errors = other.read_errors.load();
  this->write_requests = other.write_requests.load();
  this->write_series = other.write_series.load();
  this->write_datapoints = other.write_datapoints.load();
  this->write_errors = other.write_errors.load();
  this->find_requests = other.find_requests.load();
  this->find_patterns = other.find_patterns.load();
  this->find_results = other.find_results.load();
  this->find_errors = other.find_errors.load();
  return *this;
}

unordered_map<string, int64_t> DiskStore::Stats::to_map() const {
  unordered_map<string, int64_t> ret;
  ret.emplace("start_time", this->start_time.load());
  ret.emplace("duration", this->duration.load());
  ret.emplace("directory_creates", this->directory_creates.load());
  ret.emplace("directory_deletes", this->directory_deletes.load());
  ret.emplace("series_creates", this->series_creates.load());
  ret.emplace("series_truncates", this->series_truncates.load());
  ret.emplace("series_update_metadatas", this->series_update_metadatas.load());
  ret.emplace("series_autocreates", this->series_autocreates.load());
  ret.emplace("series_deletes", this->series_deletes.load());
  ret.emplace("read_requests", this->read_requests.load());
  ret.emplace("read_series", this->read_series.load());
  ret.emplace("read_datapoints", this->read_datapoints.load());
  ret.emplace("read_errors", this->read_errors.load());
  ret.emplace("write_requests", this->write_requests.load());
  ret.emplace("write_series", this->write_series.load());
  ret.emplace("write_datapoints", this->write_datapoints.load());
  ret.emplace("write_errors", this->write_errors.load());
  ret.emplace("find_requests", this->find_requests.load());
  ret.emplace("find_patterns", this->find_patterns.load());
  ret.emplace("find_results", this->find_results.load());
  ret.emplace("find_errors", this->find_errors.load());
  return ret;
}

void DiskStore::Stats::report_directory_delete(size_t directories,
    size_t files) {
  this->directory_deletes += directories;
  this->series_deletes += files;
}

void DiskStore::Stats::report_read_request(
    const unordered_map<string, unordered_map<string, ReadResult>>& ret) {
  size_t datapoints = 0;
  size_t errors = 0;
  for (const auto& it : ret) { // (pattern, key_to_result)
    for (const auto& it2 : it.second) { // (key_name, result)
      if (!it2.second.error.empty()) {
        errors++;
      }
      datapoints += it2.second.data.size();
    }
  }

  this->read_requests++;
  this->read_series += ret.size();
  this->read_datapoints += datapoints;
  this->read_errors += errors;
}

void DiskStore::Stats::report_write_request(
    const unordered_map<string, string>& ret,
    const unordered_map<string, Series>& data) {
  size_t datapoints = 0;
  size_t errors = 0;
  for (const auto& it : ret) {
    if (!it.second.empty()) {
      errors++;
    } else {
      try {
        datapoints += data.at(it.first).size();
      } catch (const out_of_range& e) { }
    }
  }

  this->write_requests++;
  this->write_series += data.size();
  this->write_datapoints += datapoints;
  this->write_errors += errors;
}

void DiskStore::Stats::report_find_request(
    const unordered_map<string, FindResult>& ret) {
  size_t results = 0;
  size_t errors = 0;
  for (const auto& it : ret) {
    if (!it.second.error.empty()) {
      errors++;
    }
    results += it.second.results.size();
  }

  this->find_requests++;
  this->find_patterns += ret.size();
  this->find_results += results;
  this->find_errors += errors;
}
