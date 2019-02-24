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

#include "Formats/Whisper.hh"
#include "Utils/Errors.hh"

using namespace std;



static size_t makedirs_for_file(string filename, int mode = 0755) {
  size_t num_created = 0;
  for (size_t p = filename.find('/'); p != string::npos; p = filename.find('/', p + 1)) {
    if (p == 0) {
      continue; // don't try to create the root directory
    }

    filename[p] = 0;
    if (mkdir(filename.c_str(), 0755) == -1) {
      if (errno != EEXIST) {
        throw runtime_error("can\'t create directory " + filename);
      }
    } else {
      num_created++;
    }
    filename[p] = '/';
  }
  return num_created;
}

static size_t delete_empty_directories_for_file(const string& base_directory,
    string filename) {
  size_t num_deleted = 0;
  while (filename.size() > base_directory.size()) {
    size_t slash_pos = filename.rfind('/');
    if (slash_pos == string::npos) {
      break;
    }
    filename.resize(slash_pos);

    if (filename.size() <= base_directory.size()) {
      break;
    }
    if (rmdir(filename.c_str())) {
      if (errno != EBUSY && errno != ENOENT && errno != ENOTEMPTY) {
        throw runtime_error("can\'t delete directory: " + filename);
      }
      break;
    }
    num_deleted++;
  }
  return num_deleted;
}



DiskStore::DiskStore(const string& directory) :
    directory(directory), stats(3) { }

std::string DiskStore::get_directory() const {
  return this->directory;
}

void DiskStore::set_directory(const std::string& new_value) {
  // TODO: this isn't thread-safe!
  this->directory = new_value;
  // close all open file descriptors
  WhisperArchive::clear_files_lru();
}

shared_ptr<Store::UpdateMetadataTask> DiskStore::update_metadata(StoreTaskManager*,
    shared_ptr<const UpdateMetadataArguments> args, BaseFunctionProfiler* profiler) {

  // TODO: add profiling metadata and checkpoints for this function

  unordered_map<string, Error> ret;
  for (auto& it : args->metadata) {
    auto& key_name = it.first;
    auto& metadata = it.second;

    if (!this->key_name_is_valid(it.first)) {
      ret.emplace(it.first, make_ignored("key contains invalid characters"));
      continue;
    }

    try {
      string filename = this->filename_for_key(key_name);

      // create directories if we need to
      if (args->create_new) {
        this->stats[0].directory_creates += makedirs_for_file(filename);
      }

      // create or update the series
      if (isfile(filename)) {
        if (args->behavior == UpdateMetadataBehavior::Ignore) {
          ret.emplace(key_name, make_ignored());

        } else if (args->behavior == UpdateMetadataBehavior::Update) {
          WhisperArchive(filename).update_metadata(metadata.archive_args,
              metadata.x_files_factor, metadata.agg_method);
          ret.emplace(key_name, make_success());
          this->stats[0].series_update_metadatas++;

        } else if (args->behavior == UpdateMetadataBehavior::Recreate) {
          WhisperArchive(filename, metadata.archive_args, metadata.x_files_factor,
              metadata.agg_method);
          ret.emplace(key_name, make_success());
          this->stats[0].series_truncates++;
        }

      } else {
        if (args->create_new) {
          WhisperArchive(filename, metadata.archive_args, metadata.x_files_factor,
              metadata.agg_method);
          ret.emplace(key_name, make_success());
          this->stats[0].series_creates++;

        } else {
          ret.emplace(key_name, make_ignored());
        }
      }

    } catch (const exception& e) {
      ret.emplace(key_name, make_error(e.what()));
    }
  }

  return shared_ptr<UpdateMetadataTask>(new UpdateMetadataTask(move(ret)));
}

shared_ptr<Store::DeleteSeriesTask> DiskStore::delete_series(StoreTaskManager*,
    shared_ptr<const DeleteSeriesArguments> args,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, DeleteResult> ret;

  // create results for all input patterns
  for (const auto& pattern : args->patterns) {
    auto& res = ret[pattern];
    res.disk_series_deleted = 0;
    res.buffer_series_deleted = 0;
    res.error = make_success();
  }

  // if a pattern ends in **, delete the entire tree. ** doesn't work in
  // resolve_patterns and isn't allowed anywhere else in patterns, so
  // special-case it here
  vector<string> determinate_patterns;
  vector<string> directory_patterns_to_delete;
  for (const auto& pattern : args->patterns) {
    if (ends_with(pattern, ".**")) {
      directory_patterns_to_delete.emplace_back(pattern.substr(0, pattern.size() - 3));
    } else {
      determinate_patterns.emplace_back(pattern);
    }
  }
  profiler->checkpoint("separate_determinate_patterns");

  StoreTaskManager m;
  if (!determinate_patterns.empty()) {
    auto resolve_patterns_task = this->resolve_patterns(&m,
        determinate_patterns, args->local_only, profiler);
    m.run(resolve_patterns_task);
    profiler->checkpoint("resolve_file_patterns");

    for (const auto& key_it : resolve_patterns_task->value()) {
      // if the token is a pattern, don't delete it - directory trees must be
      // deleted with the .** form of this command instead
      if (this->token_is_pattern(key_it.first)) {
        continue;
      }

      try {
        // delete the file. note: we don't need to explicitly close the fd in
        // WhisperArchive's file cache; there shouldn't be an open file for this
        // series because it was closed in the WhisperArchive destructor
        string filename = this->filename_for_key(key_it.first);
        unlink(filename);
        this->stats[0].series_deletes++;

        // then delete any empty directories in which the file resided. stop
        // when we reach the data directory or any directory isn't empty
        delete_empty_directories_for_file(this->directory, filename);

        for (const auto& pattern : key_it.second) {
          ret.at(pattern).disk_series_deleted++;
        }

      } catch (const exception& e) {
        for (const auto& pattern : key_it.second) {
          ret.at(pattern).error = make_error(string_printf(
              "failed to delete series %s (%s)", key_it.first.c_str(), e.what()));
        }
      }
    }
    profiler->checkpoint("delete_files");
  }

  if (!directory_patterns_to_delete.empty()) {
    auto resolve_patterns_task = this->resolve_patterns(&m,
        directory_patterns_to_delete, args->local_only, profiler);
    m.run(resolve_patterns_task);
    profiler->checkpoint("resolve_directory_patterns");

    for (const auto& key_it : resolve_patterns_task->value()) {
      string key = key_it.first; // copy so we can modify it
      const auto& patterns = key_it.second;

      // trim off the .* from the directory name. this is safe because files
      // have the .wsp suffix so we won't accidentally delete a file that has
      // the same name as a directory
      if (ends_with(key, ".*")) {
        key.resize(key.size() - 2);
      }

      string directory_path = this->filename_for_key(key, false);

      size_t disk_series_deleted = 0;
      vector<string> paths_to_count;
      paths_to_count.emplace_back(directory_path);
      while (!paths_to_count.empty()) {
        string path = paths_to_count.back();
        paths_to_count.pop_back();

        // note: list_directory can fail if the directory doesn't exist; in this
        // case we just treat it as empty
        unordered_set<string> contents;
        try {
          contents = list_directory(path);
        } catch (const exception& e) { }

        for (const string& item : contents) {
          string item_path = path + "/" + item;
          if (isdir(item_path)) {
            paths_to_count.emplace_back(item_path);
          } else {
            disk_series_deleted += 1;
          }
        }
      }

      Error error;
      try {
        unlink(directory_path, true);
      } catch (const exception& e) {
        error = make_error(string_printf(
            "failed to delete directory %s (%s)", directory_path.c_str(), e.what()));
      }

      for (const auto& pattern : patterns) {
        DeleteResult& res = ret.at(pattern + ".**");
        res.disk_series_deleted = disk_series_deleted;
        res.error = error;
      }
    }
    profiler->checkpoint("delete_directories");
  }

  return shared_ptr<DeleteSeriesTask>(new DeleteSeriesTask(move(ret)));
}

shared_ptr<Store::RenameSeriesTask> DiskStore::rename_series(StoreTaskManager*,
    shared_ptr<const RenameSeriesArguments> args, BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;

  // if merging, be lazy and do the read+create+write procedure instead. note
  // that we can use a trivial task manager here because there are no network
  // calls involved. for the same reason, it doesn't help to parallelize these
  // rename tasks (and doing so might be bad because it would increase memory
  // requirements)
  if (args->merge) {
    for (const auto& it : args->renames) {
      StoreTaskManager m;
      auto task = this->emulate_rename_series(&m, this, it.first, this,
          it.second, args->merge, profiler);
      m.run(task);
      ret.emplace(it.first, task->value());
    }
    return shared_ptr<RenameSeriesTask>(new RenameSeriesTask(move(ret)));
  }

  for (const auto& rename_it : args->renames) {
    if (rename_it.first == rename_it.second) {
      ret.emplace(rename_it.first, make_success());
      continue;
    }

    try {
      string from_filename = this->filename_for_key(rename_it.first);
      string to_filename = this->filename_for_key(rename_it.second);

      // if to_filename already exists, fail
      if (isfile(to_filename)) {
        ret.emplace(rename_it.first, make_ignored());
        continue;
      }

      // create directories if we need to
      this->stats[0].directory_creates += makedirs_for_file(to_filename);

      // note: we don't need to explicitly close the fd in WhisperArchive's file
      // cache; there shouldn't be an open file for this series because it was
      // closed in the WhisperArchive destructor
      rename(from_filename, to_filename);
      this->stats[0].series_renames++;

      // then delete any empty directories in which the original file resided
      delete_empty_directories_for_file(this->directory, from_filename);

      ret.emplace(rename_it.first, make_success());

    } catch (const exception& e) {
      ret.emplace(rename_it.first, make_error(e.what()));
    }
  }
  profiler->checkpoint("rename_files");

  return shared_ptr<RenameSeriesTask>(new RenameSeriesTask(move(ret)));
}

shared_ptr<Store::ReadTask> DiskStore::read(StoreTaskManager*,
    shared_ptr<const ReadArguments> args, BaseFunctionProfiler* profiler) {

  StoreTaskManager m;
  auto resolve_patterns_task = this->resolve_patterns(&m, args->key_names,
      args->local_only, profiler);
  m.run(resolve_patterns_task);
  auto key_to_patterns = resolve_patterns_task->value();
  profiler->checkpoint("resolve_patterns");

  unordered_map<string, ReadResult*> key_to_read_result;
  unordered_map<string, unordered_map<string, ReadResult>> ret;
  for (const auto& it : key_to_patterns) {
    const string& key_name = it.first;
    const vector<string>& patterns = it.second;

    for (const string& pattern : patterns) {
      unordered_map<string, ReadResult>& read_results = ret[pattern];
      ReadResult& r = read_results[key_name];

      // if we've already read this key during this query, don't read it again;
      // just copy the first ReadResult to it
      try {
        r = *key_to_read_result.at(key_name);
        continue;
      } catch (const out_of_range&) {
        key_to_read_result.emplace(key_name, &r);
      }

      try {
        WhisperArchive d(this->filename_for_key(key_name));
        if (args->start_time && args->end_time) {
          auto res = d.read(args->start_time, args->end_time);
          r.data = move(res.data);
          r.start_time = res.start_time;
          r.end_time = res.end_time;
          r.step = res.step;
        }

      } catch (const cannot_open_file& e) {
        if (e.error == ENOENT) {
          r.start_time = args->start_time;
          r.end_time = args->end_time;
          r.step = 0;
        } else {
          r.error = make_error(e.what());
        }

      } catch (const exception& e) {
        r.error = make_error(e.what());
      }
    }
  }
  profiler->checkpoint("read_data");

  this->stats[0].report_read_request(ret);
  return shared_ptr<ReadTask>(new ReadTask(move(ret)));
}

shared_ptr<Store::ReadAllTask> DiskStore::read_all(StoreTaskManager*,
    shared_ptr<const ReadAllArguments> args, BaseFunctionProfiler* profiler) {
  ReadAllResult ret;
  try {
    WhisperArchive d(this->filename_for_key(args->key_name));

    ret.data = d.read_all();

    auto metadata = d.get_metadata();
    ret.metadata.x_files_factor = metadata->x_files_factor;
    ret.metadata.agg_method = metadata->aggregation_method;
    ret.metadata.archive_args.resize(metadata->num_archives);
    for (size_t x = 0; x < metadata->num_archives; x++) {
      ret.metadata.archive_args[x].precision = metadata->archives[x].seconds_per_point;
      ret.metadata.archive_args[x].points = metadata->archives[x].points;
    }

  } catch (const cannot_open_file& e) {
    // if the file doesn't exist, it's not an error, but we return no data and
    // no metadata

  } catch (const exception& e) {
    ret.error = make_error(e.what());
  }
  return shared_ptr<ReadAllTask>(new ReadAllTask(move(ret)));
}

shared_ptr<Store::WriteTask> DiskStore::write(StoreTaskManager*,
    shared_ptr<const WriteArguments> args, BaseFunctionProfiler* profiler) {

  unordered_map<string, Error> ret;
  for (const auto& it : args->data) {
    if (!this->key_name_is_valid(it.first)) {
      ret.emplace(it.first, make_ignored("key contains invalid characters"));
      continue;
    }

    try {
      string filename = this->filename_for_key(it.first);
      try {
        WhisperArchive d(filename);
        d.write(it.second);
        ret.emplace(it.first, make_success());

      } catch (const cannot_open_file& e) {
        // the file doesn't exist - check if it can be autocreated
        auto m = this->get_autocreate_metadata_for_key(it.first);
        if (m.archive_args.empty()) {
          ret.emplace(it.first, make_error("series does not exist"));

        } else {
          shared_ptr<UpdateMetadataArguments> sub_args(new UpdateMetadataArguments());
          sub_args->metadata.emplace(it.first, m);
          sub_args->behavior = UpdateMetadataBehavior::Ignore;
          sub_args->create_new = true;
          sub_args->skip_buffering = args->skip_buffering;
          sub_args->local_only = args->local_only;

          StoreTaskManager m;
          auto task = this->update_metadata(&m, sub_args, profiler);
          m.run(task);
          auto& update_metadata_ret = task->value();
          auto series_ret = update_metadata_ret.at(it.first);

          if (series_ret.description.empty() || series_ret.ignored) {
            WhisperArchive d(filename);
            d.write(it.second);
            ret.emplace(it.first, make_success());
            this->stats[0].series_autocreates++;

          } else {
            ret.emplace(it.first, series_ret);
          }
        }
      }

    } catch (const exception& e) {
      ret.emplace(it.first, make_error(e.what()));
    }
  }

  this->stats[0].report_write_request(ret, args->data);
  return shared_ptr<WriteTask>(new WriteTask(move(ret)));
}

void DiskStore::find_recursive(vector<string>& ret,
    const string& current_path_prefix, const string& current_key_prefix,
    size_t part_index, const vector<string>& pattern_parts,
    BaseFunctionProfiler* profiler) {

  if (part_index >= pattern_parts.size()) {
    ret.push_back(current_key_prefix + "*");
    return;
  }

  const string& this_part = pattern_parts[part_index];
  if (this_part.empty()) {
    return; // nothing matches an empty pattern
  }

  bool is_find_all_recursive = (this_part == "**");
  if (this_part == "**") {
    if (part_index != pattern_parts.size() - 1) {
      throw invalid_argument("indeterminate pattern (**) may only appear at the end");
    }
  }

  for (auto filename : list_directory(current_path_prefix)) {
    string full_path = current_path_prefix + filename;
    if (isdir(full_path)) {
      if (!name_matches_pattern(filename, this_part)) {
        continue;
      }

      // if we're processing a ** token at the end of the pattern, use the same
      // part_index so deeper levels will also see the **
      this->find_recursive(ret, full_path + "/",
          current_key_prefix + filename + ".",
          part_index + !is_find_all_recursive, pattern_parts, profiler);

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
  profiler->checkpoint("list_directory:" + current_path_prefix);
}

shared_ptr<Store::FindTask> DiskStore::find(StoreTaskManager*,
    shared_ptr<const FindArguments> args, BaseFunctionProfiler* profiler) {
  unordered_map<string, FindResult> ret;
  for (const auto& pattern : args->patterns) {
    FindResult& r = ret[pattern];
    try {
      vector<string> pattern_parts = split(pattern, '.');
      this->find_recursive(r.results, this->directory + "/", "", 0,
          pattern_parts, profiler);
    } catch (const exception& e) {
      r.error = make_error(e.what());
    }
  }

  this->stats[0].report_find_request(ret);
  return shared_ptr<FindTask>(new FindTask(move(ret)));
}

unordered_map<string, int64_t> DiskStore::get_stats(bool rotate) {
  const Stats& current_stats = this->stats[0];

  if (rotate) {
    this->stats.rotate();

    uint64_t n = now();
    this->stats[0].start_time = n;
    this->stats[1].duration = n - this->stats[1].start_time;
  }

  auto ret = current_stats.to_map();
  ret.emplace("open_file_cache_size", WhisperArchive::get_files_lru_size());
  return ret;
}

string DiskStore::str() const {
  return "DiskStore(" + this->directory + ")";
}

string DiskStore::filename_for_key(const string& key_name, bool is_file) {
  // input: a.b.c.d
  // output: /root/directory/a/b/c/d.wsp
  // normally we'd just use string_printf but we need to modify key_name too

  string fname;
  fname.reserve(key_name.size() + this->directory.size() + (is_file ? 5 : 1));

  fname += this->directory;
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

DiskStore::Stats::Stats() : Store::Stats::Stats(),
    directory_creates(0),
    directory_deletes(0),
    series_creates(0),
    series_truncates(0),
    series_update_metadatas(0),
    series_autocreates(0),
    series_deletes(0),
    series_renames(0),
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
  this->Store::Stats::operator=(other);
  this->directory_creates = other.directory_creates.load();
  this->directory_deletes = other.directory_deletes.load();
  this->series_creates = other.series_creates.load();
  this->series_truncates = other.series_truncates.load();
  this->series_update_metadatas = other.series_update_metadatas.load();
  this->series_autocreates = other.series_autocreates.load();
  this->series_deletes = other.series_deletes.load();
  this->series_renames = other.series_renames.load();
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
  unordered_map<string, int64_t> ret = this->Store::Stats::to_map();
  ret.emplace("directory_creates", this->directory_creates.load());
  ret.emplace("directory_deletes", this->directory_deletes.load());
  ret.emplace("series_creates", this->series_creates.load());
  ret.emplace("series_truncates", this->series_truncates.load());
  ret.emplace("series_update_metadatas", this->series_update_metadatas.load());
  ret.emplace("series_autocreates", this->series_autocreates.load());
  ret.emplace("series_deletes", this->series_deletes.load());
  ret.emplace("series_renames", this->series_renames.load());
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
      if (!it2.second.error.description.empty()) {
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
    const unordered_map<string, Error>& ret,
    const unordered_map<string, Series>& data) {
  size_t datapoints = 0;
  size_t errors = 0;
  for (const auto& it : ret) {
    if (it.second.ignored) {
      continue;
    } else if (!it.second.description.empty()) {
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
    if (!it.second.error.description.empty()) {
      errors++;
    }
    results += it.second.results.size();
  }

  this->find_requests++;
  this->find_patterns += ret.size();
  this->find_results += results;
  this->find_errors += errors;
}
