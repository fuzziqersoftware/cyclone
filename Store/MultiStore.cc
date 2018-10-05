#include "MultiStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <iostream>
#include <phosg/Strings.hh>
#include <stdexcept>
#include <string>
#include <vector>

#include "Utils/Errors.hh"

using namespace std;


MultiStore::MultiStore(const unordered_map<string, shared_ptr<Store>>& stores) :
    Store(), stores(stores) { }

unordered_map<string, shared_ptr<Store>> MultiStore::get_substores() const {
  return this->stores;
}

void MultiStore::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>>& autocreate_rules) {
  this->Store::set_autocreate_rules(autocreate_rules);
  for (const auto& it : this->stores) {
    it.second->set_autocreate_rules(autocreate_rules);
  }
}

unordered_map<string, Error> MultiStore::update_metadata(
    const SeriesMetadataMap& metadata, bool create_new,
    UpdateMetadataBehavior update_behavior, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {

  unordered_map<string, Error> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->update_metadata(metadata, create_new,
        update_behavior, skip_buffering, local_only, profiler);
    this->combine_simple_results(ret, move(results));
  }
  return ret;
}

unordered_map<string, int64_t> MultiStore::delete_series(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {

  unordered_map<string, int64_t> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->delete_series(patterns, local_only, profiler);
    for (const auto& result_it : results) {
      ret[result_it.first] += result_it.second;
    }
  }
  return ret;
}

unordered_map<string, Error> MultiStore::rename_series(
    const unordered_map<string, string>& renames, bool local_only,
    BaseFunctionProfiler* profiler) {

  unordered_map<string, Error> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->rename_series(renames, local_only, profiler);
    this->combine_simple_results(ret, move(results));
  }
  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> MultiStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time,
    bool local_only, BaseFunctionProfiler* profiler) {
  unordered_map<string, unordered_map<string, ReadResult>> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->read(key_names, start_time, end_time, local_only,
        profiler);
    this->combine_read_results(ret, move(results));
  }
  return ret;
}

ReadAllResult MultiStore::read_all(const string& key_name, bool local_only,
    BaseFunctionProfiler* profiler) {
  ReadAllResult ret;
  for (const auto& it : this->stores) {
    auto result = it.second->read_all(key_name, local_only, profiler);
    if (result.metadata.archive_args.empty()) {
      continue;
    }
    if (!ret.metadata.archive_args.empty()) {
      ret.error = make_error("multiple stores returned nonempty results");
    } else {
      ret = move(result);
    }
  }
  return ret;
}

unordered_map<string, Error> MultiStore::write(
    const unordered_map<string, Series>& data, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  unordered_map<string, Error> ret;
  for (const auto& it : this->stores) {
    auto results = it.second->write(data, skip_buffering, local_only, profiler);
    this->combine_simple_results(ret, move(results));
  }
  return ret;
}

unordered_map<string, FindResult> MultiStore::find(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {
  unordered_map<string, FindResult> ret;
  for (const auto it : this->stores) {
    auto results = it.second->find(patterns, local_only, profiler);
    this->combine_find_results(ret, move(results));
  }
  return ret;
}

unordered_map<string, int64_t> MultiStore::get_stats(bool rotate) {
  unordered_map<string, int64_t> ret;
  for (const auto& it : this->stores) {
    // replace any invalid characters in the store name
    string store_name;
    for (char ch : it.first) {
      if (!this->key_char_is_valid(ch)) {
        ch = '_';
      }
      store_name.push_back(ch);
    }

    for (const auto& stat : it.second->get_stats(rotate)) {
      ret.emplace(string_printf("%s:%s", store_name.c_str(),
          stat.first.c_str()), stat.second);
    }
  }
  return ret;
}

int64_t MultiStore::delete_from_cache(const std::string& path, bool local_only) {
  int64_t ret = 0;
  for (const auto& it : this->stores) {
    ret += it.second->delete_from_cache(path, local_only);
  }
  return ret;
}

int64_t MultiStore::delete_pending_writes(const std::string& pattern, bool local_only) {
  int64_t ret = 0;
  for (const auto& it : this->stores) {
    ret += it.second->delete_pending_writes(pattern, local_only);
  }
  return ret;
}

string MultiStore::str() const {
  string ret = "MultiStore(";
  for (const auto& it : this->stores) {
    if (ret.size() > 11) {
      ret += ", ";
    }
    ret += it.first;
    ret += '=';
    ret += it.second->str();
  }
  ret += ')';
  return ret;
}



void MultiStore::combine_simple_results(unordered_map<string, Error>& into,
    unordered_map<string, Error>&& from) {

  for (auto& from_it : from) {
    const string& from_key = from_it.first;
    auto& from_error = from_it.second;

    auto emplace_ret = into.emplace(from_key, from_error);
    if (!emplace_ret.second) {

      // errors take precedence over success, which takes precedence over "ignored"
      auto& into_error = emplace_ret.first->second;
      if ((into_error.ignored) ||
          (into_error.description.empty() && !from_error.ignored)) {
        into_error = from_error;
      }
    }
  }
}

void MultiStore::combine_read_results(
    unordered_map<string, unordered_map<string, ReadResult>>& into,
    unordered_map<string, unordered_map<string, ReadResult>>&& from) {
  // the maps are {pattern: {key_name: result}}
  for (auto& from_query_it : from) { // (pattern, {key_name: result})
    const string& from_query = from_query_it.first;
    auto& from_series_map = from_query_it.second;

    auto into_query_it = into.find(from_query);
    if (into_query_it == into.end()) {
      into.emplace(from_query, move(from_series_map));

    } else {
      auto& into_series_map = into_query_it->second;
      for (auto& from_series_it : from_series_map) { // (key_name, result)

        // attempt to insert the result. if it's already there, then merge the
        // data manually
        auto emplace_ret = into_series_map.emplace(from_series_it.first,
            move(from_series_it.second));
        if (!emplace_ret.second) {
          auto& existing_result = emplace_ret.first->second;
          auto& new_result = from_series_it.second;

          if (!existing_result.error.description.empty()) {
            // the existing result has an error; just leave it there

          } else if (existing_result.step == 0) {
            // the existing result is a missing series result. just replace it
            // entirely with the new result (which might have data)
            existing_result = move(new_result);

          } else if (new_result.step != 0) {
            // both results have data. seriously? dammit

            if (new_result.step != existing_result.step) {
              existing_result.error = make_error("merged results with different schemas");
            } else {
              existing_result.data.insert(existing_result.data.end(),
                  new_result.data.begin(), new_result.data.end());
              sort(existing_result.data.begin(), existing_result.data.end(),
                  [](const Datapoint& a, const Datapoint& b) {
                    return a.timestamp < b.timestamp;
                  });
              if (new_result.start_time < existing_result.start_time) {
                existing_result.start_time = new_result.start_time;
              }
              if (new_result.end_time > existing_result.end_time) {
                existing_result.end_time = new_result.end_time;
              }
            }
          }
        }
      }
    }
  }
}

void MultiStore::combine_find_results(unordered_map<string, FindResult>& into,
    unordered_map<string, FindResult>&& from) {
  for (auto& from_query_it : from) {
    const string& from_query = from_query_it.first;
    auto& from_result = from_query_it.second;
    auto into_query_it = into.find(from_query);
    if (into_query_it == into.end()) {
      into.emplace(from_query, move(from_result));

    } else {
      auto& into_result = into_query_it->second;
      if (!into_result.error.description.empty()) {
        continue;
      } else if (!from_result.error.description.empty()) {
        into_result.error = move(from_result.error);
        into_result.results.clear();
        continue;
      } else {
        bool needs_deduplication = !into_result.results.empty();
        into_result.results.insert(into_result.results.end(),
            make_move_iterator(from_result.results.begin()),
            make_move_iterator(from_result.results.end()));

        if (needs_deduplication) {
          auto& r = into_result.results;
          sort(r.begin(), r.end());
          size_t write_offset = 0;
          for (size_t read_offset = 0; read_offset < r.size();) {
            size_t run_start_offset = read_offset;
            for (read_offset++; (read_offset < r.size()) && (r[read_offset] == r[run_start_offset]); read_offset++);
            if (write_offset != run_start_offset) {
              r[write_offset] = r[run_start_offset];
            }
            write_offset++;
          }
          r.resize(write_offset);
        }
      }
    }
  }
}
