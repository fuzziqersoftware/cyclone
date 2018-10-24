#include "QueryStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <phosg/Strings.hh>
#include <stdexcept>
#include <string>
#include <vector>

#include "Query/Functions.hh"
#include "Query/Parser.hh"
#include "Utils/Errors.hh"

using namespace std;


QueryStore::QueryStore(shared_ptr<Store> store) : Store(), store(store) { }

shared_ptr<Store> QueryStore::get_substore() const {
  return this->store;
}

void QueryStore::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>>& autocreate_rules) {
  this->Store::set_autocreate_rules(autocreate_rules);
  this->store->set_autocreate_rules(autocreate_rules);
}

unordered_map<string, Error> QueryStore::update_metadata(
    const SeriesMetadataMap& metadata, bool create_new,
    UpdateMetadataBehavior update_behavior, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  return this->store->update_metadata(metadata, create_new, update_behavior,
      skip_buffering, local_only, profiler);
}

unordered_map<string, DeleteResult> QueryStore::delete_series(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {
  return this->store->delete_series(patterns, local_only, profiler);
}

unordered_map<string, Error> QueryStore::rename_series(
    const unordered_map<string, string>& renames, bool local_only,
    BaseFunctionProfiler* profiler) {
  return this->store->rename_series(renames, local_only, profiler);
}

unordered_map<string, unordered_map<string, ReadResult>> QueryStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time,
    bool local_only, BaseFunctionProfiler* profiler) {

  unordered_map<string, Query> parsed_queries;
  unordered_map<string, unordered_map<string, ReadResult>> ret;
  for (const auto& query : key_names) {
    try {
      auto tokens = tokenize_query(query);
      parsed_queries.emplace(query, parse_query(tokens));
    } catch (const exception& e) {
      ret[query][query].error = make_error(e.what());
    }
  }
  profiler->checkpoint("parse_query");

  // find all the read patterns and execute them all
  // TODO: we can probably do something better than this (copying the
  // unordered_set to a vector)
  vector<string> substore_reads;
  {
    unordered_set<string> substore_reads_set;
    for (const auto& it : parsed_queries) {
      this->extract_series_references_into(substore_reads_set, it.second);
    }
    substore_reads.insert(substore_reads.end(), substore_reads_set.begin(),
        substore_reads_set.end());
  }
  profiler->checkpoint("extract_series_references");

  auto substore_results = this->store->read(substore_reads, start_time,
      end_time, local_only, profiler);
  profiler->checkpoint("query_substore_read");

  // now apply the relevant functions on top of them
  // TODO: if a series is only referenced once, we probably can move the data
  // instead of copying
  for (auto& it : parsed_queries) {
    auto error = this->execute_query(it.second, substore_results);
    if (!error.description.empty()) {
      ret[it.first][it.first].error = error;
    } else {
      ret.emplace(it.first, it.second.series_data);
    }
  }
  profiler->checkpoint("execute_query");

  return ret;
}

ReadAllResult QueryStore::read_all(const string& key_name, bool local_only,
    BaseFunctionProfiler* profiler) {
  return this->store->read_all(key_name, local_only, profiler);
}

unordered_map<string, Error> QueryStore::write(
    const unordered_map<string, Series>& data, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  return this->store->write(data, skip_buffering, local_only, profiler);
}

unordered_map<string, FindResult> QueryStore::find(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {
  return this->store->find(patterns, local_only, profiler);
}

unordered_map<string, int64_t> QueryStore::get_stats(bool rotate) {
  return this->store->get_stats();
}

int64_t QueryStore::delete_from_cache(const std::string& path, bool local_only) {
  return this->store->delete_from_cache(path, local_only);
}

int64_t QueryStore::delete_pending_writes(const std::string& pattern, bool local_only) {
  return this->store->delete_pending_writes(pattern, local_only);
}

string QueryStore::str() const {
  return "QueryStore(" + this->store->str() + ")";
}



void QueryStore::extract_series_references_into(
    unordered_set<string>& substore_reads_set, const Query& q) {
  if (q.type == Query::Type::SeriesReference) {
    substore_reads_set.emplace(q.string_data);
  } else if (q.type == Query::Type::FunctionCall) {
    for (const auto& subq : q.function_call_args) {
      QueryStore::extract_series_references_into(substore_reads_set, subq);
    }
  }
}

Error QueryStore::execute_query(Query& q,
    const unordered_map<string, unordered_map<string, ReadResult>>& substore_results) {

  if (q.type == Query::Type::SeriesReference) {
    q.series_data = substore_results.at(q.string_data);
    q.computed = true;

  } else if (q.type == Query::Type::FunctionCall) {
    auto fn = get_query_function(q.string_data);
    if (!fn) {
      q.series_data[q.str()].error = make_error(
          "function does not exist: " + q.string_data);
    } else {
      for (auto& subq : q.function_call_args) {
        auto e = this->execute_query(subq, substore_results);
        if (!e.description.empty()) {
          return e;
        }
      }
      q.series_data = fn(q.function_call_args);
    }
    q.computed = true;

  } else {
    q.series_data[q.str()].error = make_error("incorrect query type: " + q.str());
    q.computed = true;
  }

  return make_success();
}
