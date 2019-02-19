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



class QueryStore::QueryStoreReadTask : public Store::ReadTask {
public:
  virtual ~QueryStoreReadTask() = default;
  QueryStoreReadTask(StoreTaskManager* m, QueryStore* s,
      shared_ptr<const ReadArguments> args, BaseFunctionProfiler* profiler) :
      ReadTask(m, args, profiler), store(s) {
    for (const auto& query : this->args->key_names) {
      try {
        auto tokens = tokenize_query(query);
        this->parsed_queries.emplace(query, parse_query(tokens));
      } catch (const exception& e) {
        this->return_value[query][query].error = make_error(e.what());
      }
    }
    this->profiler->checkpoint("parse_query");

    // find all the read patterns and execute them all
    // TODO: we can probably do something better than this (copying the
    // unordered_set to a vector)
    shared_ptr<Store::ReadArguments> subread_args(new Store::ReadArguments());
    vector<string>& substore_reads = subread_args->key_names;
    subread_args->start_time = args->start_time;
    subread_args->end_time = args->end_time;
    subread_args->local_only = args->local_only;
    {
      unordered_set<string> substore_reads_set;
      for (const auto& it : parsed_queries) {
        QueryStore::extract_series_references_into(substore_reads_set, it.second);
      }
      substore_reads.insert(substore_reads.end(), substore_reads_set.begin(),
          substore_reads_set.end());
    }
    profiler->checkpoint("extract_series_references");

    this->read_task = this->store->store->read(this->manager, subread_args, this->profiler);
    this->delegate(this->read_task, bind(&QueryStoreReadTask::on_read_complete, this));
  }

  void on_read_complete() {
    auto substore_results = this->read_task->value();
    this->profiler->checkpoint("query_substore_read");

    // now apply the relevant functions on top of them
    // TODO: if a series is only referenced once, we probably can move the data
    // instead of copying
    for (auto& it : this->parsed_queries) {
      auto error = this->store->execute_query(it.second, substore_results);
      if (!error.description.empty()) {
        this->return_value[it.first][it.first].error = move(error);
      } else {
        this->return_value.emplace(it.first, it.second.series_data);
      }
    }
    profiler->checkpoint("execute_query");

    this->set_complete();
  }

private:
  QueryStore* store;
  unordered_map<string, Query> parsed_queries;

  shared_ptr<ReadTask> read_task;
};



QueryStore::QueryStore(shared_ptr<Store> store) : Store(), store(store) { }

shared_ptr<Store> QueryStore::get_substore() const {
  return this->store;
}

void QueryStore::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>>& autocreate_rules) {
  this->Store::set_autocreate_rules(autocreate_rules);
  this->store->set_autocreate_rules(autocreate_rules);
}

shared_ptr<Store::UpdateMetadataTask> QueryStore::update_metadata(StoreTaskManager* m,
    shared_ptr<const UpdateMetadataArguments> args, BaseFunctionProfiler* profiler) {
  return this->store->update_metadata(m, args, profiler);
}

shared_ptr<Store::DeleteSeriesTask> QueryStore::delete_series(StoreTaskManager* m,
    shared_ptr<const DeleteSeriesArguments> args, BaseFunctionProfiler* profiler) {
  return this->store->delete_series(m, args, profiler);
}

shared_ptr<Store::RenameSeriesTask> QueryStore::rename_series(StoreTaskManager* m,
    shared_ptr<const RenameSeriesArguments> args, BaseFunctionProfiler* profiler) {
  return this->store->rename_series(m, args, profiler);
}

shared_ptr<Store::ReadTask> QueryStore::read(StoreTaskManager* m,
    shared_ptr<const ReadArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<ReadTask>(new QueryStoreReadTask(m, this, args, profiler));
}

shared_ptr<Store::ReadAllTask> QueryStore::read_all(StoreTaskManager* m,
    shared_ptr<const ReadAllArguments> args, BaseFunctionProfiler* profiler) {
  return this->store->read_all(m, args, profiler);
}

shared_ptr<Store::WriteTask> QueryStore::write(StoreTaskManager* m,
    shared_ptr<const WriteArguments> args, BaseFunctionProfiler* profiler) {
  return this->store->write(m, args, profiler);
}

shared_ptr<Store::FindTask> QueryStore::find(StoreTaskManager* m,
    shared_ptr<const FindArguments> args, BaseFunctionProfiler* profiler) {
  return this->store->find(m, args, profiler);
}

unordered_map<string, int64_t> QueryStore::get_stats(bool rotate) {
  return this->store->get_stats();
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
