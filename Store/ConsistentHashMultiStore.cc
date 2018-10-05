#include "ConsistentHashMultiStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <phosg/Filesystem.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>
#include <stdexcept>
#include <string>
#include <vector>

#include "Utils/CarbonConsistentHashRing.hh"
#include "Utils/Errors.hh"

using namespace std;


ConsistentHashMultiStore::ConsistentHashMultiStore(
    const unordered_map<string, shared_ptr<Store>>& stores, int64_t precision) :
    MultiStore(stores), precision(precision), should_exit(false),
    read_from_all(false) {
  this->create_ring();
}

ConsistentHashMultiStore::~ConsistentHashMultiStore() {
  this->should_exit = true;
  if (this->verify_thread.joinable()) {
    this->verify_thread.join();
  }
}

int64_t ConsistentHashMultiStore::get_precision() const {
  return this->precision;
}

void ConsistentHashMultiStore::set_precision(int64_t new_precision) {
  this->precision = new_precision;
  // TODO: this isn't thread-safe! other threads may call host_id_for_key on an
  // object being destroyed here! fix this!
  this->create_ring();
}

unordered_map<string, Error> ConsistentHashMultiStore::update_metadata(
    const SeriesMetadataMap& metadata, bool create_new,
    UpdateMetadataBehavior update_behavior, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  unordered_map<uint8_t, unordered_map<string, SeriesMetadata>> partitioned_data;

  for (const auto& it : metadata) {
    uint8_t store_id = this->ring->host_id_for_key(it.first);
    partitioned_data[store_id].emplace(it.first, it.second);
  }
  profiler->checkpoint("partition_series");

  unordered_map<string, Error> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->update_metadata(it.second,
        create_new, update_behavior, skip_buffering, local_only, profiler);
    for (const auto& result_it : results) {
      ret.emplace(move(result_it.first), move(result_it.second));
    }
  }
  return ret;
}

unordered_map<string, int64_t> ConsistentHashMultiStore::delete_series(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {

  // if a verify is in progress, we need to forward all deletes everywhere
  // because there could be keys on wrong backends. so we'll use the parent
  // class' method to do this
  if (this->read_from_all || (this->verify_progress.in_progress() && this->verify_progress.repair)) {
    profiler->add_metadata("read_from_all", "true");
    return this->MultiStore::delete_series(patterns, local_only, profiler);
  }

  size_t host_count = this->ring->all_hosts().size();

  unordered_map<uint8_t, vector<string>> partitioned_data;
  for (const string& pattern : patterns) {
    // send patterns to all stores, but we can send non-patterns to only one
    // substore to reduce work
    if (this->token_is_pattern(pattern)) {
      for (size_t host_id = 0; host_id < host_count; host_id++) {
        partitioned_data[host_id].push_back(pattern);
      }
    } else {
      uint8_t store_id = this->ring->host_id_for_key(pattern);
      partitioned_data[store_id].push_back(pattern);
    }
  }
  profiler->checkpoint("partition_series");

  unordered_map<string, int64_t> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->delete_series(it.second,
        local_only, profiler);
    for (const auto& result_it : results) {
      ret[result_it.first] += result_it.second;
    }
  }
  return ret;
}

unordered_map<string, Error> ConsistentHashMultiStore::rename_series(
    const unordered_map<string, string>& renames, bool local_only,
    BaseFunctionProfiler* profiler) {

  // if a verify+repair is in progress or read_from_all is on, then renames
  // can't be forwarded to substores as an optimization; we have to emulate them
  // using read_all and write instead
  bool can_forward = !this->read_from_all && (!this->verify_progress.in_progress() || !this->verify_progress.repair);

  unordered_map<uint8_t, unordered_map<string, string>> renames_to_forward;
  unordered_map<string, string> renames_to_emulate;
  if (can_forward) {
    for (const auto& it : renames) {
      uint8_t from_store_id = this->ring->host_id_for_key(it.first);
      uint8_t to_store_id = this->ring->host_id_for_key(it.second);
      if (from_store_id == to_store_id) {
        renames_to_forward[to_store_id].emplace(it.first, it.second);
      } else {
        renames_to_emulate.emplace(it.first, it.second);
      }
    }
    profiler->add_metadata("can_forward", "true");
  } else {
    renames_to_emulate = renames;
    profiler->add_metadata("can_forward", "false");
  }
  profiler->checkpoint("partition_series");

  unordered_map<string, Error> ret;
  for (const auto& it : renames_to_forward) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->rename_series(it.second,
        local_only, profiler);
    this->combine_simple_results(ret, move(results));
    profiler->checkpoint(string_printf("forward_renames_store_%hhu", it.first));
  }

  for (const auto& it : renames_to_emulate) {
    const string& from_key_name = it.first;
    const string& to_key_name = it.second;
    uint8_t from_store_id = this->ring->host_id_for_key(from_key_name);
    uint8_t to_store_id = this->ring->host_id_for_key(to_key_name);
    auto& from_store = this->stores[this->ring->host_for_id(from_store_id).name];
    auto& to_store = this->stores[this->ring->host_for_id(to_store_id).name];

    auto read_all_result = from_store->read_all(from_key_name, false, profiler);
    profiler->checkpoint("read_all_" + it.first);
    if (!read_all_result.error.description.empty()) {
      ret.emplace(from_key_name, move(read_all_result.error));
      continue;
    }
    if (read_all_result.metadata.archive_args.empty()) {
      ret.emplace(from_key_name, make_error("series does not exist"));
      continue;
    }

    // create the series in the remote store if it doesn't exist already
    SeriesMetadataMap metadata_map({{to_key_name, read_all_result.metadata}});
    auto update_metadata_ret = to_store->update_metadata(metadata_map, true,
        UpdateMetadataBehavior::Recreate, true, false, profiler);
    profiler->checkpoint("update_metadata_" + to_key_name);
    try {
      Error& error = update_metadata_ret.at(to_key_name);
      if (!error.description.empty()) {
        ret.emplace(from_key_name, move(error));
        continue;
      }
    } catch (const out_of_range&) {
      ret.emplace(from_key_name, make_error("update_metadata returned no results"));
      continue;
    }

    // write all the data from the from series into the to series
    SeriesMap write_map({{to_key_name, move(read_all_result.data)}});
    auto write_ret = to_store->write(write_map, true, false, profiler);
    profiler->checkpoint("write_" + to_key_name);
    try {
      Error& error = write_ret.at(to_key_name);
      if (!error.description.empty()) {
        ret.emplace(from_key_name, move(error));
        continue;
      }
    } catch (const out_of_range&) {
      ret.emplace(from_key_name, make_error("write returned no results"));
      continue;
    }

    // delete the original series
    auto delete_ret = from_store->delete_series({from_key_name}, false,
        profiler);
    profiler->checkpoint("delete_series_" + from_key_name);
    int64_t num_deleted = delete_ret[from_key_name];
    if (num_deleted == 1) {
      ret.emplace(from_key_name, make_success());
    } else {
      ret.emplace(from_key_name, make_error("move successful, but delete failed"));
    }
  }

  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> ConsistentHashMultiStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time,
    bool local_only, BaseFunctionProfiler* profiler) {

  // see comment in delete_series about this
  if (this->read_from_all || (this->verify_progress.in_progress() && this->verify_progress.repair)) {
    profiler->add_metadata("read_from_all", "true");
    return this->MultiStore::read(key_names, start_time, end_time, local_only,
        profiler);
  }

  unordered_map<size_t, vector<string>> partitioned_data;
  for (size_t x = 0; x < key_names.size(); x++) {
    if (this->token_is_pattern(key_names[x])) {
      size_t num_hosts = this->ring->all_hosts().size();
      for (size_t store_id = 0; store_id < num_hosts; store_id++) {
        partitioned_data[store_id].push_back(key_names[x]);
      }
    } else {
      size_t store_id = this->ring->host_id_for_key(key_names[x]);
      partitioned_data[store_id].push_back(key_names[x]);
    }
  }
  profiler->checkpoint("partition_series");

  // TODO: we should find some way to make the reads happen in parallel
  unordered_map<string, unordered_map<string, ReadResult>> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->read(it.second, start_time,
        end_time, local_only, profiler);
    this->combine_read_results(ret, move(results));
  }
  return ret;
}

ReadAllResult ConsistentHashMultiStore::read_all(const string& key_name,
    bool local_only, BaseFunctionProfiler* profiler) {
  // see comment in delete_series about this
  if (this->read_from_all || (this->verify_progress.in_progress() && this->verify_progress.repair)) {
    profiler->add_metadata("read_from_all", "true");
    return this->MultiStore::read_all(key_name, local_only, profiler);
  }

  size_t store_id = this->ring->host_id_for_key(key_name);
  const string& store_name = this->ring->host_for_id(store_id).name;
  return this->stores[store_name]->read_all(key_name, local_only, profiler);
}

unordered_map<string, Error> ConsistentHashMultiStore::write(
    const unordered_map<string, Series>& data, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {

  unordered_map<uint8_t, unordered_map<string, Series>> partitioned_data;
  for (const auto& it : data) {
    uint8_t store_id = this->ring->host_id_for_key(it.first);
    partitioned_data[store_id].emplace(it.first, it.second);
  }
  profiler->checkpoint("partition_series");

  unordered_map<string, Error> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->write(it.second, skip_buffering,
        local_only, profiler);
    for (const auto& result_it : results) {
      ret.emplace(move(result_it.first), move(result_it.second));
    }
  }
  return ret;
}

unordered_map<string, int64_t> ConsistentHashMultiStore::get_stats(bool rotate) {
  auto stats = this->MultiStore::get_stats(rotate);
  stats.emplace("verify_in_progress", this->verify_progress.in_progress());
  stats.emplace("verify_keys_examined", this->verify_progress.keys_examined);
  stats.emplace("verify_keys_moved", this->verify_progress.keys_moved);
  stats.emplace("verify_read_all_errors", this->verify_progress.read_all_errors);
  stats.emplace("verify_update_metadata_errors", this->verify_progress.update_metadata_errors);
  stats.emplace("verify_write_errors", this->verify_progress.write_errors);
  stats.emplace("verify_delete_errors", this->verify_progress.delete_errors);
  stats.emplace("verify_find_queries_executed", this->verify_progress.find_queries_executed);
  return stats;
}

void ConsistentHashMultiStore::create_ring() {
  vector<ConsistentHashRing::Host> hosts;
  for (const auto& it : this->stores) {
    hosts.emplace_back(it.first, "", 0); // host/port are ignored
  }
  if (this->precision > 0) {
    this->ring = shared_ptr<ConsistentHashRing>(
        new ConstantTimeConsistentHashRing(hosts, this->precision));
  } else if (this->precision < 0) {
    this->ring = shared_ptr<ConsistentHashRing>(
        new CarbonConsistentHashRing(hosts, -this->precision));
  } else {
    throw invalid_argument("precision must be nonzero");
  }
}

ConsistentHashMultiStore::VerifyProgress::VerifyProgress() : keys_examined(0),
    keys_moved(0), read_all_errors(0), update_metadata_errors(0),
    write_errors(0), delete_errors(0), find_queries_executed(0),
    start_time(now()), end_time(this->start_time.load()), repair(false),
    cancelled(false) { }

bool ConsistentHashMultiStore::VerifyProgress::in_progress() const {
  return (this->end_time < this->start_time);
}

const ConsistentHashMultiStore::VerifyProgress& ConsistentHashMultiStore::get_verify_progress() const {
  return this->verify_progress;
}

bool ConsistentHashMultiStore::start_verify(bool repair) {
  if (this->verify_progress.in_progress()) {
    return false;
  }

  if (this->verify_thread.joinable()) {
    this->verify_thread.join();
  }

  this->verify_progress.repair = repair;
  this->verify_thread = thread(&ConsistentHashMultiStore::verify_thread_routine,
      this);
  return true;
}

bool ConsistentHashMultiStore::cancel_verify() {
  if (!this->verify_progress.in_progress()) {
    return false;
  }

  this->verify_progress.cancelled = true;
  return true;
}

void ConsistentHashMultiStore::verify_thread_routine() {
  log(INFO, "[ConsistentHashMultiStore] starting verify");

  this->verify_progress.end_time = 0;
  this->verify_progress.start_time = now();
  this->verify_progress.keys_examined = 0;
  this->verify_progress.keys_moved = 0;
  this->verify_progress.find_queries_executed = 0;
  this->verify_progress.cancelled = false;

  vector<string> pending_patterns;
  pending_patterns.emplace_back("*");
  while (!this->should_exit && !pending_patterns.empty() &&
         !this->verify_progress.cancelled) {
    string pattern = pending_patterns.back();
    pending_patterns.pop_back();

    string function_name = string_printf("verify(%s)", pattern.c_str());
    ProfilerGuard pg(create_internal_profiler(
        "ConsistentHashMultiStore::verify_thread_routine", function_name.c_str()));

    auto find_results = this->find({pattern}, false, pg.profiler.get());
    pg.profiler->checkpoint("find");

    auto find_result_it = find_results.find(pattern);
    this->verify_progress.find_queries_executed++;
    if (find_result_it == find_results.end()) {
      log(WARNING, "[ConsistentHashMultiStore] find(%s) returned no results during verify",
          pattern.c_str());
      continue;
    }
    auto find_result = find_result_it->second;
    if (!find_result.error.description.empty()) {
      // TODO: should we retry this somehow?
      log(WARNING, "[ConsistentHashMultiStore] find(%s) returned error during verify: %s",
          pattern.c_str(), find_result.error.description.c_str());
      continue;
    }

    for (const string& key_name : find_result.results) {
      if (this->should_exit) {
        break;
      }

      if (ends_with(key_name, ".*")) {
        pending_patterns.emplace_back(key_name);
        continue;
      }

      // find which store this key should go to
      uint8_t store_id = this->ring->host_id_for_key(key_name);
      const string& responsible_store_name = this->ring->host_for_id(store_id).name;
      auto& responsible_store = this->stores.at(responsible_store_name);

      // find out which stores it actually exists on
      // TODO: we should find some way to make the reads happen in parallel
      unordered_map<string, unordered_map<string, ReadResult>> ret;
      for (const auto& store_it : this->stores) {
        // skip the store that the key is supposed to be in
        if (store_it.first == responsible_store_name) {
          continue;
        }

        // note: we set local_only = true here because the verify procedure
        // must run on ALL nodes in the cluster, so it makes sense for each
        // node to only be responsible for the series on its local disk
        auto read_all_result = store_it.second->read_all(key_name, true,
            pg.profiler.get());
        pg.profiler->checkpoint("read_all");
        if (!read_all_result.error.description.empty()) {
          log(WARNING, "[ConsistentHashMultiStore] key %s could not be read from %s (error: %s)",
              key_name.c_str(), store_it.first.c_str(),
              read_all_result.error.description.c_str());
          this->verify_progress.read_all_errors++;
          continue;
        }
        if (read_all_result.metadata.archive_args.empty()) {
          continue; // key isn't in this store (good)
        }

        // count this key as needing to be moved
        this->verify_progress.keys_moved++;

        if (this->verify_progress.repair) {
          // move step 1: create the series in the remote store if it doesn't
          // exist already
          SeriesMetadataMap metadata_map({{key_name, read_all_result.metadata}});
          auto update_metadata_ret = responsible_store->update_metadata(
              metadata_map, true, UpdateMetadataBehavior::Ignore, true, false,
              pg.profiler.get());
          pg.profiler->checkpoint("update_metadata");
          try {
            const Error& error = update_metadata_ret.at(key_name);
            if (!error.description.empty() && !error.ignored) {
              log(WARNING, "[ConsistentHashMultiStore] update_metadata returned error (%s) when moving %s from %s to %s",
                  error.description.c_str(), key_name.c_str(),
                  store_it.first.c_str(), responsible_store_name.c_str());
              this->verify_progress.update_metadata_errors++;
              continue;
            }
          } catch (const out_of_range&) {
            log(WARNING, "[ConsistentHashMultiStore] update_metadata returned no results when moving %s from %s to %s",
                key_name.c_str(), store_it.first.c_str(), responsible_store_name.c_str());
            this->verify_progress.update_metadata_errors++;
            continue;
          }

          // move step 2: write all the data from the local series into the
          // remote series
          SeriesMap write_map({{key_name, move(read_all_result.data)}});
          auto write_ret = responsible_store->write(write_map, true, false,
              pg.profiler.get());
          pg.profiler->checkpoint("write");
          try {
            const Error& error = write_ret.at(key_name);
            if (!error.description.empty()) {
              log(WARNING, "[ConsistentHashMultiStore] write returned error (%s) when moving %s from %s to %s",
                  error.description.c_str(), key_name.c_str(),
                  store_it.first.c_str(), responsible_store_name.c_str());
              this->verify_progress.write_errors++;
              continue;
            }
          } catch (const out_of_range&) {
            log(WARNING, "[ConsistentHashMultiStore] write returned no results when moving %s from %s to %s",
                key_name.c_str(), store_it.first.c_str(), responsible_store_name.c_str());
            this->verify_progress.write_errors++;
            continue;
          }

          // at this point the data has been successfully moved; reads and
          // writes will now be consistent

          // move step 3: delete the original series. note that we use
          // local_only = true here because we also did so when read_all'ing
          // this series' data
          auto delete_ret = store_it.second->delete_series({key_name}, true,
              pg.profiler.get());
          pg.profiler->checkpoint("delete_series");
          int64_t num_deleted = delete_ret[key_name];
          if (num_deleted == 1) {
            log(INFO, "[ConsistentHashMultiStore] key %s moved from %s to %s",
                key_name.c_str(), store_it.first.c_str(), responsible_store_name.c_str());
          } else {
            log(WARNING, "[ConsistentHashMultiStore] key %s moved from %s to %s, but %" PRId64 " keys were deleted from the source store",
                key_name.c_str(), store_it.first.c_str(), responsible_store_name.c_str(),
                num_deleted);
            this->verify_progress.delete_errors++;
          }

        } else {
          log(INFO, "[ConsistentHashMultiStore] key %s exists in store %s but should be in store %s",
              key_name.c_str(), store_it.first.c_str(), responsible_store_name.c_str());
        }
      }

      this->verify_progress.keys_examined++;
    }
  }

  log(INFO, "[ConsistentHashMultiStore] verify terminated with %" PRId64
      " of %" PRId64 " keys moved", this->verify_progress.keys_moved.load(),
      this->verify_progress.keys_examined.load());
  this->verify_progress.end_time = now();
}

bool ConsistentHashMultiStore::get_read_from_all() const {
  return this->read_from_all;
}

bool ConsistentHashMultiStore::set_read_from_all(bool read_from_all) {
  bool old_read_from_all = this->read_from_all.exchange(read_from_all);
  if (old_read_from_all != read_from_all) {
    log(INFO, "[ConsistentHashMultiStore] read-all %s\n",
        read_from_all ? "enabled" : "disabled");
  }
  return old_read_from_all;
}
