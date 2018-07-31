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

#include "CarbonConsistentHashRing.hh"

using namespace std;


ConsistentHashMultiStore::ConsistentHashMultiStore(
    const unordered_map<string, shared_ptr<Store>>& stores, int64_t precision) :
    MultiStore(stores), precision(precision), should_exit(false) {
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

unordered_map<string, string> ConsistentHashMultiStore::update_metadata(
    const SeriesMetadataMap& metadata, bool create_new,
    UpdateMetadataBehavior update_behavior, bool local_only) {
  unordered_map<uint8_t, unordered_map<string, SeriesMetadata>> partitioned_data;

  for (const auto& it : metadata) {
    uint8_t store_id = this->ring->host_id_for_key(it.first);
    partitioned_data[store_id].emplace(it.first, it.second);
  }

  unordered_map<string, string> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->update_metadata(it.second,
        create_new, update_behavior, local_only);
    for (const auto& result_it : results) {
      ret.emplace(move(result_it.first), move(result_it.second));
    }
  }
  return ret;
}

unordered_map<string, int64_t> ConsistentHashMultiStore::delete_series(
    const vector<string>& patterns, bool local_only) {

  // if a verify is in progress, we need to forward all deletes everywhere
  // because there could be keys on wrong backends. so we'll use the parent
  // class' method to do this
  if (this->verify_progress.in_progress()) {
    return this->MultiStore::delete_series(patterns, local_only);
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

  unordered_map<string, int64_t> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->delete_series(it.second, local_only);
    for (const auto& result_it : results) {
      ret[result_it.first] += result_it.second;
    }
  }
  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> ConsistentHashMultiStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time,
    bool local_only) {

  // see comment in delete_series about this
  if (this->verify_progress.in_progress()) {
    return this->MultiStore::read(key_names, start_time, end_time, local_only);
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

  // TODO: we should find some way to make the reads happen in parallel
  unordered_map<string, unordered_map<string, ReadResult>> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->read(it.second, start_time,
        end_time, local_only);
    this->combine_read_results(ret, move(results));
  }
  return ret;
}

unordered_map<string, string> ConsistentHashMultiStore::write(
    const unordered_map<string, Series>& data, bool local_only) {

  unordered_map<uint8_t, unordered_map<string, Series>> partitioned_data;
  for (const auto& it : data) {
    uint8_t store_id = this->ring->host_id_for_key(it.first);
    partitioned_data[store_id].emplace(it.first, it.second);
  }

  unordered_map<string, string> ret;
  for (const auto& it : partitioned_data) {
    const string& store_name = this->ring->host_for_id(it.first).name;
    auto results = this->stores[store_name]->write(it.second, local_only);
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
  stats.emplace("verify_restore_errors", this->verify_progress.restore_errors);
  stats.emplace("verify_find_queries_executed", this->verify_progress.find_queries_executed);
  return stats;
}

string ConsistentHashMultiStore::restore_series(const string& key_name,
      const string& data, bool combine_from_existing, bool local_only) {
  uint8_t store_id = this->ring->host_id_for_key(key_name);
  const string& store_name = this->ring->host_for_id(store_id).name;
  return this->stores[store_name]->restore_series(key_name, data,
      combine_from_existing, local_only);
}

string ConsistentHashMultiStore::serialize_series(const string& key_name,
    bool local_only) {
  uint8_t store_id = this->ring->host_id_for_key(key_name);
  const string& store_name = this->ring->host_for_id(store_id).name;
  return this->stores[store_name]->serialize_series(key_name, local_only);
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
    keys_moved(0), restore_errors(0), find_queries_executed(0),
    start_time(now()), end_time(this->start_time.load()), cancelled(false) { }

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

    auto find_results = this->find({pattern}, false);
    auto find_result_it = find_results.find(pattern);
    this->verify_progress.find_queries_executed++;
    if (find_result_it == find_results.end()) {
      log(WARNING, "[ConsistentHashMultiStore] find(%s) returned no results during verify",
          pattern.c_str());
      continue;
    }
    auto find_result = find_result_it->second;
    if (!find_result.error.empty()) {
      // TODO: should we retry this somehow?
      log(WARNING, "[ConsistentHashMultiStore] find(%s) returned error during verify: %s",
          pattern.c_str(), find_result.error.c_str());
      continue;
    }

    int64_t read_time = now() / 1000000;
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
      const string& store_name = this->ring->host_for_id(store_id).name;
      auto& responsible_store = this->stores.at(store_name);

      // find out which stores it actually exists on
      // TODO: we should find some way to make the reads happen in parallel
      unordered_map<string, unordered_map<string, ReadResult>> ret;
      for (const auto& store_it : this->stores) {
        // skip the store that the key is supposed to be in
        if (store_it.first == store_name) {
          continue;
        }

        if (this->verify_progress.repair) {
          // note: we set local_only = true here because the verify procedure
          // must run on ALL nodes in the cluster, so it makes sense for each
          // node to only be responsible for the series on its local disk
          string serialized = store_it.second->serialize_series({key_name}, true);
          if (serialized.empty()) {
            continue; // key isn't in this store (good)
          }

          // if we get here, then serialize() returned series data; merge it
          // into the correct store and delete the original
          auto res = responsible_store->restore_series(key_name, serialized,
              true, false);
          if (!res.empty()) {
            log(WARNING, "[ConsistentHashMultiStore] key %s could not be restored when moving from %s to %s (error: %s)",
                key_name.c_str(), store_it.first.c_str(), store_name.c_str(), res.c_str());
            this->verify_progress.restore_errors++;
          } else {
            log(INFO, "[ConsistentHashMultiStore] key %s moved from %s to %s",
                key_name.c_str(), store_it.first.c_str(), store_name.c_str());
            store_it.second->delete_series({key_name}, false);
            this->verify_progress.keys_moved++;
          }

        } else {
          auto results = store_it.second->read({key_name}, read_time, read_time,
              true);
          auto result_pattern_it = results.find(key_name);
          if (result_pattern_it == results.end()) {
            continue; // key isn't in this store (good)
          }
          auto result_it = result_pattern_it->second.find(key_name);
          if (result_it == result_pattern_it->second.end()) {
            log(WARNING, "[ConsistentHashMultiStore] read(%s) returned pattern with no key results during verify",
                pattern.c_str());
            continue;
          }
          auto result = result_it->second;
          if (result.step) {
            log(INFO, "[ConsistentHashMultiStore] key %s exists in store %s but should be in store %s",
                key_name.c_str(), store_it.first.c_str(), store_name.c_str());
            this->verify_progress.keys_moved++;
          }
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
