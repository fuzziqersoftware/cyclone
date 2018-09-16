#include "WriteBufferStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <iostream>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "FunctionProfiler.hh"

using namespace std;


WriteBufferStore::WriteBufferStore(shared_ptr<Store> store,
    size_t num_write_threads, size_t batch_size,
    size_t max_update_metadatas_per_second, size_t max_write_batches_per_second,
    ssize_t disable_rate_limit_for_queue_length) : Store(), store(store),
    max_update_metadatas_per_second(max_update_metadatas_per_second),
    max_write_batches_per_second(max_write_batches_per_second),
    disable_rate_limit_for_queue_length(disable_rate_limit_for_queue_length),
    queued_update_metadatas(0), queued_writes(0), queued_datapoints(0),
    update_metadata_rate_limiter(this->max_update_metadatas_per_second),
    write_batch_rate_limiter(this->max_write_batches_per_second),
    batch_size(batch_size), should_exit(false) {
  while (this->write_threads.size() < num_write_threads) {
    this->write_threads.emplace_back(new WriteThread());
  }
  for (size_t x = 0; x < this->write_threads.size(); x++) {
    this->write_threads[x]->t = thread(&WriteBufferStore::write_thread_routine,
        this, x);
  }
}

WriteBufferStore::~WriteBufferStore() {
  this->should_exit = true;
  for (auto& wt : this->write_threads) {
    wt->t.join();
  }
}

size_t WriteBufferStore::get_batch_size() const {
  return this->batch_size;
}

void WriteBufferStore::set_batch_size(size_t new_value) {
  this->batch_size = new_value;
}

size_t WriteBufferStore::get_max_update_metadatas_per_second() const {
  return this->max_update_metadatas_per_second;
}

void WriteBufferStore::set_max_update_metadatas_per_second(size_t new_value) {
  this->max_update_metadatas_per_second = new_value;
}

size_t WriteBufferStore::get_max_write_batches_per_second() const {
  return this->max_write_batches_per_second;
}

void WriteBufferStore::set_max_write_batches_per_second(size_t new_value) {
  this->max_write_batches_per_second = new_value;
}

ssize_t WriteBufferStore::get_disable_rate_limit_for_queue_length() const {
  return this->disable_rate_limit_for_queue_length;
}

void WriteBufferStore::set_disable_rate_limit_for_queue_length(ssize_t new_value) {
  this->disable_rate_limit_for_queue_length = new_value;
}



shared_ptr<Store> WriteBufferStore::get_substore() const {
  return this->store;
}

void WriteBufferStore::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>>& autocreate_rules) {
  this->Store::set_autocreate_rules(autocreate_rules);
  this->store->set_autocreate_rules(autocreate_rules);
}

unordered_map<string, string> WriteBufferStore::update_metadata(
      const SeriesMetadataMap& metadata_map, bool create_new,
      UpdateMetadataBehavior update_behavior, bool skip_buffering,
      bool local_only, BaseFunctionProfiler* profiler) {
  profiler->add_metadata("series_count",
      string_printf("%zu", metadata_map.size()));

  if (skip_buffering) {
    profiler->add_metadata("skip_buffering", "true");
    return this->store->update_metadata(metadata_map, create_new,
        update_behavior, skip_buffering, local_only, profiler);
  }

  unordered_map<string, string> ret;

  for (const auto& it : metadata_map) {
    const auto& key_name = it.first;
    const auto& metadata = it.second;

    try {
      // TODO: we can probably reduce the lock scope here by a lot
      rw_guard g(this->queue_lock, true);

      auto emplace_ret = this->queue.emplace(piecewise_construct,
          forward_as_tuple(key_name),
          forward_as_tuple(metadata, create_new, update_behavior));

      if (emplace_ret.second) {
        this->queued_update_metadatas++;

      } else {
        if (update_behavior == UpdateMetadataBehavior::Ignore) {
          ret.emplace(key_name, "ignored");
          continue;
        }

        auto& command = emplace_ret.first->second;

        if (!command.create_new && create_new) {
          command.create_new = true;
        }

        if (update_behavior == UpdateMetadataBehavior::Recreate) {
          command.update_behavior = UpdateMetadataBehavior::Recreate;
          command.data.clear();

        } else if ((update_behavior == UpdateMetadataBehavior::Update) &&
            (command.update_behavior != UpdateMetadataBehavior::Recreate)) {
          command.update_behavior = UpdateMetadataBehavior::Update;
        }

        command.metadata = metadata;
      }
      ret.emplace(key_name, "");

    } catch (const exception& e) {
      ret.emplace(key_name, e.what());
    }
  }

  return ret;
}

unordered_map<string, int64_t> WriteBufferStore::delete_series(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {
  // remove any pending writes/creates

  profiler->add_metadata("pattern_count",
      string_printf("%zu", patterns.size()));

  // we move the items out of the map so the destructors will be called
  // outside of the lock context
  // TODO: make patterns work here... currently we only match key names
  vector<QueueItem> items_to_delete;
  size_t num_deleted_from_queue = 0;
  {
    rw_guard g(this->queue_lock, true);
    for (const auto& key_name : patterns) {
      auto it = this->queue.find(key_name);
      if (it == this->queue.end()) {
        continue;
      }
      items_to_delete.emplace_back(move(it->second));
      this->queue.erase(it);
      num_deleted_from_queue++;
    }
  }

  profiler->add_metadata("num_deleted_from_queue",
      string_printf("%zu", num_deleted_from_queue));
  profiler->checkpoint("write_buffer_delete");

  // issue the delete to the underlying store
  return this->store->delete_series(patterns, local_only, profiler);

  // items_to_delete are deleted here. importantly, this is after we call
  // delete_series on the substore to minimize the time between removing the
  // pending writes from the queue and deleting the keys in the substore
}

unordered_map<string, unordered_map<string, ReadResult>> WriteBufferStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time,
    bool local_only, BaseFunctionProfiler* profiler) {
  // TODO: merge with writes in progress somehow, so data in the process of
  // being written isn't temporarily invisible. this will be nontrivial since we
  // pop things from the queue in the write threads - there could be data on the
  // write threads' stacks that will be invisible here. potential solutions:
  // 1. have a vector of maps of in-progress writes for each thread and look
  //    through that too
  // 2. replace the queue with a vector of writes for each thread and distribute
  //    writes via ConsistentHashRing (will need a special case for
  //    num_threads=0)
  // 3. don't pop queue items in write threads and instead mark them as in
  //    progress somehow so other threads won't claim them

  auto ret = this->store->read(key_names, start_time, end_time, local_only,
      profiler);

  profiler->checkpoint("write_buffer_substore_read");

  for (auto& it : ret) { // (pattern, key_to_result)
    for (auto& it2 : it.second) { // (key, result)
      const auto& key_name = it2.first;
      auto& result = it2.second;

      map<uint32_t, double> data_to_insert;
      try {
        rw_guard g(this->queue_lock, false);
        auto& item = this->queue.at(key_name);

        bool series_exists_in_substore = (result.step != 0);
        bool item_can_create_series = !item.metadata.archive_args.empty();
        bool item_will_truncate_series = (item.update_behavior == UpdateMetadataBehavior::Recreate);

        // if this item will truncate the series, then behave as if the substore
        // read returned nothing, and apply the writes from the queue item only
        // TODO: we can skip the substore read in this case but I'm lazy
        if (item_can_create_series &&
            (item_will_truncate_series ||
             (item.create_new && !series_exists_in_substore))) {
          // if there's data, reduce its resolution to that of the first archive
          result.data.clear();
          if (!item.data.empty()) {
            result.step = item.metadata.archive_args[0].precision;
            result.start_time = 0;
            result.end_time = 0;

            uint32_t precision = item.metadata.archive_args[0].precision;
            uint32_t current_ts = 0;
            for (const auto& queue_data_it : item.data) {
              uint32_t effective_ts = (queue_data_it.first / precision) * precision;
              if (!current_ts || (effective_ts != current_ts)) {
                result.data.emplace_back();
                result.data.back().timestamp = effective_ts;
                current_ts = effective_ts;
              }
              result.data.back().value = queue_data_it.second;
              if (!result.start_time || (effective_ts < result.start_time)) {
                result.start_time = effective_ts;
              }
              if (effective_ts > result.end_time) {
                result.end_time = effective_ts;
              }
            }
          } else {
            result.step = item.metadata.archive_args[0].precision;
            result.start_time = start_time + result.step - (start_time % result.step);
            result.end_time = end_time + result.step - (end_time % result.step);
          }
          continue;
        }

        auto dp_it = item.data.lower_bound(start_time);
        if (dp_it == item.data.end()) {
          continue;
        }
        auto dp_end_it = item.data.upper_bound(end_time);
        if (dp_end_it == dp_it) {
          continue;
        }

        // note: we round the timestamps to the right interval based on the step
        // returned by the read result, disregarding any pending update_metadata
        // commands. if there's no read result (step == 0) then we get the step
        // from the first archive in the metadata (if this item can create the
        // series), then from the autocreate metadata. if none of these work,
        // return an error
        if (!series_exists_in_substore) {
          if (item_can_create_series && item.create_new) {
            result.step = item.metadata.archive_args[0].precision;
          }
          if (!result.step) {
            auto metadata = this->get_autocreate_metadata_for_key(it2.first);
            if (!metadata.archive_args.empty()) {
              result.step = metadata.archive_args[0].precision;
            }
          }
        }
        for (; dp_it != dp_end_it; dp_it++) {
          data_to_insert[(dp_it->first / result.step) * result.step] = dp_it->second;
        }

      } catch (const out_of_range& e) { }

      // if we get here, then data_to_insert cannot be empty
      if (!data_to_insert.empty()) {
        size_t ret_dp_index = 0;
        size_t ret_dp_check_count = result.data.size();
        bool needs_sort = false;
        for (const auto& dp : data_to_insert) {
          for (; (ret_dp_index < ret_dp_check_count) &&
                 (result.data[ret_dp_index].timestamp < dp.first); ret_dp_index++);

          if ((ret_dp_index < ret_dp_check_count) &&
              (result.data[ret_dp_index].timestamp == dp.first)) {
            result.data[ret_dp_index].value = dp.second;

          } else {
            result.data.emplace_back();
            auto& ret_dp = result.data.back();
            ret_dp.timestamp = dp.first;
            ret_dp.value = dp.second;
            // we only have to sort if there are datapoints after this one
            if (ret_dp_index < ret_dp_check_count) {
              needs_sort = true;
            }
          }
        }

        // sort by timestamp if needed
        if (needs_sort) {
          sort(result.data.begin(), result.data.end(), +[](const Datapoint& a, const Datapoint& b) {
            return a.timestamp < b.timestamp;
          });
        }

        // set start_time and end_time appropriately
        // note that results.data cannot be empty here because data_to_insert
        // was not empty
        result.start_time = result.data.begin()->timestamp;
        result.end_time = result.data.rbegin()->timestamp;
      }
    }
  }
  profiler->checkpoint("write_buffer_read_merge");

  return ret;
}

ReadAllResult WriteBufferStore::read_all(const string& key_name,
    bool local_only, BaseFunctionProfiler* profiler) {  
  // TODO: we really should merge the result with anything in the write buffer.
  // but I'm lazy and this is primarily used for verification+repair, during
  // which there should not be writes to series that exist on the wrong node
  return this->store->read_all(key_name, local_only, profiler);
}

unordered_map<string, string> WriteBufferStore::write(
    const unordered_map<string, Series>& data, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  if (skip_buffering) {
    profiler->add_metadata("skip_buffering", "true");
    return this->store->write(data, skip_buffering, local_only, profiler);
  }

  unordered_map<string, string> ret;
  for (const auto& it : data) {
    ret.emplace(it.first, "");
  }

  // construct a copy of data so we don't do this inside the lock context
  unordered_map<string, Series> mutable_data = data;
  {
    rw_guard g(this->queue_lock, true);
    for (auto& it : mutable_data) {
      auto emplace_ret = this->queue.emplace(piecewise_construct,
          forward_as_tuple(it.first), forward_as_tuple(it.second));
      if (emplace_ret.second) {
        this->queued_writes++;
        this->queued_datapoints += emplace_ret.first->second.data.size();
      } else {
        // there was already a queue for this key; need to merge the contents
        auto& existing_key_data = emplace_ret.first->second.data;
        for (const auto& it2 : it.second) {
          auto emplace_ret2 = existing_key_data.emplace(it2.timestamp, it2.value);
          if (emplace_ret2.second) {
            this->queued_datapoints++;
          } else {
            emplace_ret2.first->second = it2.value;
          }
        }
      }
    }
  }
  return ret;
}

unordered_map<string, FindResult> WriteBufferStore::find(
    const vector<string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {
  // the comment in read() about data temporarily disappearing applies here too
  auto ret = this->store->find(patterns, local_only, profiler);

  profiler->checkpoint("write_buffer_substore_find");

  // merge the find results with the create queue
  for (auto& it : ret) {
    // if there was an error, don't bother merging for this query
    if (!it.second.error.empty()) {
      continue;
    }

    size_t pattern_start_offset = it.first.find_first_of("[{*");
    if (pattern_start_offset != string::npos) {

      // this query is a pattern; need to merge with a queue range. but we can
      // make it faster by only skipping the parts of the queue that we know
      // won't match the pattern.
      // TODO: upper_bound can cause us to miss keys that end in multiple \xFF
      // chars. I'm too lazy to fix this; it's bad practice to use nonprintable
      // chars in key names anyway
      string lower_bound_str = it.first.substr(0, pattern_start_offset);
      string upper_bound_str = lower_bound_str + "\xFF";

      // convert the results to an unordered_set, then scan through the queue
      // and put stuff in the result set appropriately
      // TODO: is this slower than the super-complex code that was here before
      // that scanned through the vectors and didn't handle directory matches
      // correctly? at least the copies aren't within the lock context
      unordered_set<string> results(
          make_move_iterator(it.second.results.begin()),
          make_move_iterator(it.second.results.end()));

      // scan the queue segment that matches the prefix we're looking for
      {
        rw_guard g(this->queue_lock, false);
        auto queue_it = this->queue.lower_bound(lower_bound_str);
        auto queue_end_it = this->queue.upper_bound(upper_bound_str);
        for (; queue_it != queue_end_it; queue_it++) {
          // we should only add the queue item if it has create_new = true. if
          // it doesn't and the series doesn't exist in the substore, then this
          // queued command won't create it.
          // TODO: we should consider autocreate rules here too
          if (!queue_it->second.create_new) {
            continue;
          }

          if (this->name_matches_pattern(queue_it->first, it.first)) {
            // the queue item directly matches the pattern; just add it
            results.emplace(queue_it->first);
          } else {
            // the queue item doesn't match the pattern; check if any of its
            // directories do
            string item = queue_it->first;
            while (!item.empty()) {
              size_t dot_pos = item.rfind('.');
              if (dot_pos == string::npos) {
                break;
              }
              item.resize(dot_pos);
              if (this->name_matches_pattern(item, it.first)) {
                results.emplace(item + ".*");
              }
            }
          }
        }
      }

      // move the results back into the ret vector
      // TODO: is it safe to do this? presumably since we have an iterator we
      // don't need to worry about the hash result changing?
      it.second.results.clear();
      for (auto results_it = results.begin(); results_it != results.end(); results_it++) {
        it.second.results.emplace_back(move(*results_it));
      }

    } else {
      // this query is not a pattern - we only have to spot-check the queue if
      // there are no results; if there are results, we don't have to check the
      // queue at all
      if (it.second.results.empty()) {
        bool should_insert = false;
        {
          rw_guard g(this->queue_lock, false);
          should_insert = this->queue.count(it.first);
        }
        if (should_insert) {
          it.second.results.emplace_back(it.first);
        }
      }
    }
  }
  profiler->checkpoint("write_buffer_find_merge");

  return ret;
}


void WriteBufferStore::flush() {
  auto profiler = create_profiler("WriteBufferStore::flush");
  unordered_map<string, Series> write_batch;
  uint64_t start_time = now();
  {
    rw_guard g(this->queue_lock, true);
    log(INFO, "[WriteBufferStore] synchronous flush (%zu commands to run)",
        this->queue.size());

    while (!this->queue.empty()) {
      auto it = this->queue.begin();
      const auto& key_name = it->first;
      auto& command = it->second;

      // if archive_args isn't empty, there was an update_metadata request
      if (!command.metadata.archive_args.empty()) {
        this->store->update_metadata({{key_name, command.metadata}},
            command.create_new, command.update_behavior, false, false,
            profiler.get());
      }

      // if data isn't empty, there was a write request
      if (!command.data.empty()) {
        auto& series = write_batch.emplace(piecewise_construct,
            forward_as_tuple(key_name), forward_as_tuple()).first->second;
        for (const auto& pt : command.data) {
          series.emplace_back();
          series.back().timestamp = pt.first;
          series.back().value = pt.second;
        }
      }

      this->queue.erase(it);
    }
    this->queued_update_metadatas = 0;
    this->queued_writes = 0;
    this->queued_datapoints = 0;
  }
  uint64_t lock_end_time = now();

  if (!write_batch.empty()) {
    this->store->write(write_batch, false, false, profiler.get());
  }

  uint64_t end_time = now();
  log(INFO, "[WriteBufferStore] synchronous flush took %llu usecs (%llu usecs locked)",
      end_time - start_time, lock_end_time - start_time);
}

unordered_map<string, int64_t> WriteBufferStore::get_stats(bool rotate) {
  unordered_map<string, int64_t> ret = this->store->get_stats(rotate);

  size_t queue_length;
  {
    rw_guard g(this->queue_lock, false);
    queue_length = this->queue.size();
  }

  int64_t total_queue_sweep_time = 0;
  size_t counted_queue_sweep_times = 0;
  for (const auto& wt : this->write_threads) {
    int64_t queue_sweep_time = wt->queue_sweep_time;
    if (queue_sweep_time > 0) {
      total_queue_sweep_time += queue_sweep_time;
      counted_queue_sweep_times++;
    }
  }
  if (counted_queue_sweep_times == 0) {
    total_queue_sweep_time = -1;
  } else {
    total_queue_sweep_time /= counted_queue_sweep_times;
  }

  ret.emplace("queue_commands", queue_length);
  ret.emplace("queue_update_metadatas", this->queued_update_metadatas);
  ret.emplace("queue_writes", this->queued_writes);
  ret.emplace("queue_datapoints", this->queued_datapoints);
  ret.emplace("queue_batch_size", this->batch_size);
  ret.emplace("queue_sweep_time", total_queue_sweep_time);
  ret.emplace("max_update_metadatas_per_second", this->max_update_metadatas_per_second);
  ret.emplace("max_write_batches_per_second", this->max_write_batches_per_second);
  ret.emplace("disable_rate_limit_for_queue_length", this->disable_rate_limit_for_queue_length);
  return ret;
}

int64_t WriteBufferStore::delete_from_cache(const std::string& path,
    bool local_only) {
  return this->store->delete_from_cache(path, local_only);
}

int64_t WriteBufferStore::delete_pending_writes(const string& pattern,
    bool local_only) {

  // if the pattern is '*' or empty, delete everything
  if (pattern.empty() || (pattern == "*")) {
    // swap the map with a blank object so the destructors are called outside of
    // the lock context
    map<string, QueueItem> local_queue;
    {
      rw_guard g(this->queue_lock, true);
      local_queue.swap(this->queue);
      this->queued_update_metadatas = 0;
      this->queued_writes = 0;
      this->queued_datapoints = 0;
    }

    return local_queue.size() + this->store->delete_pending_writes(
        pattern, local_only);
  }

  // else, delete everything that matches the pattern
  size_t num_queue_items_deleted = 0;
  size_t num_update_metadatas_deleted = 0;
  size_t num_writes_deleted = 0;
  size_t num_datapoints_deleted = 0;

  rw_guard g(this->queue_lock, true);
  for (auto it = this->queue.begin(); it != this->queue.end();) {
    if (this->name_matches_pattern(it->first, pattern)) {
      num_queue_items_deleted++;
      if (!it->second.metadata.archive_args.empty()) {
        num_update_metadatas_deleted++;
      }
      if (!it->second.data.empty()) {
        num_writes_deleted++;
        num_datapoints_deleted += it->second.data.size();
      }
      // TODO: maybe move these destructor calls out of the lock context too
      it = this->queue.erase(it);
    } else {
      it++;
    }
  }

  this->queued_update_metadatas -= num_update_metadatas_deleted;
  this->queued_writes -= num_writes_deleted;
  this->queued_datapoints -= num_datapoints_deleted;

  return num_queue_items_deleted + this->store->delete_pending_writes(
      pattern, local_only);
}

string WriteBufferStore::str() const {
  return "WriteBufferStore(" + this->store->str() + ")";
}


WriteBufferStore::QueueItem::QueueItem(const SeriesMetadata& metadata,
    bool create_new, UpdateMetadataBehavior update_behavior) :
    metadata(metadata), create_new(create_new),
    update_behavior(update_behavior) { }

WriteBufferStore::QueueItem::QueueItem(const Series& data) : create_new(true),
    update_behavior(UpdateMetadataBehavior::Ignore) {
  for (const auto& dp : data) {
    this->data[dp.timestamp] = dp.value;
  }
}



WriteBufferStore::WriteThread::WriteThread() : t(), last_queue_restart(0),
    queue_sweep_time(0) { }

void WriteBufferStore::write_thread_routine(size_t thread_index) {
  string queue_position;

  auto& wt = this->write_threads[thread_index];
  wt->last_queue_restart = now();
  wt->queue_sweep_time = -1;

  while (!this->should_exit) {
    bool enable_rate_limits = true;

    auto profiler = create_profiler("WriteBufferStore::write_thread_routine");

    // pick out a batch of creates from the create queue
    map<string, QueueItem> commands;
    {
      rw_guard g(this->queue_lock, true);

      ssize_t disable_rate_limit_threshold = this->disable_rate_limit_for_queue_length;
      if ((disable_rate_limit_threshold >= 0) &&
          (this->queue.size() >= static_cast<size_t>(disable_rate_limit_threshold))) {
        enable_rate_limits = false;
      }

      auto it = this->queue.lower_bound(queue_position);
      while (it != this->queue.end() && commands.size() < this->batch_size) {
        commands.emplace(it->first, move(it->second));
        it = this->queue.erase(it);
      }
      if (it == this->queue.end()) {
        queue_position.clear();
        wt->queue_sweep_time = now() - wt->last_queue_restart;
        wt->last_queue_restart += wt->queue_sweep_time;
      } else {
        queue_position = it->first;
      }
    }
    profiler->checkpoint("get_queue_items");

    // execute the creates in order, and batch the writes
    size_t write_batch_datapoints = 0;
    unordered_map<string, Series> write_batch;
    for (const auto& it : commands) {
      const auto& key_name = it.first;
      auto& command = it.second;

      // if archive_args isn't empty, there was a create request
      if (!command.metadata.archive_args.empty()) {
        if (enable_rate_limits && this->max_update_metadatas_per_second) {
          uint64_t delay = this->update_metadata_rate_limiter.delay_until_next_action();
          if (delay) {
            usleep(delay);
          }
        }

        try {
          this->store->update_metadata({{key_name, command.metadata}},
              command.create_new, command.update_behavior, false, false,
              profiler.get());
          this->queued_update_metadatas--;
        } catch (const exception& e) {
          log(WARNING, "[WriteBufferStore] update_metadata failed on key=%s (%s)",
              key_name.c_str(), e.what());
          continue;
        }
      }

      // if data isn't empty, there was a write request
      if (!command.data.empty()) {
        auto& series = write_batch.emplace(piecewise_construct,
            forward_as_tuple(key_name), forward_as_tuple()).first->second;
        for (const auto& pt : command.data) {
          series.emplace_back();
          series.back().timestamp = pt.first;
          series.back().value = pt.second;
        }
        write_batch_datapoints += command.data.size();
        this->queued_writes--;
      }
    }
    profiler->checkpoint("execute_update_metadatas");

    if (!write_batch.empty()) {
      if (enable_rate_limits && this->max_write_batches_per_second) {
        uint64_t delay = this->write_batch_rate_limiter.delay_until_next_action();
        if (delay) {
          usleep(delay);
        }
      }

      this->queued_datapoints -= write_batch_datapoints;
      try {
        auto write_errors = this->store->write(write_batch, false, false,
            profiler.get());
        for (const auto& it : write_errors) {
          if (!it.second.empty()) {
            log(WARNING, "[WriteBufferStore] write error on key %s: %s",
                it.first.c_str(), it.second.c_str());
          }
        }
      } catch (const exception& e) {
        log(WARNING, "[WriteBufferStore] write failed for batch of %zu keys (%s)",
            write_batch.size(), e.what());
      }
      profiler->checkpoint("execute_writes");
    }

    // TODO: if there were failed items, put them back in the queue

    // don't profile the sleep call below
    profiler.reset();

    // if there was nothing to do, wait a second
    if (commands.empty()) {
      sleep(1);
    }
  }
}
