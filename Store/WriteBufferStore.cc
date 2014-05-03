#include "WriteBufferStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

using namespace std;


WriteBufferStore::WriteBufferStore(shared_ptr<Store> store,
    size_t num_write_threads, size_t batch_size) : Store(), store(store),
    queue(), queue_lock(), batch_size(batch_size), should_exit(false),
    write_threads() {
  while (this->write_threads.size() < num_write_threads) {
    this->write_threads.emplace_back(&WriteBufferStore::write_thread_routine, this);
  }
}

WriteBufferStore::~WriteBufferStore() {
  this->should_exit = true;
  for (auto& t : this->write_threads) {
    t.join();
  }
}

void WriteBufferStore::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>> autocreate_rules) {
  this->store->set_autocreate_rules(autocreate_rules);
}

unordered_map<string, string> WriteBufferStore::update_metadata(
      const SeriesMetadataMap& metadata_map, bool create_new,
      UpdateMetadataBehavior update_behavior) {

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

unordered_map<string, string> WriteBufferStore::delete_series(
    const vector<string>& key_names) {
  // remove any pending writes/creates
  {
    // we move the items out of the map so the destructors will be called
    // outside of the lock context
    // TODO: make sure doing this doesn't call the copy constructor, lolz
    vector<QueueItem> items_to_delete;
    {
      rw_guard g(this->queue_lock, true);
      for (const auto& key_name : key_names) {
        auto it = this->queue.find(key_name);
        if (it == this->queue.end()) {
          continue;
        }
        items_to_delete.emplace_back(move(it->second));
        this->queue.erase(it);
      }
    }
  }

  // issue the delete to the underlying store
  return this->store->delete_series(key_names);
}

unordered_map<string, ReadResult> WriteBufferStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time) {
  // TODO: merge with writes in progress somehow, so data in the process of
  // being written isn't temporarily invisible. this will be nontrivial since we
  // pop things from the queue in the write threads - there could be data on the
  // write threads' stacks that will be invisible here. potential solutions:
  // 1. have a vector of maps of in-progress writes for each thread and look
  //    through that too
  // 2. replace the queue with a vector of writes for each thread and distribute
  //    writes via ConsistentHashRing (will need a special case for
  //    num_threads=0)

  auto ret = this->store->read(key_names, start_time, end_time);

  uint32_t range_secs = time(NULL) - start_time;

  for (auto& it : ret) {
    map<uint32_t, double> data_to_insert;
    try {
      rw_guard g(this->queue_lock, false);
      auto& item = this->queue.at(it.first);

      // if archive_args isn't blank and there's a Recreate request, behave as
      // if the archive is blank (the Recreate will truncate it, so it's
      // equivalent to enqueueing a write that deletes all existing datapoints)
      // TODO: can we skip the substore read in this case? might not be worth it
      // since this is probably pretty rare
      if (!item.metadata.archive_args.empty() && 
          ((item.update_behavior == UpdateMetadataBehavior::Recreate) ||
           (item.create_new && it.second.metadata.archive_args.empty()))) {
        it.second.metadata = item.metadata;

        // if there's data, reduce its resolution to that of the first archive
        it.second.data.clear();
        if (!item.data.empty()) {
          uint32_t precision = item.metadata.archive_args[0].precision;
          uint32_t current_ts = 0;
          for (const auto& queue_data_it : item.data) {
            uint32_t effective_ts = (queue_data_it.first / precision) * precision;
            if (!current_ts || (effective_ts != current_ts)) {
              it.second.data.emplace_back();
              it.second.data.back().timestamp = effective_ts;
              current_ts = effective_ts;
            }
            it.second.data.back().value = queue_data_it.second;
          }
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

      // note: we round the timestamps to the right interval based on the
      // archive metadata returned by the read result, disregarding any pending
      // update_metadata commands. if no usable archives are found, skip the
      // merge step for that series
      uint32_t archive_index;
      uint32_t precision = 0;
      for (archive_index = 0;
           (precision == 0) && (archive_index < it.second.metadata.archive_args.size());
           archive_index++) {
        const auto& archive = it.second.metadata.archive_args[archive_index];
        if (archive.points * archive.precision >= range_secs) {
          precision = archive.precision;
        }
      }
      if (precision) {
        for (; dp_it != dp_end_it; dp_it++) {
          data_to_insert[(dp_it->first / precision) * precision] = dp_it->second;
        }
      }

    } catch (const out_of_range& e) { }

    // if we get here, then data_to_insert cannot be empty
    size_t ret_dp_index = 0;
    size_t ret_dp_check_count = it.second.data.size();
    bool needs_sort = false;
    for (const auto& dp : data_to_insert) {
      for (; (ret_dp_index < ret_dp_check_count) &&
             (it.second.data[ret_dp_index].timestamp < dp.first); ret_dp_index++);

      if ((ret_dp_index < ret_dp_check_count) &&
          (it.second.data[ret_dp_index].timestamp == dp.first)) {
        it.second.data[ret_dp_index].value = dp.second;

      } else {
        it.second.data.emplace_back();
        auto& ret_dp = it.second.data.back();
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
      sort(it.second.data.begin(), it.second.data.end(), [](const Datapoint& a, const Datapoint& b) {
        return a.timestamp < b.timestamp;
      });
    }
  }

  return ret;
}

unordered_map<string, string> WriteBufferStore::write(
    const unordered_map<string, Series>& data) {
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
    const vector<string>& patterns) {
  // the comment in read() about data temporarily disappearing applies here too
  auto ret = this->store->find(patterns);

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

  return ret;
}


void WriteBufferStore::flush() {
  log(INFO, "[WriteBufferStore] synchronous flush");

  unordered_map<string, Series> write_batch;
  uint64_t start_time = now();
  {
    rw_guard g(this->queue_lock, true);
    while (!this->queue.empty()) {
      auto it = this->queue.begin();
      const auto& key_name = it->first;
      auto& command = it->second;

      // if archive_args isn't empty, there was an update_metadata request
      if (!command.metadata.archive_args.empty()) {
        this->store->update_metadata({{key_name, command.metadata}},
            command.create_new, command.update_behavior);
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
    this->store->write(write_batch);
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
  ret.emplace("queue_length", queue_length);
  ret.emplace("queued_update_metadatas", this->queued_update_metadatas);
  ret.emplace("queued_writes", this->queued_writes);
  ret.emplace("queued_datapoints", this->queued_datapoints);

  return ret;
}


WriteBufferStore::QueueItem::QueueItem(const SeriesMetadata& metadata,
    bool create_new, UpdateMetadataBehavior update_behavior) :
    metadata(metadata), create_new(create_new),
    update_behavior(update_behavior) { }

WriteBufferStore::QueueItem::QueueItem(const Series& data) {
  for (const auto& dp : data) {
    this->data[dp.timestamp] = dp.value;
  }
}


void WriteBufferStore::write_thread_routine() {
  string queue_position;

  while (!this->should_exit) {

    // pick out a batch of creates from the create queue
    map<string, QueueItem> commands;
    {
      rw_guard g(this->queue_lock, true);
      auto it = this->queue.lower_bound(queue_position);
      while (it != this->queue.end() && commands.size() < this->batch_size) {
        commands.emplace(it->first, move(it->second));
        it = this->queue.erase(it);
      }
      if (it == this->queue.end()) {
        queue_position.clear();
      } else {
        queue_position = it->first;
      }
    }

    // execute the creates in order, and batch the writes
    size_t write_batch_datapoints = 0;
    unordered_map<string, Series> write_batch;
    for (const auto& it : commands) {
      const auto& key_name = it.first;
      auto& command = it.second;

      // if archive_args isn't empty, there was a create request
      if (!command.metadata.archive_args.empty()) {
        this->store->update_metadata({{key_name, command.metadata}},
            command.create_new, command.update_behavior);
        this->queued_update_metadatas--;
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

    if (!write_batch.empty()) {
      this->store->write(write_batch);
    }
    this->queued_datapoints -= write_batch_datapoints;

    // if there was nothing to do, wait a second
    if (commands.empty()) {
      sleep(1);
    }
  }
}
