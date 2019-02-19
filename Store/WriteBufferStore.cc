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

#include "Utils/FunctionProfiler.hh"
#include "Utils/Errors.hh"

using namespace std;


WriteBufferStore::WriteBufferStore(shared_ptr<Store> store,
    size_t num_write_threads, size_t batch_size,
    size_t max_update_metadatas_per_second, size_t max_write_batches_per_second,
    ssize_t disable_rate_limit_for_queue_length, bool merge_find_patterns,
    bool enable_deferred_deletes) : Store(), store(store),
    max_update_metadatas_per_second(max_update_metadatas_per_second),
    max_write_batches_per_second(max_write_batches_per_second),
    disable_rate_limit_for_queue_length(disable_rate_limit_for_queue_length),
    merge_find_patterns(merge_find_patterns),
    enable_deferred_deletes(enable_deferred_deletes), should_exit(false),
    queued_update_metadatas(0), queued_writes(0), queued_datapoints(0),
    update_metadata_rate_limiter(this->max_update_metadatas_per_second),
    write_batch_rate_limiter(this->max_write_batches_per_second),
    batch_size(batch_size),
    delete_thread(&WriteBufferStore::delete_thread_routine, this) {
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
  this->delete_thread.join();
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

bool WriteBufferStore::get_merge_find_patterns() const {
  return this->merge_find_patterns;
}

void WriteBufferStore::set_merge_find_patterns(bool new_value) {
  this->merge_find_patterns = new_value;
}

bool WriteBufferStore::get_enable_deferred_deletes() const {
  return this->enable_deferred_deletes;
}

void WriteBufferStore::set_enable_deferred_deletes(bool new_value) {
  this->enable_deferred_deletes = new_value;
}



shared_ptr<Store> WriteBufferStore::get_substore() const {
  return this->store;
}

void WriteBufferStore::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>>& autocreate_rules) {
  this->Store::set_autocreate_rules(autocreate_rules);
  this->store->set_autocreate_rules(autocreate_rules);
}



shared_ptr<Store::UpdateMetadataTask> WriteBufferStore::update_metadata(
    StoreTaskManager* m, shared_ptr<const UpdateMetadataArguments> args,
    BaseFunctionProfiler* profiler) {
  profiler->add_metadata("series_count",
      string_printf("%zu", args->metadata.size()));

  if (args->skip_buffering) {
    profiler->add_metadata("skip_buffering", "true");
    return this->store->update_metadata(m, args, profiler);
  }

  unordered_map<string, Error> ret;

  for (const auto& it : args->metadata) {
    const auto& key_name = it.first;
    const auto& metadata = it.second;

    try {
      // TODO: we can probably reduce the lock scope here by a lot
      rw_guard g(this->queue_lock, true);

      auto emplace_ret = this->queue.emplace(piecewise_construct,
          forward_as_tuple(key_name),
          forward_as_tuple(metadata, args->create_new, args->behavior));

      if (emplace_ret.second) {
        this->queued_update_metadatas++;

      } else {
        if (args->behavior == UpdateMetadataBehavior::Ignore) {
          ret.emplace(key_name, make_ignored());
          continue;
        }

        auto& command = emplace_ret.first->second;

        if (!command.create_new && args->create_new) {
          command.create_new = true;
        }

        if (args->behavior == UpdateMetadataBehavior::Recreate) {
          command.update_behavior = UpdateMetadataBehavior::Recreate;
          command.data.clear();

        } else if ((args->behavior == UpdateMetadataBehavior::Update) &&
            (command.update_behavior != UpdateMetadataBehavior::Recreate)) {
          command.update_behavior = UpdateMetadataBehavior::Update;
        }

        command.metadata = metadata;
      }
      ret.emplace(key_name, make_success());

    } catch (const exception& e) {
      ret.emplace(key_name, make_error(e.what()));
    }
  }

  return shared_ptr<UpdateMetadataTask>(new UpdateMetadataTask(move(ret)));
}



class WriteBufferStore::WriteBufferStoreDeleteSeriesTask : public Store::DeleteSeriesTask {
public:
  virtual ~WriteBufferStoreDeleteSeriesTask() = default;
  WriteBufferStoreDeleteSeriesTask(StoreTaskManager* m, WriteBufferStore* s,
      shared_ptr<const DeleteSeriesArguments> args,
      BaseFunctionProfiler* profiler) : DeleteSeriesTask(m, args, profiler),
      store(s), num_deleted_from_queue(0) {
    this->profiler->add_metadata("pattern_count",
        string_printf("%zu", args->patterns.size()));

    // remove any pending writes/creates first
    {
      rw_guard g(this->store->queue_lock, true);
      for (const auto& key_name : this->args->patterns) {
        auto& res = this->return_value[key_name];

        // we move the items out of the map so the destructors will be called
        // outside of the lock context (specifically, they'll be called when
        // this task is destroyed)

        // optimization: the queue is ordered, and usually we don't have a pattern
        // at the root level, so we only need to scan a portion of the queue for
        // items to delete. this can be done by finding the first pattern token
        // and scanning only the range of the queue that matches the string before
        // that token.
        size_t pattern_start_offset = key_name.find_first_of("[{*");
        if (pattern_start_offset != string::npos) {
          string prefix = key_name.substr(0, pattern_start_offset);
          auto end_it = this->store->queue.upper_bound(key_name);
          for (auto it = this->store->queue.lower_bound(key_name); it != end_it;) {
            if (!Store::name_matches_pattern(it->first, key_name)) {
              it++;
              continue;
            }

            this->items_to_delete.emplace_back(move(it->second));
            it = this->store->queue.erase(it);
            res.buffer_series_deleted++;
          }

        } else {
          auto it = this->store->queue.find(key_name);
          if (it == this->store->queue.end()) {
            continue;
          }
          this->items_to_delete.emplace_back(move(it->second));
          this->store->queue.erase(it);
          res.buffer_series_deleted++;
        }
      }
    }

    this->profiler->add_metadata("num_deleted_from_queue",
        string_printf("%zu", num_deleted_from_queue));
    this->profiler->checkpoint("write_buffer_delete");

    // if the deletes are deferred, just stick them in the queue and return
    if (this->args->deferred && this->store->enable_deferred_deletes) {
      {
        rw_guard g(this->store->delete_queue_lock, true);
        for (const string& pattern : this->args->patterns) {
          this->store->delete_queue.emplace_back(pattern);
        }
      }
      this->profiler->checkpoint("enqueue_delete");

      this->on_delete_complete();

    } else {
      // issue the delete to the underlying store
      this->delete_task = this->store->store->delete_series(this->manager,
          this->args, profiler);
      this->delegate(this->delete_task, bind(
          &WriteBufferStoreDeleteSeriesTask::on_delete_complete, this));
    }
  }

  void on_delete_complete() {
    // note: in case of deferred deletes, this method is called with a null
    // delete task
    if (this->delete_task.get()) {
      Store::combine_delete_results(this->return_value, move(this->delete_task->value()));
    }

    // count the items that are about to be deleted
    size_t num_update_metadatas_deleted = 0;
    size_t num_writes_deleted = 0;
    size_t num_datapoints_deleted = 0;
    for (const auto& it : items_to_delete) {
      if (it.has_update_metadata()) {
        num_update_metadatas_deleted++;
      }
      size_t num_datapoints = it.data.size();
      num_datapoints_deleted += num_datapoints;
      if (num_datapoints) {
        num_writes_deleted++;
      }
    }
    this->store->queued_update_metadatas -= num_update_metadatas_deleted;
    this->store->queued_writes -= num_writes_deleted;
    this->store->queued_datapoints -= num_datapoints_deleted;
    profiler->checkpoint("write_buffer_merge");

    this->set_complete();
  }

private:
  WriteBufferStore* store;
  vector<QueueItem> items_to_delete;
  size_t num_deleted_from_queue;
  shared_ptr<DeleteSeriesTask> delete_task;
};

shared_ptr<Store::DeleteSeriesTask> WriteBufferStore::delete_series(
    StoreTaskManager* m, shared_ptr<const DeleteSeriesArguments> args,
    BaseFunctionProfiler* profiler) {
  return shared_ptr<DeleteSeriesTask>(new WriteBufferStoreDeleteSeriesTask(
      m, this, args, profiler));
}



class WriteBufferStore::WriteBufferStoreRenameSeriesTask : public Store::RenameSeriesTask {
public:
  virtual ~WriteBufferStoreRenameSeriesTask() = default;
  WriteBufferStoreRenameSeriesTask(StoreTaskManager* m, WriteBufferStore* s,
      shared_ptr<const RenameSeriesArguments> args,
      BaseFunctionProfiler* profiler) : RenameSeriesTask(m, args), store(s) {
    profiler->add_metadata("rename_count",
        string_printf("%zu", this->args->renames.size()));

    // if there are queue items, temporarily remove them from the queue - this is
    // necessary to make sure they don't get written during the rename
    {
      rw_guard g(this->store->queue_lock, true);
      for (const auto& rename_it : this->args->renames) {
        auto queue_it = this->store->queue.find(rename_it.first);
        if (queue_it == this->store->queue.end()) {
          continue;
        }
        items_to_merge.emplace(rename_it.second, move(queue_it->second));
        this->store->queue.erase(queue_it);
      }
    }

    profiler->add_metadata("num_renamed_in_queue",
        string_printf("%zu", items_to_merge.size()));
    profiler->checkpoint("write_buffer_remove");

    // issue the rename to the underlying store
    this->rename_task = this->store->store->rename_series(this->manager,
        this->args, profiler);
    this->delegate(this->rename_task, bind(
        &WriteBufferStoreRenameSeriesTask::on_rename_complete, this));
  }

  void on_rename_complete() {
    for (const auto& it : this->rename_task->value()) {
      if (!it.second.description.empty()) {
        // un-rename the queue item so that the writes don't get reassigned,
        // because this rename failed in the substore and reassigning the writes
        // is definitely not expected behavior from the caller's perspective
        auto item_it = this->items_to_merge.find(this->args->renames.at(it.first));
        if (item_it != this->items_to_merge.end()) {
          this->items_to_merge.emplace(it.first, move(item_it->second));
          this->items_to_merge.erase(item_it);
        }
      }
      this->return_value.emplace(it.first, move(it.second));
    }

    // merge the renamed queue items back in
    {
      rw_guard g(this->store->queue_lock, true);
      this->store->merge_earlier_queue_items_locked(move(this->items_to_merge));
    }

    this->set_complete();
  }

private:
  WriteBufferStore* store;
  unordered_map<string, QueueItem> items_to_merge;
  shared_ptr<RenameSeriesTask> rename_task;
};

shared_ptr<Store::RenameSeriesTask> WriteBufferStore::rename_series(
    StoreTaskManager* m, shared_ptr<const RenameSeriesArguments> args,
    BaseFunctionProfiler* profiler) {
  return shared_ptr<RenameSeriesTask>(new WriteBufferStoreRenameSeriesTask(m,
      this, args, profiler));
}



class WriteBufferStore::WriteBufferStoreReadTask : public Store::ReadTask {
public:
  virtual ~WriteBufferStoreReadTask() = default;
  WriteBufferStoreReadTask(StoreTaskManager* m, WriteBufferStore* s,
      shared_ptr<const ReadArguments> args, BaseFunctionProfiler* profiler) :
      ReadTask(m, args, profiler), store(s) {
    // TODO: merge with writes in progress somehow, so data in the process of
    // being written isn't temporarily invisible. this will be nontrivial since
    // we pop things from the queue in the write threads - there could be data
    // on the write threads' stacks that will be invisible here. potential
    // solutions:
    // 1. have a vector of maps of in-progress writes for each thread and look
    //    through that too
    // 2. replace the queue with a vector of writes for each thread and
    //    distribute writes via ConsistentHashRing (will need a special case for
    //    num_threads=0)
    // 3. don't pop queue items in write threads and instead mark them as in
    //    progress somehow so other threads won't claim them

    this->read_task = this->store->store->read(this->manager, this->args, this->profiler);
    this->delegate(this->read_task, bind(&WriteBufferStoreReadTask::on_read_complete, this));
  }

  void on_read_complete() {
    this->profiler->checkpoint("write_buffer_substore_read");

    this->return_value = move(this->read_task->value());
    for (auto& it : this->return_value) { // (pattern, key_to_result)
      for (auto& it2 : it.second) { // (key, result)
        const auto& key_name = it2.first;
        auto& result = it2.second;

        map<uint32_t, double> data_to_insert;
        try {
          rw_guard g(this->store->queue_lock, false);
          auto& item = this->store->queue.at(key_name);

          bool series_exists_in_substore = (result.step != 0);
          bool item_can_create_series = !item.metadata.archive_args.empty();
          bool item_will_truncate_series = (item.update_behavior == UpdateMetadataBehavior::Recreate);

          // if this item will truncate the series, then behave as if the
          // substore read returned nothing, and apply the writes from the queue
          // item only
          // TODO: we can skip the substore read in this case but I'm lazy
          if (item_can_create_series &&
              (item_will_truncate_series ||
               (item.create_new && !series_exists_in_substore))) {
            // if there's data, reduce resolution to that of the first archive
            result.data.clear();
            if (item.has_data()) {
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
              result.start_time = this->args->start_time + result.step - (this->args->start_time % result.step);
              result.end_time = this->args->end_time + result.step - (this->args->end_time % result.step);
            }
            continue;
          }

          auto dp_it = item.data.lower_bound(this->args->start_time);
          if (dp_it == item.data.end()) {
            continue;
          }
          auto dp_end_it = item.data.upper_bound(this->args->end_time);
          if (dp_end_it == dp_it) {
            continue;
          }

          // note: we round the timestamps to the right interval based on the
          // step returned by the read result, disregarding any pending
          // update_metadata commands. if there's no read result (step == 0)
          // then we get the step from the first archive in the metadata (if
          // this item can create the series), then from the autocreate
          // metadata. if none of these work, return an error
          if (!series_exists_in_substore) {
            if (item_can_create_series && item.create_new) {
              result.step = item.metadata.archive_args[0].precision;
            }
            if (!result.step) {
              auto metadata = this->store->get_autocreate_metadata_for_key(it2.first);
              if (!metadata.archive_args.empty()) {
                result.step = metadata.archive_args[0].precision;
              }
            }
          }
          for (; dp_it != dp_end_it; dp_it++) {
            uint32_t ts = (dp_it->first / result.step) * result.step;
            data_to_insert[ts] = dp_it->second;
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

    this->set_complete();
  }

private:
  WriteBufferStore* store;
  shared_ptr<ReadTask> read_task;
};

shared_ptr<Store::ReadTask> WriteBufferStore::read(StoreTaskManager* m,
    shared_ptr<const ReadArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<ReadTask>(new WriteBufferStoreReadTask(m, this, args, profiler));
}



shared_ptr<Store::ReadAllTask> WriteBufferStore::read_all(StoreTaskManager* m,
    shared_ptr<const ReadAllArguments> args, BaseFunctionProfiler* profiler) {
  // TODO: we really should merge the result with anything in the write buffer.
  // but I'm lazy and this is primarily used for verification+repair, during
  // which there should not be writes to series that exist on the wrong node
  return this->store->read_all(m, args, profiler);
}



shared_ptr<Store::WriteTask> WriteBufferStore::write(StoreTaskManager* m,
    shared_ptr<const WriteArguments> args, BaseFunctionProfiler* profiler) {
  if (args->skip_buffering) {
    profiler->add_metadata("skip_buffering", "true");
    return this->store->write(m, args, profiler);
  }

  unordered_map<string, Error> ret;
  for (const auto& it : args->data) {
    ret.emplace(it.first, make_success());
  }

  // construct a copy of data so we don't do this inside the lock context
  unordered_map<string, Series> mutable_data = args->data;
  {
    rw_guard g(this->queue_lock, true);
    for (auto& it : mutable_data) {
      auto emplace_ret = this->queue.emplace(piecewise_construct,
          forward_as_tuple(it.first), forward_as_tuple(move(it.second)));
      if (emplace_ret.second) {
        this->queued_writes++;
        this->queued_datapoints += emplace_ret.first->second.data.size();
      } else {
        // there's already a queue item for this key; need to merge the contents
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
  return shared_ptr<WriteTask>(new WriteTask(move(ret)));
}



class WriteBufferStore::WriteBufferStoreFindTask : public Store::FindTask {
public:
  virtual ~WriteBufferStoreFindTask() = default;
  WriteBufferStoreFindTask(StoreTaskManager* m, WriteBufferStore* s,
      shared_ptr<const FindArguments> args, BaseFunctionProfiler* profiler) :
      FindTask(m, args, profiler), store(s) {
    // the comment in read() about data temporarily disappearing applies here
    this->find_task = this->store->store->find(this->manager, this->args, this->profiler);
    this->delegate(this->find_task, bind(&WriteBufferStoreFindTask::on_find_complete, this));
  }

  void on_find_complete() {
    this->return_value = this->find_task->value();
    this->profiler->checkpoint("write_buffer_substore_find");

    // merge the find results with the create queue
    for (auto& it : this->return_value) {
      // if there was an error, don't bother merging for this query
      if (!it.second.error.description.empty()) {
        continue;
      }

      size_t pattern_start_offset = it.first.find_first_of("[{*");
      if (pattern_start_offset != string::npos) {
        // if merging patterns is disabled, don't do anything (this can be slow)
        if (!this->store->merge_find_patterns) {
          continue;
        }

        // this query is a pattern; need to merge with a queue range. but we can
        // make it faster by only skipping the parts of the queue that we know
        // won't match the pattern.
        // TODO: upper_bound can cause us to miss keys that end in multiple \xFF
        // chars. I'm too lazy to fix this; it's bad practice to use
        // nonprintable chars in key names anyway
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
          rw_guard g(this->store->queue_lock, false);
          auto queue_it = this->store->queue.lower_bound(lower_bound_str);
          auto queue_end_it = this->store->queue.upper_bound(upper_bound_str);
          for (; queue_it != queue_end_it; queue_it++) {
            // we should only add the queue item if it has create_new = true. if
            // it doesn't and the series doesn't exist in the substore, then this
            // queued command won't create it.
            // TODO: we should consider autocreate rules here too
            if (!queue_it->second.create_new) {
              continue;
            }

            if (this->store->name_matches_pattern(queue_it->first, it.first)) {
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
                if (this->store->name_matches_pattern(item, it.first)) {
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
            rw_guard g(this->store->queue_lock, false);
            should_insert = this->store->queue.count(it.first);
          }
          if (should_insert) {
            it.second.results.emplace_back(it.first);
          }
        }
      }
    }
    this->profiler->checkpoint("write_buffer_find_merge");
    this->set_complete();
  }

private:
  WriteBufferStore* store;
  unordered_map<string, QueueItem> items_to_merge;
  shared_ptr<FindTask> find_task;
};

shared_ptr<Store::FindTask> WriteBufferStore::find(StoreTaskManager* m,
    shared_ptr<const FindArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<FindTask>(new WriteBufferStoreFindTask(m, this, args, profiler));
}



void WriteBufferStore::flush(StoreTaskManager* m) {
  auto profiler = create_profiler("WriteBufferStore::flush", "flush()", -1);

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
      if (command.has_update_metadata()) {
        shared_ptr<UpdateMetadataArguments> args(new UpdateMetadataArguments());
        args->metadata.emplace(key_name, command.metadata);
        args->behavior = command.update_behavior;
        args->create_new = command.create_new;
        args->skip_buffering = false;
        args->local_only = false;
        this->store->update_metadata(m, args, profiler.get());
      }

      // if data isn't empty, there was a write request
      if (command.has_data()) {
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
    shared_ptr<WriteArguments> args(new WriteArguments());
    args->data = move(write_batch);
    args->skip_buffering = false;
    args->local_only = false;
    this->store->write(m, args, profiler.get());
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
  ret.emplace("merge_find_patterns", this->merge_find_patterns);
  ret.emplace("enable_deferred_deletes", this->enable_deferred_deletes);
  {
    rw_guard g(this->delete_queue_lock, false);
    ret.emplace("delete_queue_length", this->delete_queue.size());
  }
  return ret;
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

bool WriteBufferStore::QueueItem::has_update_metadata() const {
  return !this->metadata.archive_args.empty();
}

bool WriteBufferStore::QueueItem::has_data() const {
  return !this->data.empty();
}

void WriteBufferStore::QueueItem::erase_update_metadata() {
  this->metadata.archive_args.clear();
}



void WriteBufferStore::merge_earlier_queue_items_locked(
    unordered_map<string, QueueItem>&& items) {
  size_t merged_update_metadatas = 0;
  size_t merged_writes = 0;
  size_t merged_datapoints = 0;

  for (auto& merge_it : items) {
    auto& old_item = merge_it.second;
    size_t datapoint_count = old_item.data.size();
    bool is_update_metadata = old_item.has_update_metadata();

    auto emplace_ret = this->queue.emplace(piecewise_construct,
        forward_as_tuple(merge_it.first), forward_as_tuple(move(old_item)));
    if (emplace_ret.second) {
      if (datapoint_count) {
        merged_writes++;
        merged_datapoints += datapoint_count;
      }
      if (is_update_metadata) {
        merged_update_metadatas++;
      }
      continue; // there wasn't an existing queue item
    }

    // there's already a queue item for this key; need to merge the contents.
    // unlike in write(), we merge the data under the existing data (that is,
    // we drop the point to be merged if it would overwrite an existing point)
    // because the merging data was received earlier than the existing data
    auto& new_item = emplace_ret.first->second;

    bool move_data = true;
    bool move_update_metadata = false;

    if (old_item.has_update_metadata() && new_item.has_update_metadata()) {
      // merging update_metadata is kind of hard to understand. here's a table of
      // what should happen:
      //    new        old      result
      //  Ignore     Ignore     old takes precedence
      //  Ignore     Update     old takes precedence
      //  Ignore    Recreate    old takes precedence
      //  Update     Ignore     new takes precedence
      //  Update     Update     new takes precedence
      //  Update    Recreate    new takes precedence, but behavior is Recreate
      // Recreate    Ignore     new takes precedence, and data from old is ignored
      // Recreate    Update     new takes precedence, and data from old is ignored
      // Recreate   Recreate    new takes precedence, and data from old is ignored
      //
      // so in summary, we use other's update_metadata command if this'
      // update_metadata command has behavior Ignore, and otherwise don't use it
      // at all. but if other's behavior was Recreate, we need to make sure that
      // that happens, so we add that special case here (#2).

      if (new_item.update_behavior == UpdateMetadataBehavior::Ignore) {
        move_update_metadata = true;

      } else if ((new_item.update_behavior == UpdateMetadataBehavior::Update) &&
          (old_item.update_behavior == UpdateMetadataBehavior::Recreate)) {
        new_item.update_behavior = UpdateMetadataBehavior::Recreate;

      } else if (new_item.update_behavior == UpdateMetadataBehavior::Recreate) {
        move_data = false;
      }

    } else if (old_item.has_update_metadata()) {
      move_update_metadata = true;
      merged_update_metadatas++;

    } else if (new_item.has_update_metadata() &&
        (new_item.update_behavior == UpdateMetadataBehavior::Recreate)) {
      move_data = false;
    }

    if (move_update_metadata) {
      new_item.metadata = move(old_item.metadata);
      new_item.update_behavior = old_item.update_behavior;
      new_item.create_new = old_item.create_new;
    }

    if (move_data) {
      if (new_item.has_data() && old_item.has_data()) {
        // put the other item's data into this item, but don't overwrite any
        // existing datapoints
        for (const auto& old_dp_it : old_item.data) {
          merged_datapoints += new_item.data.emplace(old_dp_it.first, old_dp_it.second).second;
        }

      } else if (old_item.has_data()) {
        new_item.data = move(old_item.data);
        merged_datapoints += new_item.data.size();
        merged_writes++;
      }
    }
  }

  this->queued_update_metadatas += merged_update_metadatas;
  this->queued_writes += merged_writes;
  this->queued_datapoints += merged_datapoints;
}



WriteBufferStore::WriteThread::WriteThread() : t(), last_queue_restart(0),
    queue_sweep_time(0) { }

void WriteBufferStore::write_thread_routine(size_t thread_index) {
  // note: we use blocking calls (manager = NULL) in this function to limit load

  string queue_position;
  string thread_name = string_printf("WriteBufferStore::write_thread_routine (thread_id=%zu)",
      this_thread::get_id());

  auto& wt = this->write_threads[thread_index];
  wt->last_queue_restart = now();
  wt->queue_sweep_time = -1;

  while (!this->should_exit) {
    bool commands_were_fetched = false;
    {
      ProfilerGuard pg(create_internal_profiler(thread_name,
          "WriteBufferStore::write_thread_routine"));

      bool enable_rate_limits = true;

      // pick out a batch of creates from the create queue
      unordered_map<string, QueueItem> commands;
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
      pg.profiler->checkpoint("get_queue_items");
      commands_were_fetched = !commands.empty();

      // execute the creates in order, and batch the writes
      size_t write_batch_datapoints = 0;
      shared_ptr<WriteArguments> write_args(new WriteArguments());
      write_args->skip_buffering = false;
      write_args->local_only = false;
      auto& write_batch = write_args->data;
      for (auto& it : commands) {
        const auto& key_name = it.first;
        auto& command = it.second;

        // if archive_args isn't empty, there was a create request
        if (command.has_update_metadata()) {
          if (enable_rate_limits && this->max_update_metadatas_per_second) {
            uint64_t delay = this->update_metadata_rate_limiter.delay_until_next_action();
            if (delay) {
              usleep(delay);
              pg.profiler->checkpoint("update_metadata_rate_limit_sleep");
            }
          }

          try {
            shared_ptr<UpdateMetadataArguments> args(new UpdateMetadataArguments());
            args->metadata.emplace(key_name, command.metadata);
            args->behavior = command.update_behavior;
            args->create_new = command.create_new;
            args->skip_buffering = false;
            args->local_only = false;

            StoreTaskManager m;
            auto task = this->store->update_metadata(&m, args, pg.profiler.get());
            m.run(task);
            auto& errors = task->value();

            const auto& error = errors.at(key_name);
            if (!error.description.empty()) {
              string error_str = string_for_error(error);
              log(WARNING, "[WriteBufferStore] update_metadata error on key %s: %s",
                  it.first.c_str(), error_str.c_str());
            }

          } catch (const exception& e) {
            log(WARNING, "[WriteBufferStore] update_metadata failed on key %s: %s",
                key_name.c_str(), e.what());
          }

          // erase the update_metadata part of the command in case the write
          // also fails (and the command is merged back into the queue)
          command.erase_update_metadata();
          this->queued_update_metadatas--;
        }

        // if data isn't empty, there was a write request
        if (command.has_data()) {
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
      pg.profiler->checkpoint("execute_update_metadatas");

      if (!write_batch.empty()) {
        if (enable_rate_limits && this->max_write_batches_per_second) {
          uint64_t delay = this->write_batch_rate_limiter.delay_until_next_action();
          if (delay) {
            usleep(delay);
              pg.profiler->checkpoint("write_rate_limit_sleep");
          }
        }

        this->queued_datapoints -= write_batch_datapoints;
        try {
          // note: write_batch is a reference to write_args->data, so it's
          // already populated
          StoreTaskManager m;
          auto task = this->store->write(&m, write_args, pg.profiler.get());
          m.run(task);

          for (const auto& it : task->value()) {
            if (!it.second.description.empty()) {
              string error_str = string_for_error(it.second);
              log(WARNING, "[WriteBufferStore] write error on key %s: %s",
                  it.first.c_str(), error_str.c_str());
              if (!it.second.recoverable || it.second.ignored) {
                commands.erase(it.first);
              }
            } else {
              commands.erase(it.first);
            }
          }
        } catch (const exception& e) {
          log(WARNING, "[WriteBufferStore] write failed for batch of %zu keys (%s)",
              write_batch.size(), e.what());
        }
        pg.profiler->checkpoint("execute_writes");
        pg.profiler->add_metadata("write_series", to_string(write_batch.size()));
        pg.profiler->add_metadata("write_datapoints", to_string(write_batch_datapoints));
      }

      // return the failed but recoverable items to the queue
      if (!commands.empty()) {
        rw_guard g(this->queue_lock, true);
        this->merge_earlier_queue_items_locked(move(commands));
        pg.profiler->checkpoint("return_recoverable_failed_items");
      }
    } // this ends the profiler scope

    // if there was nothing to do, wait a second
    if (!commands_were_fetched) {
      sleep(1);
    }
  }
}

void WriteBufferStore::delete_thread_routine() {
  string thread_name = string_printf("WriteBufferStore::delete_thread_routine (thread_id=%zu)",
      this_thread::get_id());

  while (!this->should_exit) {
    string query;
    {
      rw_guard g(this->delete_queue_lock, true);
      if (!this->delete_queue.empty()) {
        query = move(this->delete_queue.front());
        this->delete_queue.pop_front();
      }
    }

    if (query.empty()) {
      usleep(1000000);
      continue;
    }

    shared_ptr<DeleteSeriesArguments> args(new DeleteSeriesArguments());
    args->patterns.emplace_back(query);
    args->deferred = false;
    args->local_only = false;

    ProfilerGuard pg(create_internal_profiler(thread_name,
        string_for_delete_series(args->patterns, false, false)));
    StoreTaskManager m;
    auto task = this->delete_series(&m, args, pg.profiler.get());
    m.run(task);
    // TODO: report errors here somehow
  }
}
