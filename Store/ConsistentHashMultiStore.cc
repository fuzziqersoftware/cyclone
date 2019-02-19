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
    const unordered_map<string, StoreInfo>& substores, int64_t precision) :
    MultiStore(substores), precision(precision), should_exit(false),
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



class ConsistentHashMultiStore::ConsistentHashMultiStoreUpdateMetadataTask : public Store::UpdateMetadataTask {
public:
  virtual ~ConsistentHashMultiStoreUpdateMetadataTask() = default;
  ConsistentHashMultiStoreUpdateMetadataTask(StoreTaskManager* m, ConsistentHashMultiStore* s,
      shared_ptr<const UpdateMetadataArguments> args,
      BaseFunctionProfiler* profiler) : UpdateMetadataTask(m, args, profiler),
      store(s) {
    unordered_map<string, unordered_map<string, SeriesMetadata>> partitioned_data;

    for (const auto& it : args->metadata) {
      uint64_t store_id = this->store->ring->host_id_for_key(it.first);
      const string& store_name = this->store->ring->host_for_id(store_id).name;
      partitioned_data[store_name].emplace(it.first, it.second);
    }
    profiler->checkpoint("partition_series");

    for (const auto& store_name : this->store->ordered_store_names) {
      auto partitioned_data_it = partitioned_data.find(store_name);
      if (partitioned_data_it == partitioned_data.end()) {
        continue;
      }
      shared_ptr<UpdateMetadataArguments> sub_args(new UpdateMetadataArguments());
      sub_args->metadata = move(partitioned_data_it->second);
      sub_args->behavior = args->behavior;
      sub_args->create_new = args->create_new;
      sub_args->skip_buffering = args->skip_buffering;
      sub_args->local_only = args->local_only;
      this->tasks.emplace_back(this->store->all_stores[store_name]->update_metadata(
          m, sub_args, profiler));
      this->delegate(this->tasks.back());
    }
    this->suspend(bind(&ConsistentHashMultiStoreUpdateMetadataTask::on_all_tasks_complete, this));
  }

  void on_all_tasks_complete() {
    for (const auto& it : this->tasks) {
      for (const auto& result_it : it->value()) {
        this->return_value.emplace(move(result_it.first), move(result_it.second));
      }
    }
    this->set_complete();
  }

private:
  ConsistentHashMultiStore* store;
  vector<shared_ptr<UpdateMetadataTask>> tasks;
};

shared_ptr<Store::UpdateMetadataTask> ConsistentHashMultiStore::update_metadata(StoreTaskManager* m,
    shared_ptr<const UpdateMetadataArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<UpdateMetadataTask>(new ConsistentHashMultiStoreUpdateMetadataTask(m, this, args, profiler));
}



class ConsistentHashMultiStore::ConsistentHashMultiStoreDeleteSeriesTask : public Store::DeleteSeriesTask {
public:
  virtual ~ConsistentHashMultiStoreDeleteSeriesTask() = default;
  ConsistentHashMultiStoreDeleteSeriesTask(StoreTaskManager* m, ConsistentHashMultiStore* s,
      shared_ptr<const DeleteSeriesArguments> args,
      BaseFunctionProfiler* profiler) : DeleteSeriesTask(m, args, profiler),
      store(s) {
    unordered_map<string, vector<string>> partitioned_data;
    for (const string& pattern : this->args->patterns) {
      // send patterns to all stores, but we can send non-patterns to only one
      // substore to reduce work
      if (Store::token_is_pattern(pattern)) {
        for (const string& store_name : this->store->ordered_store_names) {
          partitioned_data[store_name].push_back(pattern);
        }
      } else {
        uint64_t store_id = this->store->ring->host_id_for_key(pattern);
        const string& store_name = this->store->ring->host_for_id(store_id).name;
        partitioned_data[store_name].push_back(pattern);
      }
    }
    profiler->checkpoint("partition_series");

    for (const auto& store_name : this->store->ordered_store_names) {
      auto partitioned_data_it = partitioned_data.find(store_name);
      if (partitioned_data_it == partitioned_data.end()) {
        continue;
      }
      shared_ptr<DeleteSeriesArguments> sub_args(new DeleteSeriesArguments());
      sub_args->patterns = move(partitioned_data_it->second);
      sub_args->deferred = args->deferred;
      sub_args->local_only = args->local_only;
      this->tasks.emplace_back(this->store->all_stores[store_name]->delete_series(
          m, sub_args, profiler));
      this->delegate(this->tasks.back());
    }
    this->suspend(bind(&ConsistentHashMultiStoreDeleteSeriesTask::on_all_tasks_complete, this));
  }

  void on_all_tasks_complete() {
    for (const auto& it : this->tasks) {
      Store::combine_delete_results(this->return_value, move(it->value()));
    }
    this->set_complete();
  }

private:
  ConsistentHashMultiStore* store;
  vector<shared_ptr<DeleteSeriesTask>> tasks;
};

shared_ptr<Store::DeleteSeriesTask> ConsistentHashMultiStore::delete_series(
    StoreTaskManager* m, shared_ptr<const DeleteSeriesArguments> args,
    BaseFunctionProfiler* profiler) {

  // if a verify is in progress, we need to forward all deletes everywhere
  // because there could be keys on wrong backends. so we'll use the parent
  // class' method to do this
  if (this->read_from_all ||
      (this->verify_progress.in_progress() && this->verify_progress.repair)) {
    profiler->add_metadata("read_from_all", "true");
    return this->MultiStore::delete_series(m, args, profiler);
  }
  return shared_ptr<DeleteSeriesTask>(new ConsistentHashMultiStoreDeleteSeriesTask(m, this, args, profiler));
}



class ConsistentHashMultiStore::ConsistentHashMultiStoreRenameSeriesTask : public Store::RenameSeriesTask {
public:
  virtual ~ConsistentHashMultiStoreRenameSeriesTask() = default;
  ConsistentHashMultiStoreRenameSeriesTask(StoreTaskManager* m, ConsistentHashMultiStore* s,
      shared_ptr<const RenameSeriesArguments> args,
      BaseFunctionProfiler* profiler) : RenameSeriesTask(m, args, profiler),
      store(s) {

    // if a verify+repair is in progress or read_from_all is on, then renames
    // can't be forwarded to substores as an optimization; we have to emulate
    // them using read_all and write instead
    bool can_forward = !this->store->read_from_all &&
        (!this->store->verify_progress.in_progress() ||
         !this->store->verify_progress.repair);

    unordered_map<string, unordered_map<string, string>> renames_to_forward;
    unordered_map<string, string> renames_to_emulate;
    if (can_forward) {
      for (const auto& it : this->args->renames) {
        uint8_t from_store_id = this->store->ring->host_id_for_key(it.first);
        uint8_t to_store_id = this->store->ring->host_id_for_key(it.second);
        if (from_store_id == to_store_id) {
          const string& store_name = this->store->ring->host_for_id(to_store_id).name;
          renames_to_forward[store_name].emplace(it.first, it.second);
        } else {
          renames_to_emulate.emplace(it.first, it.second);
        }
      }
      profiler->add_metadata("can_forward", "true");
    } else {
      // TODO: don't copy args->renames here (we don't need to modify it later)
      renames_to_emulate = this->args->renames;
      profiler->add_metadata("can_forward", "false");
    }
    profiler->checkpoint("partition_series");

    for (const auto& store_name : this->store->ordered_store_names) {
      auto renames_to_forward_it = renames_to_forward.find(store_name);
      if (renames_to_forward_it == renames_to_forward.end()) {
        continue;
      }
      shared_ptr<RenameSeriesArguments> sub_args(new RenameSeriesArguments());
      sub_args->renames = move(renames_to_forward_it->second);
      sub_args->merge = args->merge;
      sub_args->local_only = args->local_only;
      this->direct_rename_tasks.emplace_back(this->store->all_stores[store_name]->rename_series(
          m, sub_args, profiler));
      this->delegate(this->direct_rename_tasks.back());
    }

    // TODO: order these in some sensible manner (the network-dependent tasks
    // should execute first)
    for (const auto& it : renames_to_emulate) {
      const string& from_key_name = it.first;
      const string& to_key_name = it.second;
      uint8_t from_store_id = this->store->ring->host_id_for_key(from_key_name);
      uint8_t to_store_id = this->store->ring->host_id_for_key(to_key_name);
      auto& from_store = this->store->all_stores[this->store->ring->host_for_id(from_store_id).name];
      auto& to_store = this->store->all_stores[this->store->ring->host_for_id(to_store_id).name];

      auto emplace_ret = this->emulate_rename_tasks.emplace(from_key_name, Store::emulate_rename_series(
          m, from_store.get(), from_key_name, to_store.get(), to_key_name,
          this->args->merge, profiler));
      this->delegate(emplace_ret.first->second);
    }
    this->suspend(bind(&ConsistentHashMultiStoreRenameSeriesTask::on_all_tasks_complete, this));
  }

  void on_all_tasks_complete() {
    // direct tasks
    for (auto& task : this->direct_rename_tasks) {
      Store::combine_simple_results(this->return_value, move(task->value()));
    }
    for (auto& it : this->emulate_rename_tasks) {
      this->return_value.emplace(it.first, move(it.second->value()));
    }
    this->set_complete();
  }

private:
  ConsistentHashMultiStore* store;
  vector<shared_ptr<RenameSeriesTask>> direct_rename_tasks;
  unordered_map<string, shared_ptr<EmulateRenameSeriesTask>> emulate_rename_tasks;
};

shared_ptr<Store::RenameSeriesTask> ConsistentHashMultiStore::rename_series(
    StoreTaskManager* m, shared_ptr<const RenameSeriesArguments> args,
    BaseFunctionProfiler* profiler) {
  return shared_ptr<RenameSeriesTask>(new ConsistentHashMultiStoreRenameSeriesTask(
      m, this, args, profiler));
}



class ConsistentHashMultiStore::ConsistentHashMultiStoreReadTask : public Store::ReadTask {
public:
  virtual ~ConsistentHashMultiStoreReadTask() = default;
  ConsistentHashMultiStoreReadTask(StoreTaskManager* m, ConsistentHashMultiStore* s,
      shared_ptr<const ReadArguments> args, BaseFunctionProfiler* profiler) :
      ReadTask(m, args, profiler), store(s) {

    unordered_map<string, vector<string>> partitioned_data;
    for (const auto& key_name : this->args->key_names) {
      if (Store::token_is_pattern(key_name)) {
        for (const string& store_name : this->store->ordered_store_names) {
          partitioned_data[store_name].push_back(key_name);
        }
      } else {
        size_t store_id = this->store->ring->host_id_for_key(key_name);
        const string& store_name = this->store->ring->host_for_id(store_id).name;
        partitioned_data[store_name].push_back(key_name);
      }
    }
    profiler->checkpoint("partition_series");

    for (const auto& store_name : this->store->ordered_store_names) {
      auto partitioned_data_it = partitioned_data.find(store_name);
      if (partitioned_data_it == partitioned_data.end()) {
        continue;
      }
      shared_ptr<ReadArguments> sub_args(new ReadArguments());
      sub_args->key_names = move(partitioned_data_it->second);
      sub_args->start_time = args->start_time;
      sub_args->end_time = args->end_time;
      sub_args->local_only = args->local_only;
      this->tasks.emplace_back(this->store->all_stores[store_name]->read(m, sub_args, profiler));
      this->delegate(this->tasks.back());
    }
    this->suspend(bind(&ConsistentHashMultiStoreReadTask::on_all_tasks_complete, this));
  }

  void on_all_tasks_complete() {
    for (const auto& it : this->tasks) {
      Store::combine_read_results(this->return_value, move(it->value()));
    }
    this->set_complete();
  }

private:
  ConsistentHashMultiStore* store;
  vector<shared_ptr<ReadTask>> tasks;
};

shared_ptr<Store::ReadTask> ConsistentHashMultiStore::read(StoreTaskManager* m,
    shared_ptr<const ReadArguments> args, BaseFunctionProfiler* profiler) {
  // see comment in delete_series about this
  if (this->read_from_all ||
      (this->verify_progress.in_progress() && this->verify_progress.repair)) {
    profiler->add_metadata("read_from_all", "true");
    return this->MultiStore::read(m, args, profiler);
  }

  return shared_ptr<ReadTask>(new ConsistentHashMultiStoreReadTask(m, this, args, profiler));
}



shared_ptr<Store::ReadAllTask> ConsistentHashMultiStore::read_all(StoreTaskManager* m,
    shared_ptr<const ReadAllArguments> args, BaseFunctionProfiler* profiler) {
  // see comment in delete_series about this
  if (this->read_from_all || (this->verify_progress.in_progress() && this->verify_progress.repair)) {
    profiler->add_metadata("read_from_all", "true");
    return this->MultiStore::read_all(m, args, profiler);
  }

  size_t store_id = this->ring->host_id_for_key(args->key_name);
  const string& store_name = this->ring->host_for_id(store_id).name;
  return this->all_stores[store_name]->read_all(m, args, profiler);
}



class ConsistentHashMultiStore::ConsistentHashMultiStoreWriteTask : public Store::WriteTask {
public:
  virtual ~ConsistentHashMultiStoreWriteTask() = default;
  ConsistentHashMultiStoreWriteTask(StoreTaskManager* m, ConsistentHashMultiStore* s,
      shared_ptr<const WriteArguments> args, BaseFunctionProfiler* profiler) :
      WriteTask(m, args, profiler), store(s) {

    unordered_map<string, unordered_map<string, Series>> partitioned_data;
    for (const auto& it : args->data) {
      uint8_t store_id = this->store->ring->host_id_for_key(it.first);
      const string& store_name = this->store->ring->host_for_id(store_id).name;
      partitioned_data[store_name].emplace(it.first, it.second);
    }
    profiler->checkpoint("partition_series");

    for (const auto& store_name : this->store->ordered_store_names) {
      auto partitioned_data_it = partitioned_data.find(store_name);
      if (partitioned_data_it == partitioned_data.end()) {
        continue;
      }
      shared_ptr<WriteArguments> sub_args(new WriteArguments());
      sub_args->data = move(partitioned_data_it->second);
      sub_args->skip_buffering = args->skip_buffering;
      sub_args->local_only = args->local_only;
      this->tasks.emplace_back(this->store->all_stores[store_name]->write(m, sub_args, profiler));
      this->delegate(this->tasks.back());
    }
    this->suspend(bind(&ConsistentHashMultiStoreWriteTask::on_all_tasks_complete, this));
  }

  void on_all_tasks_complete() {
    for (auto& task : this->tasks) {
      Store::combine_simple_results(this->return_value, move(task->value()));
    }
    this->set_complete();
  }

private:
  ConsistentHashMultiStore* store;
  vector<shared_ptr<WriteTask>> tasks;
};

shared_ptr<Store::WriteTask> ConsistentHashMultiStore::write(StoreTaskManager* m,
    shared_ptr<const WriteArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<WriteTask>(new ConsistentHashMultiStoreWriteTask(m, this, args, profiler));
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
  for (const auto& it : this->all_stores) {
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

  StoreTaskManager m;

  vector<string> pending_patterns;
  pending_patterns.emplace_back("*");
  while (!this->should_exit && !pending_patterns.empty() &&
         !this->verify_progress.cancelled) {
    string pattern = pending_patterns.back();
    pending_patterns.pop_back();

    string function_name = string_printf("verify(%s)", pattern.c_str());
    ProfilerGuard pg(create_internal_profiler(
        "ConsistentHashMultiStore::verify_thread_routine", function_name.c_str()));

    shared_ptr<FindTask> find_task;
    {
      shared_ptr<FindArguments> args(new FindArguments());
      args->patterns.emplace_back(pattern);
      args->local_only = false;
      find_task = this->find(&m, args, pg.profiler.get());
      m.run(find_task);
      pg.profiler->checkpoint("find");
    }

    auto& find_results = find_task->value();
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
      string error_str = string_for_error(find_result.error);
      log(WARNING, "[ConsistentHashMultiStore] find(%s) returned error during verify: %s",
          pattern.c_str(), error_str.c_str());
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
      auto& responsible_store = this->all_stores.at(responsible_store_name);

      // find out which stores it actually exists on
      // TODO: we should find some way to make the reads happen in parallel
      unordered_map<string, unordered_map<string, ReadResult>> ret;
      for (const auto& store_it : this->all_stores) {
        // skip the store that the key is supposed to be in
        if (store_it.first == responsible_store_name) {
          continue;
        }

        // note: we set local_only = true here because the verify procedure
        // must run on ALL nodes in the cluster, so it makes sense for each
        // node to only be responsible for the series on its local disk
        shared_ptr<ReadAllTask> read_all_task;
        {
          shared_ptr<ReadAllArguments> args(new ReadAllArguments());
          args->key_name = key_name;
          args->local_only = true;
          read_all_task = store_it.second->read_all(&m, args, pg.profiler.get());
          m.run(read_all_task);
        }
        auto& read_all_result = read_all_task->value();
        pg.profiler->checkpoint("read_all");
        if (!read_all_result.error.description.empty()) {
          string error_str = string_for_error(read_all_result.error);
          log(WARNING, "[ConsistentHashMultiStore] key %s could not be read from %s (error: %s)",
              key_name.c_str(), store_it.first.c_str(), error_str.c_str());
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
          {
            shared_ptr<UpdateMetadataArguments> args(new UpdateMetadataArguments());
            args->metadata.emplace(key_name, read_all_result.metadata);
            args->behavior = UpdateMetadataBehavior::Ignore;
            args->create_new = true;
            args->skip_buffering = true;
            args->local_only = false;
            auto update_metadata_task = responsible_store->update_metadata(&m,
                args, pg.profiler.get());
            m.run(update_metadata_task);
            auto& update_metadata_ret = update_metadata_task->value();

            pg.profiler->checkpoint("update_metadata");
            try {
              const Error& error = update_metadata_ret.at(key_name);
              if (!error.description.empty() && !error.ignored) {
                string error_str = string_for_error(error);
                log(WARNING, "[ConsistentHashMultiStore] update_metadata returned error (%s) when moving %s from %s to %s",
                    error_str.c_str(), key_name.c_str(), store_it.first.c_str(),
                    responsible_store_name.c_str());
                this->verify_progress.update_metadata_errors++;
                continue;
              }
            } catch (const out_of_range&) {
              log(WARNING, "[ConsistentHashMultiStore] update_metadata returned no results when moving %s from %s to %s",
                  key_name.c_str(), store_it.first.c_str(), responsible_store_name.c_str());
              this->verify_progress.update_metadata_errors++;
              continue;
            }
          }

          // move step 2: write all the data from the local series into the
          // remote series. note: we do this in a separate scope so the data
          // will be deallocated before calling delete_series, so we don't keep
          // it in memory any longer than we need to
          {
            shared_ptr<WriteArguments> args(new WriteArguments());
            args->data.emplace(key_name, move(read_all_result.data));
            args->skip_buffering = true;
            args->local_only = false;
            auto write_task = responsible_store->write(&m, args, pg.profiler.get());
            m.run(write_task);
            auto& write_ret = write_task->value();
            pg.profiler->checkpoint("write");
            try {
              const Error& error = write_ret.at(key_name);
              if (!error.description.empty()) {
                string error_str = string_for_error(error);
                log(WARNING, "[ConsistentHashMultiStore] write returned error (%s) when moving %s from %s to %s",
                    error_str.c_str(), key_name.c_str(), store_it.first.c_str(),
                    responsible_store_name.c_str());
                this->verify_progress.write_errors++;
                continue;
              }
            } catch (const out_of_range&) {
              log(WARNING, "[ConsistentHashMultiStore] write returned no results when moving %s from %s to %s",
                  key_name.c_str(), store_it.first.c_str(), responsible_store_name.c_str());
              this->verify_progress.write_errors++;
              continue;
            }
          }

          // at this point the data has been successfully moved; reads and
          // writes will now be consistent

          // move step 3: delete the original series. note that we use
          // local_only = true here because we also did so when read_all'ing
          // this series' data
          shared_ptr<DeleteSeriesArguments> args(new DeleteSeriesArguments());
          args->patterns.emplace_back(key_name);
          args->deferred = false;
          args->local_only = true;
          auto delete_task = store_it.second->delete_series(&m, args, pg.profiler.get());
          m.run(delete_task);
          auto& delete_ret = delete_task->value();

          pg.profiler->checkpoint("delete_series");
          auto& res = delete_ret[key_name];
          int64_t num_deleted = res.disk_series_deleted + res.buffer_series_deleted;
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
