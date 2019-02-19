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



MultiStore::StoreInfo::StoreInfo(shared_ptr<Store> store,
    bool requires_network_calls) : store(store),
    requires_network_calls(requires_network_calls) { }



MultiStore::MultiStore(const unordered_map<string, StoreInfo>& stores) :
    Store() {
  // compute ordered_stores, which specifies the order in which stores should be
  // queried. this puts network stores first so we can always parallelize
  // network calls with local disk activity
  vector<string> local_store_names;
  vector<shared_ptr<Store>> local_stores;

  for (const auto& it : stores) {
    if (it.second.requires_network_calls) {
      this->ordered_store_names.emplace_back(it.first);
      this->ordered_stores.emplace_back(it.second.store);
    } else {
      local_store_names.emplace_back(it.first);
      local_stores.emplace_back(it.second.store);
    }
    this->all_stores.emplace(it.first, it.second.store);
  }

  this->ordered_store_names.insert(this->ordered_store_names.end(),
      local_store_names.begin(), local_store_names.end());
  this->ordered_stores.insert(this->ordered_stores.end(),
      local_stores.begin(), local_stores.end());
}

const unordered_map<string, shared_ptr<Store>>& MultiStore::get_substores() const {
  return this->all_stores;
}

void MultiStore::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>>& autocreate_rules) {
  this->Store::set_autocreate_rules(autocreate_rules);
  for (const auto& it : this->all_stores) {
    it.second->set_autocreate_rules(autocreate_rules);
  }
}



class MultiStore::MultiStoreUpdateMetadataTask : public Store::UpdateMetadataTask {
public:
  virtual ~MultiStoreUpdateMetadataTask() = default;
  MultiStoreUpdateMetadataTask(StoreTaskManager* m, MultiStore* s,
      shared_ptr<const UpdateMetadataArguments> args,
      BaseFunctionProfiler* profiler) : UpdateMetadataTask(m, args, profiler),
      store(s) {
    for (const auto& it : this->store->ordered_stores) {
      this->tasks.emplace_back(it->update_metadata(m, args, profiler));
      this->delegate(this->tasks.back());
    }
    this->suspend(bind(&MultiStoreUpdateMetadataTask::on_all_tasks_complete, this));
  }

  void on_all_tasks_complete() {
    for (const auto& it : this->tasks) {
      Store::combine_simple_results(this->return_value, move(it->value()));
    }
    this->set_complete();
  }

private:
  MultiStore* store;
  vector<shared_ptr<UpdateMetadataTask>> tasks;
};

shared_ptr<Store::UpdateMetadataTask> MultiStore::update_metadata(StoreTaskManager* m,
    shared_ptr<const UpdateMetadataArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<UpdateMetadataTask>(new MultiStoreUpdateMetadataTask(m, this, args, profiler));
}



class MultiStore::MultiStoreDeleteSeriesTask : public Store::DeleteSeriesTask {
public:
  virtual ~MultiStoreDeleteSeriesTask() = default;
  MultiStoreDeleteSeriesTask(StoreTaskManager* m, MultiStore* s,
      shared_ptr<const DeleteSeriesArguments> args,
      BaseFunctionProfiler* profiler) : DeleteSeriesTask(m, args, profiler),
      store(s) {
    for (const auto& it : this->store->ordered_stores) {
      this->tasks.emplace_back(it->delete_series(m, args, profiler));
      this->delegate(this->tasks.back());
    }
    this->suspend(bind(&MultiStoreDeleteSeriesTask::on_all_tasks_complete, this));
  }

  void on_all_tasks_complete() {
    for (const auto& it : this->tasks) {
      Store::combine_delete_results(this->return_value, move(it->value()));
    }
    this->set_complete();
  }

private:
  MultiStore* store;
  vector<shared_ptr<DeleteSeriesTask>> tasks;
};

shared_ptr<Store::DeleteSeriesTask> MultiStore::delete_series(StoreTaskManager* m,
    shared_ptr<const DeleteSeriesArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<DeleteSeriesTask>(new MultiStoreDeleteSeriesTask(m, this, args, profiler));
}



class MultiStore::MultiStoreRenameSeriesTask : public Store::RenameSeriesTask {
public:
  virtual ~MultiStoreRenameSeriesTask() = default;
  MultiStoreRenameSeriesTask(StoreTaskManager* m, MultiStore* s,
      shared_ptr<const RenameSeriesArguments> args,
      BaseFunctionProfiler* profiler) : RenameSeriesTask(m, args, profiler),
      store(s) {
    for (const auto& it : this->store->ordered_stores) {
      this->tasks.emplace_back(it->rename_series(m, args, profiler));
      this->delegate(this->tasks.back());
    }
    this->suspend(bind(&MultiStoreRenameSeriesTask::on_all_tasks_complete, this));
  }

  void on_all_tasks_complete() {
    for (const auto& it : this->tasks) {
      Store::combine_simple_results(this->return_value, move(it->value()));
    }
    this->set_complete();
  }

private:
  MultiStore* store;
  vector<shared_ptr<RenameSeriesTask>> tasks;
};

shared_ptr<Store::RenameSeriesTask> MultiStore::rename_series(StoreTaskManager* m,
    shared_ptr<const RenameSeriesArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<RenameSeriesTask>(new MultiStoreRenameSeriesTask(m, this, args, profiler));
}



class MultiStore::MultiStoreReadTask : public Store::ReadTask {
public:
  virtual ~MultiStoreReadTask() = default;
  MultiStoreReadTask(StoreTaskManager* m, MultiStore* s,
      shared_ptr<const ReadArguments> args, BaseFunctionProfiler* profiler) :
      ReadTask(m, args, profiler), store(s) {
    for (const auto& it : this->store->ordered_stores) {
      this->tasks.emplace_back(it->read(m, args, profiler));
      this->delegate(this->tasks.back());
    }
    this->suspend(bind(&MultiStoreReadTask::on_all_tasks_complete, this));
  }

  void on_all_tasks_complete() {
    for (const auto& it : this->tasks) {
      Store::combine_read_results(this->return_value, move(it->value()));
    }
    this->set_complete();
  }

private:
  MultiStore* store;
  vector<shared_ptr<ReadTask>> tasks;
};

shared_ptr<Store::ReadTask> MultiStore::read(StoreTaskManager* m,
    shared_ptr<const ReadArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<ReadTask>(new MultiStoreReadTask(m, this, args, profiler));
}



class MultiStore::MultiStoreReadAllTask : public Store::ReadAllTask {
public:
  virtual ~MultiStoreReadAllTask() = default;
  MultiStoreReadAllTask(StoreTaskManager* m, MultiStore* s,
      shared_ptr<const ReadAllArguments> args, BaseFunctionProfiler* profiler) :
      ReadAllTask(m, args, profiler), store(s) {
    for (const auto& it : this->store->ordered_stores) {
      this->tasks.emplace_back(it->read_all(m, args, profiler));
      this->delegate(this->tasks.back());
    }
    this->suspend(bind(&MultiStoreReadAllTask::on_all_tasks_complete, this));
  }

  void on_all_tasks_complete() {
    for (const auto& it : this->tasks) {
      auto& result = it->value();
      if (result.metadata.archive_args.empty()) {
        continue;
      }
      if (!this->return_value.metadata.archive_args.empty()) {
        this->return_value.error = make_error("multiple stores returned nonempty results");
      } else {
        this->return_value = move(result);
      }
    }
    this->set_complete();
  }

private:
  MultiStore* store;
  vector<shared_ptr<ReadAllTask>> tasks;
};

shared_ptr<Store::ReadAllTask> MultiStore::read_all(StoreTaskManager* m,
    shared_ptr<const ReadAllArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<ReadAllTask>(new MultiStoreReadAllTask(m, this, args, profiler));
}



class MultiStore::MultiStoreWriteTask : public Store::WriteTask {
public:
  virtual ~MultiStoreWriteTask() = default;
  MultiStoreWriteTask(StoreTaskManager* m, MultiStore* s,
      shared_ptr<const WriteArguments> args, BaseFunctionProfiler* profiler) :
      WriteTask(m, args, profiler), store(s) {
    for (const auto& it : this->store->ordered_stores) {
      this->tasks.emplace_back(it->write(m, args, profiler));
      this->delegate(this->tasks.back());
    }
    this->suspend(bind(&MultiStoreWriteTask::on_all_tasks_complete, this));
  }

  void on_all_tasks_complete() {
    for (const auto& it : this->tasks) {
      Store::combine_simple_results(this->return_value, move(it->value()));
    }
    this->set_complete();
  }

private:
  MultiStore* store;
  vector<shared_ptr<WriteTask>> tasks;
};

shared_ptr<Store::WriteTask> MultiStore::write(StoreTaskManager* m,
    shared_ptr<const WriteArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<WriteTask>(new MultiStoreWriteTask(m, this, args, profiler));
}



class MultiStore::MultiStoreFindTask : public Store::FindTask {
public:
  virtual ~MultiStoreFindTask() = default;
  MultiStoreFindTask(StoreTaskManager* m, MultiStore* s,
      shared_ptr<const FindArguments> args, BaseFunctionProfiler* profiler) :
      FindTask(m, args, profiler), store(s) {
    for (const auto& it : this->store->ordered_stores) {
      this->tasks.emplace_back(it->find(m, args, profiler));
      this->delegate(this->tasks.back());
    }
    this->suspend(bind(&MultiStoreFindTask::on_all_tasks_complete, this));
  }

  void on_all_tasks_complete() {
    for (const auto& it : this->tasks) {
      Store::combine_find_results(this->return_value, move(it->value()));
    }
    this->set_complete();
  }

private:
  MultiStore* store;
  vector<shared_ptr<FindTask>> tasks;
};

shared_ptr<Store::FindTask> MultiStore::find(StoreTaskManager* m,
    shared_ptr<const FindArguments> args, BaseFunctionProfiler* profiler) {
  return shared_ptr<FindTask>(new MultiStoreFindTask(m, this, args, profiler));
}



unordered_map<string, int64_t> MultiStore::get_stats(bool rotate) {
  unordered_map<string, int64_t> ret;
  for (const auto& it : this->all_stores) {
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

string MultiStore::str() const {
  string ret = "MultiStore(";
  for (const auto& it : this->all_stores) {
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
