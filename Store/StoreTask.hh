#pragma once

#include <poll.h>

#include <deque>
#include <memory>
#include <utility>
#include <stdexcept>
#include <functional>

#include "Utils/FunctionProfiler.hh"

class StoreTaskManager;


class StoreTask {
public:
  // TODO: figure out why returning these directly doesn't work even if the move
  // constructors are defaulted (the compiler complains about the COPY
  // constructors being deleted... can't you just move them like I told you to?)
  // if this is fixed, then we won't need shared_ptrs everywhere
  StoreTask(const StoreTask& rhs) = delete;
  StoreTask(StoreTask&& rhs) = delete;
  virtual ~StoreTask() = default;

  StoreTask& operator=(const StoreTask& rhs) = delete;
  StoreTask& operator=(StoreTask&& rhs) = delete;

  bool is_complete() const;
  void set_complete();

  void add_complete_callback(std::function<void()> cb);

  friend class StoreTaskManager;

protected:
  explicit StoreTask(StoreTaskManager* m = NULL, BaseFunctionProfiler* profiler = NULL);

  // shortcut for delegate + suspend below
  void delegate(std::shared_ptr<StoreTask> child,
      std::function<void()> callback);

  void delegate(std::shared_ptr<StoreTask> child);
  void suspend(std::function<void()> callback);

  // wait for an fd to become readable
  void read_suspend(int fd, std::function<void()> callback);

  void on_child_completed();

  StoreTaskManager* manager;
  bool complete;

  BaseFunctionProfiler* profiler;

  std::function<void()> resume_callback;
  size_t suspend_count;
  std::deque<std::function<void()>> on_complete_callbacks;
};

template <typename R>
class TypedStoreTask : public StoreTask {
public:
  virtual ~TypedStoreTask() = default;

  // TODO: do we need something like this?
  //virtual void schedule_on() = 0;

  void set_value(const R& return_value) {
    this->return_value = return_value;
    this->set_complete();
  }

  void set_value(R&& return_value) {
    this->return_value = std::move(return_value);
    this->set_complete();
  }

  // note: this isn't a const reference so the caller can move() it if they want
  // to. we assume the caller knows what they're doing in this regard
  R& value() {
    if (!this->complete) {
      throw std::logic_error("retrieving value for incomplete task");
    }
    return this->return_value;
  }

protected:
  explicit TypedStoreTask() = default;
  explicit TypedStoreTask(StoreTaskManager* m,
      BaseFunctionProfiler* profiler = NULL) : StoreTask(m, profiler) { }

  R return_value;
};


class StoreTaskManager {
public:
  StoreTaskManager() = default;
  ~StoreTaskManager() = default;

  void add_ready_callback(std::function<void()> cb);
  void run(std::shared_ptr<StoreTask> task);

  void read_suspend(StoreTask* task, int fd, std::function<void()> callback);

protected:
  std::deque<std::function<void()>> pending;
  std::vector<struct pollfd> pollfds;
  std::unordered_map<int, std::function<void()>> fd_to_callback;
};
