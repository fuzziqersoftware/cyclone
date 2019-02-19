#include "Store.hh"

#include <phosg/Strings.hh>
#include <phosg/Time.hh>

#include "Formats/Whisper.hh"
#include "Utils/Errors.hh"

using namespace std;



StoreTask::StoreTask(StoreTaskManager* m, BaseFunctionProfiler* profiler) :
    manager(m), complete(false), profiler(profiler), suspend_count(0) { }

bool StoreTask::is_complete() const {
  return this->complete;
}

void StoreTask::set_complete() {
  this->complete = true;
  for (auto& cb : this->on_complete_callbacks) {
    cb();
  }
  this->on_complete_callbacks.clear();
}

void StoreTask::add_complete_callback(std::function<void()> cb) {
  if (this->is_complete()) {
    throw logic_error("cannot add complete callback for completed task");
  }
  this->on_complete_callbacks.push_back(cb);
}

void StoreTask::delegate(std::shared_ptr<StoreTask> child) {
  if (!child->is_complete()) {
    child->add_complete_callback(bind(&StoreTask::on_child_completed, this));
    this->suspend_count++;
  }
}

void StoreTask::delegate(std::shared_ptr<StoreTask> child, std::function<void()> callback) {
  this->delegate(child);
  this->suspend(callback);
}

void StoreTask::suspend(std::function<void()> callback) {
  if (this->suspend_count == 0) {
    this->manager->add_ready_callback(callback);
  } else {
    this->resume_callback = callback;
  }
}

void StoreTask::read_suspend(int fd, std::function<void()> callback) {
  this->manager->read_suspend(this, fd, callback);
}

void StoreTask::on_child_completed() {
  this->suspend_count--;
  if (this->suspend_count == 0) {
    this->manager->add_ready_callback(this->resume_callback);
    this->resume_callback = std::function<void()>();
  }
}



void StoreTaskManager::add_ready_callback(std::function<void()> fn) {
  this->pending.emplace_back(fn);
}

void StoreTaskManager::run(std::shared_ptr<StoreTask> task) {
  while (!task->is_complete()) {
    if (this->pending.empty() && this->pollfds.empty()) {
      throw logic_error("incomplete task with no pending callbacks");
    }

    if (!this->pollfds.empty()) {
      // don't block if there's pending work to do
      int timeout = this->pending.empty() ? -1 : 0;
      int ret = poll(this->pollfds.data(), this->pollfds.size(), timeout);
      if (ret < 0) {
        // if it's EINTR, just try again. if it's anything else, fail
        if (errno != EINTR) {
          string s = string_for_error(errno);
          throw runtime_error(string_printf("poll failed with error %d: %s", errno, s.c_str()));
        }
      } else if (ret > 0) {
        // if any fds have reads pending, make their callbacks ready again
        for (auto it = this->pollfds.begin(); it != this->pollfds.end();) {
          if (it->revents & (POLLIN | POLLHUP)) {
            auto cb_it = this->fd_to_callback.find(it->fd);
            if (cb_it == this->fd_to_callback.end()) {
              throw logic_error("pollfd ready but no callback pending");
            }
            this->pending.emplace_back(move(cb_it->second));
            this->fd_to_callback.erase(cb_it);
            it = this->pollfds.erase(it);
          } else {
            it++;
          }
        }
      }
    }

    for (size_t count = this->pending.size(); count > 0; count--) {
      auto cb = move(this->pending.front());
      this->pending.pop_front();
      cb();
    }
  }
}

void StoreTaskManager::read_suspend(StoreTask* task, int fd,
    std::function<void()> callback) {
  auto emplace_ret = this->fd_to_callback.emplace(fd, callback);
  if (!emplace_ret.second) {
    throw logic_error("multiple tasks blocked on the same fd");
  }

  this->pollfds.emplace_back();
  auto& pfd = this->pollfds.back();
  pfd.fd = fd;
  pfd.events = POLLIN;
  pfd.revents = 0;
  return;
}
