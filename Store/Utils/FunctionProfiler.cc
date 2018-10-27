#include "FunctionProfiler.hh"

#include <inttypes.h>

#include <atomic>
#include <map>
#include <phosg/Concurrency.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>
#include <string>
#include <unordered_map>

using namespace std;



BaseFunctionProfiler::BaseFunctionProfiler(const string& function_name) :
    function_name(function_name), output_called(false) { }

void BaseFunctionProfiler::add_metadata(const string& key,
    const string& value) { }

void BaseFunctionProfiler::checkpoint(const string& key) { }

void BaseFunctionProfiler::check_for_slow_query() {
  this->output_called.store(true);
}

string BaseFunctionProfiler::output(uint64_t end_time) {
  this->output_called.store(true);
  return "";
}

bool BaseFunctionProfiler::done() const {
  return this->output_called.load();
}

string BaseFunctionProfiler::get_function_name() const {
  return this->function_name;
}



FunctionProfiler::FunctionProfiler(const string& function_name,
    uint64_t threshold_usecs) : BaseFunctionProfiler(function_name),
    start_time(now()), threshold_usecs(threshold_usecs) { }

void FunctionProfiler::add_metadata(const string& key, const string& value) {
  this->metadata.emplace(key, value);
}

void FunctionProfiler::checkpoint(const string& key) {
  this->checkpoints.emplace(now(), key);
}

void FunctionProfiler::check_for_slow_query() {
  if (this->output_called.load()) {
    return;
  }

  uint64_t end_time = now();
  uint64_t total_time = end_time - this->start_time;
  if (total_time >= this->threshold_usecs) {
    string result = this->output(end_time);
    log(INFO, "slow query: %s", result.c_str());
  } else {
    this->output_called.store(true);
  }
}

string FunctionProfiler::output(uint64_t end_time) {
  this->output_called.store(true);

  if (end_time == 0) {
    end_time = now();
  }
  uint64_t total_time = end_time - this->start_time;

  string result = this->function_name;

  if (!this->metadata.empty()) {
    result += " (";
    bool needs_spacer = false;
    for (auto it : this->metadata) {
      if (needs_spacer) {
        result += ", ";
      }
      needs_spacer = true;
      result += it.first;
      result += '=';
      result += it.second;
    }
    result += ')';
  }

  if (!this->checkpoints.empty()) {
    uint64_t last_checkpoint_time = start_time;
    result += " (";
    for (auto it : this->checkpoints) {
      result += string_printf("%s: %" PRIu64 ", ", it.second.c_str(),
          it.first - last_checkpoint_time);
      last_checkpoint_time = it.first;
    }
    result += string_printf("<remainder>: %" PRIu64,
        end_time - last_checkpoint_time);
    result += ')';
  }
  result += string_printf(" (overall: %" PRIu64 ")", total_time);
  return result;
}



ProfilerGuard::ProfilerGuard(shared_ptr<BaseFunctionProfiler> profiler) :
    profiler(profiler) { }

ProfilerGuard::~ProfilerGuard() {
  this->profiler->check_for_slow_query();
}



static atomic<int64_t> default_threshold_usecs(-1);
static atomic<int64_t> default_internal_threshold_usecs(-1);

// the rw lock below protects thread_name_to_slot and the data pointer in
// profiler_slots, but not the contents of profiler_slots - because shared_ptrs
// are thread-safe, we don't need to write-lock the vector's contents if we only
// do operations that don't cause reallocations
static vector<shared_ptr<BaseFunctionProfiler>> profiler_slots;
static unordered_map<string, size_t> thread_name_to_slot;
static rw_lock profiler_slots_lock;

void set_profiler_threshold(bool internal, int64_t threshold_usecs) {
  if (internal) {
    default_internal_threshold_usecs = threshold_usecs;
  } else {
    default_threshold_usecs = threshold_usecs;
  }
}

shared_ptr<BaseFunctionProfiler> create_profiler(const string& thread_name,
    const string& function_name, int64_t threshold_usecs) {
  shared_ptr<BaseFunctionProfiler> ret;
  if (threshold_usecs < 0) {
    ret.reset(new BaseFunctionProfiler(function_name));
  } else {
    ret.reset(new FunctionProfiler(function_name, threshold_usecs));
  }

  try {
    rw_guard g(profiler_slots_lock, false);
    size_t slot = thread_name_to_slot.at(thread_name);
    profiler_slots[slot] = ret;

  } catch (const out_of_range&) {
    rw_guard g(profiler_slots_lock, true);
    size_t slot = profiler_slots.size();
    thread_name_to_slot.emplace(thread_name, slot);
    profiler_slots.emplace_back(ret);
  }

  return ret;
}

shared_ptr<BaseFunctionProfiler> create_profiler(const string& thread_name,
    const string& function_name) {
  return create_profiler(thread_name, function_name, default_threshold_usecs);
}

shared_ptr<BaseFunctionProfiler> create_internal_profiler(
    const string& thread_name, const string& function_name) {
  return create_profiler(thread_name, function_name,
      default_internal_threshold_usecs);
}

map<string, shared_ptr<BaseFunctionProfiler>> get_active_profilers() {
  rw_guard g(profiler_slots_lock, false);

  map<string, shared_ptr<BaseFunctionProfiler>> ret;
  for (const auto& slot_it : thread_name_to_slot) {
    ret.emplace(slot_it.first, profiler_slots[slot_it.second]);
  }
  return ret;
}
