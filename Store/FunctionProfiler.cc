#include "FunctionProfiler.hh"

#include <inttypes.h>

#include <atomic>
#include <map>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>
#include <string>

using namespace std;



void BaseFunctionProfiler::add_metadata(const std::string& key,
    const std::string& value) { }

void BaseFunctionProfiler::checkpoint(const std::string& key) { }

std::string BaseFunctionProfiler::output(uint64_t end_time) {
  return "";
}



FunctionProfiler::FunctionProfiler(const char* function_name,
    uint64_t threshold_usecs) : function_name(function_name), start_time(now()),
    threshold_usecs(threshold_usecs), output_called(false) { }

FunctionProfiler::~FunctionProfiler() {
  if (output_called) {
    return;
  }

  uint64_t end_time = now();
  uint64_t total_time = end_time - this->start_time;
  if (total_time >= this->threshold_usecs) {
    string result = this->output(end_time);
    log(INFO, "%s", result.c_str());
  }
}

void FunctionProfiler::add_metadata(const string& key, const string& value) {
  this->metadata.emplace(key, value);
}

void FunctionProfiler::checkpoint(const string& key) {
  this->checkpoints.emplace(now(), key);
}

std::string FunctionProfiler::output(uint64_t end_time) {
  this->output_called = true;

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



static atomic<int64_t> default_threshold_usecs(-1);

void set_profiler_threshold(int64_t threshold_usecs) {
  default_threshold_usecs = threshold_usecs;
}

unique_ptr<BaseFunctionProfiler> create_profiler(const char* function_name,
    int64_t threshold_usecs) {
  if (threshold_usecs < 0) {
    return unique_ptr<BaseFunctionProfiler>(new BaseFunctionProfiler());
  } else {
    return unique_ptr<BaseFunctionProfiler>(
        new FunctionProfiler(function_name, threshold_usecs));
  }
}

unique_ptr<BaseFunctionProfiler> create_profiler(const char* function_name) {
  return create_profiler(function_name, default_threshold_usecs);
}
