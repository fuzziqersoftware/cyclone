#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>


class BaseFunctionProfiler {
public:
  BaseFunctionProfiler() = delete;
  explicit BaseFunctionProfiler(const std::string& function_name);
  BaseFunctionProfiler(const BaseFunctionProfiler&) = delete;
  BaseFunctionProfiler(BaseFunctionProfiler&&) = delete;
  BaseFunctionProfiler& operator=(const BaseFunctionProfiler&) = delete;
  BaseFunctionProfiler& operator=(BaseFunctionProfiler&&) = delete;
  virtual ~BaseFunctionProfiler() = default;

  virtual void add_metadata(const std::string& key, const std::string& value);
  virtual void checkpoint(const std::string& key);

  virtual void check_for_slow_query();
  virtual std::string output(uint64_t end_time = 0);
  bool done() const;

  std::string get_function_name() const;

protected:
  std::string function_name;
  std::atomic<bool> output_called;
};


class FunctionProfiler : public BaseFunctionProfiler {
public:
  FunctionProfiler() = delete;
  FunctionProfiler(const std::string& function_name, uint64_t threshold_usecs);
  FunctionProfiler(const FunctionProfiler&) = delete;
  FunctionProfiler(FunctionProfiler&&) = delete;
  FunctionProfiler& operator=(const FunctionProfiler&) = delete;
  FunctionProfiler& operator=(FunctionProfiler&&) = delete;
  virtual ~FunctionProfiler() = default;

  virtual void add_metadata(const std::string& key, const std::string& value);
  virtual void checkpoint(const std::string& key);

  virtual void check_for_slow_query();
  virtual std::string output(uint64_t end_time = 0);

protected:
  uint64_t start_time;
  uint64_t threshold_usecs;
  std::map<std::string, std::string> metadata;
  std::map<uint64_t, std::string> checkpoints;
};


struct ProfilerGuard {
  std::shared_ptr<BaseFunctionProfiler> profiler;

  ProfilerGuard(std::shared_ptr<BaseFunctionProfiler> profiler);
  ~ProfilerGuard();
};


void set_profiler_threshold(bool internal, int64_t threshold_usecs);
std::shared_ptr<BaseFunctionProfiler> create_profiler(
    const std::string& thread_name, const std::string& function_name,
    int64_t threshold_usecs);
std::shared_ptr<BaseFunctionProfiler> create_profiler(
    const std::string& thread_name, const std::string& function_name);
std::shared_ptr<BaseFunctionProfiler> create_internal_profiler(
    const std::string& thread_name, const std::string& function_name);
std::map<std::string, std::shared_ptr<BaseFunctionProfiler>> get_active_profilers();
