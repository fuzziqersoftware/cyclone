#pragma once

#include <map>
#include <memory>
#include <string>


class BaseFunctionProfiler {
public:
  BaseFunctionProfiler() = default;
  BaseFunctionProfiler(const char* function_name, uint64_t threshold_usecs);
  BaseFunctionProfiler(const BaseFunctionProfiler&) = delete;
  BaseFunctionProfiler(BaseFunctionProfiler&&) = delete;
  BaseFunctionProfiler& operator=(const BaseFunctionProfiler&) = delete;
  BaseFunctionProfiler& operator=(BaseFunctionProfiler&&) = delete;
  virtual ~BaseFunctionProfiler() = default;

  virtual void add_metadata(const std::string& key, const std::string& value);
  virtual void checkpoint(const std::string& key);

  virtual std::string output(uint64_t end_time = 0);
};


class FunctionProfiler : public BaseFunctionProfiler {
public:
  FunctionProfiler() = delete;
  FunctionProfiler(const char* function_name, uint64_t threshold_usecs);
  FunctionProfiler(const FunctionProfiler&) = delete;
  FunctionProfiler(FunctionProfiler&&) = delete;
  FunctionProfiler& operator=(const FunctionProfiler&) = delete;
  FunctionProfiler& operator=(FunctionProfiler&&) = delete;
  virtual ~FunctionProfiler();

  virtual void add_metadata(const std::string& key, const std::string& value);
  virtual void checkpoint(const std::string& key);

  virtual std::string output(uint64_t end_time = 0);

private:
  const char* function_name;
  uint64_t start_time;
  uint64_t threshold_usecs;
  std::map<std::string, std::string> metadata;
  std::map<uint64_t, std::string> checkpoints;
  bool output_called;
};


void set_profiler_threshold(int64_t threshold_usecs);
std::unique_ptr<BaseFunctionProfiler> create_profiler(
    const char* function_name);
std::unique_ptr<BaseFunctionProfiler> create_profiler(
    const char* function_name, int64_t threshold_usecs);
