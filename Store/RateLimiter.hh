#pragma once

#include <stddef.h>
#include <stdint.h>

#include <atomic>


class RateLimiter {
public:
  RateLimiter() = delete;
  explicit RateLimiter(size_t limit, uint64_t window_usecs = 1000000);
  RateLimiter(const RateLimiter&) = delete;
  RateLimiter(RateLimiter&&) = delete;
  ~RateLimiter() = default;
  RateLimiter& operator=(const RateLimiter&) = delete;
  RateLimiter& operator=(RateLimiter&&) = delete;

  uint64_t get_budget() const;
  uint64_t get_limit() const;
  uint64_t get_next_refresh_time() const;
  uint64_t get_window_usecs() const;

  void set_limit(uint64_t limit);
  void set_window_usecs(uint64_t window_usecs);

  // checks if an action can be taken. if it can, returns 0; if it can't,
  // returns the number of microseconds until it can
  uint64_t delay_until_next_action();

private:
  std::atomic<int64_t> budget;
  std::atomic<int64_t> limit;
  std::atomic<uint64_t> next_refresh_time;
  std::atomic<uint64_t> window_usecs;
};
