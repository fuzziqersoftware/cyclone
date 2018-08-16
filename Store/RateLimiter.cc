#include "RateLimiter.hh"

#include <stdint.h>

#include <atomic>
#include <phosg/Time.hh>


RateLimiter::RateLimiter(size_t limit, uint64_t window_usecs) : budget(0),
    limit(limit), next_refresh_time(0), window_usecs(window_usecs) { }

uint64_t RateLimiter::get_budget() const {
  return this->budget;
}

uint64_t RateLimiter::get_limit() const {
  return this->limit;
}

uint64_t RateLimiter::get_next_refresh_time() const {
  return this->next_refresh_time;
}

uint64_t RateLimiter::get_window_usecs() const {
  return this->window_usecs;
}

void RateLimiter::set_limit(uint64_t limit) {
  this->limit = limit;
}

void RateLimiter::set_window_usecs(uint64_t window_usecs) {
  this->window_usecs = window_usecs;
}

uint64_t RateLimiter::delay_until_next_action() {
  uint64_t now_usecs = now();

  // if it's time to refresh the budget, atomically update the budget refresh
  // time and refresh the budget
  uint64_t local_next_refresh_time = this->next_refresh_time.load();
  if (now_usecs > local_next_refresh_time) {
    uint64_t new_next_refresh_time = now_usecs + this->window_usecs;
    if (this->next_refresh_time.compare_exchange_strong(
        local_next_refresh_time, new_next_refresh_time)) {
      this->budget.store(this->limit.load());
    }
  }

  // consume an item from the budget. if the budget goes negative, add back the
  // item we consumed and return nonzero; otherwise, return zero
  if ((--this->budget) < 0) {
    this->budget++;
    int64_t ret = this->next_refresh_time - now_usecs;
    return (ret <= 0) ? 1 : ret;
  }

  return 0;
}
