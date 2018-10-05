#include <inttypes.h>
#include <unistd.h>

#include <phosg/UnitTest.hh>
#include <thread>

#include "RateLimiter.hh"

using namespace std;


int main(int argc, char* argv[]) {
  {
    printf("-- single-threaded test\n");

    RateLimiter r(100); // 100 actions per second

    printf("---- first 100 actions are allowed\n");

    for (size_t x = 0; x < 100; x++) {
      expect_eq(0, r.delay_until_next_action());
    }

    printf("---- next actions are delayed, but not more than 1 second\n");

    uint64_t delay = r.delay_until_next_action();
    expect_ne(0, delay);
    expect_lt(delay, 1000000);

    for (size_t x = 0; x < 100; x++) {
      expect_ne(0, r.delay_until_next_action());
    }

    usleep(delay);

    printf("---- later, 100 more actions are allowed\n");

    for (size_t x = 0; x < 100; x++) {
      expect_eq(0, r.delay_until_next_action());
    }

    printf("---- next actions are delayed, but not more than 1 second\n");

    delay = r.delay_until_next_action();
    expect_ne(0, delay);
    expect_lt(delay, 1000000);

    for (size_t x = 0; x < 100; x++) {
      expect_ne(0, r.delay_until_next_action());
    }
  }

  {
    printf("-- concurrent test\n");

    static const size_t thread_count = 4;
    static const size_t num_actions_per_thread = 100000;
    static const size_t window_usecs = 3000000;

    RateLimiter r(num_actions_per_thread, window_usecs);

    atomic<uint64_t> allowed_actions_counts[thread_count];
    atomic<uint64_t> denied_actions_counts[thread_count];
    thread threads[thread_count];

    auto thread_fn = [](RateLimiter* r, atomic<uint64_t>* allowed_actions_count,
        atomic<uint64_t>* denied_actions_count) {
      for (size_t x = 0; x < num_actions_per_thread; x++) {
        if (r->delay_until_next_action() == 0) {
          (*allowed_actions_count)++;
        } else {
          (*denied_actions_count)++;
        }
      }
    };

    for (size_t x = 0; x < thread_count; x++) {
      allowed_actions_counts[x] = 0;
      denied_actions_counts[x] = 0;
    }

    for (size_t x = 0; x < thread_count; x++) {
      threads[x] = thread(thread_fn, &r, &allowed_actions_counts[x],
          &denied_actions_counts[x]);
    }
    for (size_t x = 0; x < thread_count; x++) {
      threads[x].join();
    }

    uint64_t total_allowed_actions = 0;
    uint64_t total_denied_actions = 0;
    for (size_t x = 0; x < thread_count; x++) {
      expect_eq(num_actions_per_thread, allowed_actions_counts[x] + denied_actions_counts[x]);
      expect_ne(0, allowed_actions_counts[x]);
      expect_ne(0, denied_actions_counts[x]);
      total_allowed_actions += allowed_actions_counts[x];
      total_denied_actions += denied_actions_counts[x];
      fprintf(stderr, "---- thread %zu did %" PRIu64 " actions and was denied %" PRIu64 " times\n",
          x, allowed_actions_counts[x].load(), denied_actions_counts[x].load());
    }
    expect_eq(num_actions_per_thread, total_allowed_actions);
    expect_eq(num_actions_per_thread * (thread_count - 1), total_denied_actions);

    printf("-- concurrent reset test\n");
    usleep(window_usecs);

    for (size_t x = 0; x < thread_count; x++) {
      allowed_actions_counts[x] = 0;
      denied_actions_counts[x] = 0;
    }

    for (size_t x = 0; x < thread_count; x++) {
      threads[x] = thread(thread_fn, &r, &allowed_actions_counts[x],
          &denied_actions_counts[x]);
    }
    for (size_t x = 0; x < thread_count; x++) {
      threads[x].join();
    }

    total_allowed_actions = 0;
    total_denied_actions = 0;
    for (size_t x = 0; x < thread_count; x++) {
      expect_eq(num_actions_per_thread, allowed_actions_counts[x] + denied_actions_counts[x]);
      expect_ne(0, allowed_actions_counts[x]);
      expect_ne(0, denied_actions_counts[x]);
      total_allowed_actions += allowed_actions_counts[x];
      total_denied_actions += denied_actions_counts[x];
      fprintf(stderr, "---- thread %zu did %" PRIu64 " actions and was denied %" PRIu64 " times\n",
          x, allowed_actions_counts[x].load(), denied_actions_counts[x].load());
    }
    expect_eq(num_actions_per_thread, total_allowed_actions);
    expect_eq(num_actions_per_thread * (thread_count - 1), total_denied_actions);
  }

  printf("all tests passed\n");
  return 0;
}
