#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <algorithm>
#include <phosg/Filesystem.hh>
#include <phosg/Strings.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "StoreTask.hh"
#include "DiskStore.hh"
#include "CachedDiskStore.hh"
#include "MultiStore.hh"
#include "ConsistentHashMultiStore.hh"
#include "WriteBufferStore.hh"

using namespace std;


void check_token_is_pattern(bool expected_result, const string& token) {
  printf("-- %s %s a pattern\n", token.c_str(), expected_result ? "is" : "is not");
  expect_eq(expected_result, Store::token_is_pattern(token));
}

void check_pattern_is_basename(bool expected_result, const string& pattern) {
  printf("-- %s %s a basename\n", pattern.c_str(), expected_result ? "is" : "is not");
  expect_eq(expected_result, Store::pattern_is_basename(pattern));
}

void check_pattern_is_indeterminate(bool expected_result, const string& pattern) {
  printf("-- %s %s indeterminate\n", pattern.c_str(), expected_result ? "is" : "is not");
  expect_eq(expected_result, Store::pattern_is_indeterminate(pattern));
}

void check_name_matches_pattern(bool expected_result, const string& name, const string& pattern) {
  printf("-- %s %s %s\n", pattern.c_str(), expected_result ? "matches" : "does not match",
      name.c_str());
  expect_eq(expected_result, Store::name_matches_pattern(name, pattern));
}



std::unordered_map<std::string, Error> execute_update_metadata(
    shared_ptr<StoreTaskManager> m, shared_ptr<Store> s,
    const SeriesMetadataMap& metadata,
    Store::UpdateMetadataBehavior update_behavior, bool create_new,
    bool skip_buffering, bool local_only, BaseFunctionProfiler* profiler) {
  auto task = s->update_metadata(m.get(), metadata, update_behavior, create_new,
      skip_buffering, local_only, profiler);
  m->run(task);
  return task->value();
}

std::unordered_map<std::string, DeleteResult> execute_delete_series(
    shared_ptr<StoreTaskManager> m, shared_ptr<Store> s,
    const std::vector<std::string>& patterns, bool deferred, bool local_only,
    BaseFunctionProfiler* profiler) {
  auto task = s->delete_series(m.get(), patterns, deferred, local_only,
      profiler);
  m->run(task);
  return task->value();
}

std::unordered_map<std::string, Error> execute_rename_series(
    shared_ptr<StoreTaskManager> m, shared_ptr<Store> s,
    const std::unordered_map<std::string, std::string>& renames,
    bool merge, bool local_only, BaseFunctionProfiler* profiler) {
  auto task = s->rename_series(m.get(), renames, merge, local_only, profiler);
  m->run(task);
  return task->value();
}

std::unordered_map<std::string, std::unordered_map<std::string, ReadResult>> execute_read(
    shared_ptr<StoreTaskManager> m, shared_ptr<Store> s,
    const std::vector<std::string>& key_names, int64_t start_time,
    int64_t end_time, bool local_only, BaseFunctionProfiler* profiler) {
  auto task = s->read(m.get(), key_names, start_time, end_time, local_only,
      profiler);
  m->run(task);
  return task->value();
}

ReadAllResult execute_read_all(shared_ptr<StoreTaskManager> m,
    shared_ptr<Store> s,const std::string& key_name, bool local_only,
    BaseFunctionProfiler* profiler) {
  auto task = s->read_all(m.get(), key_name, local_only, profiler);
  m->run(task);
  return task->value();
}

std::unordered_map<std::string, Error> execute_write(
    shared_ptr<StoreTaskManager> m, shared_ptr<Store> s,
    const std::unordered_map<std::string, Series>& data, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  auto task = s->write(m.get(), data, skip_buffering, local_only, profiler);
  m->run(task);
  return task->value();
}

std::unordered_map<std::string, FindResult> execute_find(
    shared_ptr<StoreTaskManager> m, shared_ptr<Store> s,
    const std::vector<std::string>& patterns, bool local_only,
    BaseFunctionProfiler* profiler) {
  auto task = s->find(m.get(), patterns, local_only, profiler);
  m->run(task);
  return task->value();
}



void run_internal_functions_test() {
  // tokens are patterns if they include *, **, [], or {}
  check_token_is_pattern(false, "key1");
  check_token_is_pattern(false, "test.dir1.key1");
  check_token_is_pattern(true, "test.*.key1");
  check_token_is_pattern(true, "test.**.key1");
  check_token_is_pattern(true, "test.dir[123].key1");
  check_token_is_pattern(true, "test.dir{1,2,3}.key1");

  // patterns are basenames if they don't include anything that could match a
  // directory (. or **)
  check_pattern_is_basename(true, "key1");
  check_pattern_is_basename(true, "key*");
  check_pattern_is_basename(true, "key[123]");
  check_pattern_is_basename(true, "key{1,2,3}");
  check_pattern_is_basename(false, "key**");
  check_pattern_is_basename(false, "test.key1");

  // patterns are indeterminate if the number of result directories isn't
  // obvious from the pattern itself (that is, if they include **)
  check_pattern_is_indeterminate(false, "key1");
  check_pattern_is_indeterminate(false, "test.dir1.key1");
  check_pattern_is_indeterminate(false, "test.*.key1");
  check_pattern_is_indeterminate(true, "test.**.key1");
  check_pattern_is_indeterminate(false, "test.dir[123].key1");
  check_pattern_is_indeterminate(false, "test.dir{1,2,3}.key1");

  // duh
  check_name_matches_pattern(true, "test.key1", "test.key1");
  check_name_matches_pattern(false, "test.key2", "test.key1");

  // check that * matches up to one directory
  check_name_matches_pattern(true, "test.dir1.key1", "test.*.key1");
  check_name_matches_pattern(true, "test.dir1.key1", "test.dir*.key1");
  check_name_matches_pattern(true, "test.dir1.key1", "test.*1.key1");
  check_name_matches_pattern(false, "test.dir1.key1", "*.key1");
  check_name_matches_pattern(false, "test.dir1.key1", "test.*");
  check_name_matches_pattern(true, "test.dir1.key1", "*.dir1.key1");
  check_name_matches_pattern(true, "test.dir1.key1", "test.*.key1");
  check_name_matches_pattern(true, "test.dir1.key1", "test.dir1.*");

  // check that ** matches an arbitrary number of directories
  check_name_matches_pattern(true, "test.dir1.key1", "**");
  check_name_matches_pattern(true, "test.dir1.key1", "**.key1");
  check_name_matches_pattern(true, "test.dir1.key1", "test.**");
  check_name_matches_pattern(true, "test.dir1.key1", "**.dir1.key1");
  check_name_matches_pattern(true, "test.dir1.key1", "test.**.key1");
  check_name_matches_pattern(true, "test.dir1.key1", "test.dir1.**");
  check_name_matches_pattern(true, "test.dir1.key1", "test.dir**.key1");
  check_name_matches_pattern(true, "test.dir1.key1", "test.**1.key1");

  // check that [abc] works as expected
  check_name_matches_pattern(true, "test.dir1.key1", "test.dir[123].key1");
  check_name_matches_pattern(false, "test.dir1.key1", "test.dir[234].key1");
  check_name_matches_pattern(true, "test.dir1.key1", "test.dir[123].key[135]");
  check_name_matches_pattern(false, "test.dir1.key1", "test.dir[123].key[357]");
  check_name_matches_pattern(false, "test.dir1.key1", "test.dir[234].key[135]");

  // check that {a,b,c} works as expected
  check_name_matches_pattern(true, "test.dir1.key1", "test.{dir1,dir2}.key1");
  check_name_matches_pattern(false, "test.dir1.key1", "test.{dir0,dir2}.key1");

  // check combinations
  check_name_matches_pattern(true, "test.dir1.key1", "test.*{r1,r2}.key1");
  check_name_matches_pattern(false, "test.dir1.key1", "test.*{r2,r3}.key1");
  check_name_matches_pattern(true, "test.dir1.key1", "test.*[123].key1");
  check_name_matches_pattern(false, "test.dir1.key1", "test.*[345].key1");
  check_name_matches_pattern(true, "test.dir1.key1", "test.dir*.{ke,bu}y[123]");
  check_name_matches_pattern(false, "test.dir1.key1", "test.dir*.{gu,bu}y[123]");
  check_name_matches_pattern(false, "test.dir1.key1", "test.dir*.{ke,bu}y[234]");
  check_name_matches_pattern(false, "test.dir1.key1", "test.diz*.{ke,bu}y[123]");
}

void run_basic_test(shared_ptr<StoreTaskManager> m, shared_ptr<Store> s,
    const string& store_name, const string& data_directory,
    bool is_write_buffer = false) {
  time_t test_now = time(NULL);
  auto profiler = create_profiler("StoreTest", "run_basic_test", 1000000000);

  string key_name1 = "test.DiskStore.key1";
  string key_name2 = "test.key2";
  string autocreate_key_name1 = "test.autocreate.dir1.dir2.dir3.key1";
  string rename_key_name = "test.rename.key1";
  string key_filename1 = data_directory + "/test/DiskStore/key1.wsp";
  string key_filename2 = data_directory + "/test/key2.wsp";
  string autocreate_key_filename1 = data_directory + "/test/autocreate/dir1/dir2/dir3/key1.wsp";
  string autocreate_key_filename2 = data_directory + "/test/autocreate/dir1/dir2/dir3/key2.wsp";
  string rename_key_filename = data_directory + "/test/rename/key1.wsp";
  string pattern1 = "test.*";
  string pattern2 = "test.NoSuchDirectory*";
  string pattern3 = "test.DiskStore.no_such_key*";
  string pattern4 = "test.DiskStore.*";
  string pattern5 = "test.**";
  string pattern6 = "test.nonexistent_dir.**";
  string autocreate_pattern = "test.autocreate.dir1.dir2.dir3.*";
  string rename_pattern = "test.rename.*";

  {
    printf("-- [%s:basic_test] read from nonexistent series\n", store_name.c_str());
    auto ret = execute_read(m, s, {key_name1}, test_now - 10 * 60, test_now, false, profiler.get());
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(key_name1).size());
    expect_eq("", ret.at(key_name1).at(key_name1).error.description);
    expect(ret.at(key_name1).at(key_name1).data.empty());
    expect_eq(ret.at(key_name1).at(key_name1).start_time, test_now - 10 * 60);
    expect_eq(ret.at(key_name1).at(key_name1).end_time, test_now);
    expect_eq(ret.at(key_name1).at(key_name1).step, 0);
    expect(!isfile(key_filename1));
  }

  unordered_map<string, Series> write_data;
  write_data[key_name1].emplace_back();
  write_data[key_name1].back().timestamp = test_now;
  write_data[key_name1].back().value = 2.0;

  {
    printf("-- [%s:basic_test] write to nonexistent series (no autocreate)\n", store_name.c_str());
    auto ret = execute_write(m, s, write_data, false, false, profiler.get());
    expect_eq(1, ret.size());
    expect_eq(is_write_buffer, ret.at(key_name1).description.empty());
    s->flush(m.get());
    expect(!isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] find with no results\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern1}, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(pattern1).error.description.empty());
    expect(ret.at(pattern1).results.empty());
  }

  {
    printf("-- [%s:basic_test] find all with no results\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern5}, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(pattern5).error.description.empty());
    expect(ret.at(pattern5).results.empty());
  }

  {
    printf("-- [%s:basic_test] find all with no results through missing directory\n",
        store_name.c_str());
    auto ret = execute_find(m, s, {pattern6}, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(pattern6).error.description.empty());
    expect(ret.at(pattern6).results.empty());
  }

  SeriesMetadataMap metadata_map;
  auto& metadata = metadata_map[key_name1];
  metadata.archive_args.emplace_back();
  metadata.archive_args.back().precision = 60;
  metadata.archive_args.back().points = 60 * 24 * 30;
  metadata.archive_args.emplace_back();
  metadata.archive_args.back().precision = 3600;
  metadata.archive_args.back().points = 24 * 365;
  metadata.x_files_factor = 0.5;
  metadata.agg_method = (int32_t)AggregationMethod::Average;

  // none of these should create the series
  {
    printf("-- [%s:basic_test] update_metadata (no-op) on nonexistent series\n", store_name.c_str());
    auto ret1 = execute_update_metadata(m, s, metadata_map,
        Store::UpdateMetadataBehavior::Ignore, false, false, false, profiler.get());
    expect_eq(1, ret1.size());
    expect_eq(is_write_buffer, ret1.at(key_name1).description.empty());
    s->flush(m.get());
    expect(!isfile(key_filename1));

    auto ret2 = execute_update_metadata(m, s, metadata_map,
        Store::UpdateMetadataBehavior::Update, false, false, false, profiler.get());
    expect_eq(1, ret2.size());
    expect_eq(is_write_buffer, ret2.at(key_name1).description.empty());
    s->flush(m.get());
    expect(!isfile(key_filename1));

    auto ret3 = execute_update_metadata(m, s, metadata_map,
        Store::UpdateMetadataBehavior::Recreate, false, false, false, profiler.get());
    expect_eq(1, ret3.size());
    expect_eq(is_write_buffer, ret3.at(key_name1).description.empty());
    s->flush(m.get());
    expect(!isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] update_metadata (create) on nonexistent series\n", store_name.c_str());
    auto ret = execute_update_metadata(m, s, metadata_map,
        Store::UpdateMetadataBehavior::Ignore, true, false, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(key_name1).description.empty());
    if (is_write_buffer) {
      expect(!isfile(key_filename1));
      s->flush(m.get());
    }
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] update_metadata (no-op) on existing series\n", store_name.c_str());
    auto ret1 = execute_update_metadata(m, s, metadata_map,
        Store::UpdateMetadataBehavior::Ignore, true, false, false, profiler.get());
    expect_eq(1, ret1.size());
    expect_eq(is_write_buffer, ret1.at(key_name1).description.empty());
    s->flush(m.get());
    expect(isfile(key_filename1));

    auto ret2 = execute_update_metadata(m, s, metadata_map,
        Store::UpdateMetadataBehavior::Ignore, false, false, false, profiler.get());
    expect_eq(1, ret2.size());
    expect_eq(is_write_buffer, ret2.at(key_name1).description.empty());
    s->flush(m.get());
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] update_metadata (recreate) on existing series\n", store_name.c_str());
    auto ret = execute_update_metadata(m, s, metadata_map,
        Store::UpdateMetadataBehavior::Recreate, false, false, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(key_name1).description.empty());
    s->flush(m.get());
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] read from series with no data\n", store_name.c_str());
    auto ret = execute_read(m, s, {key_name1}, test_now - 10 * 60, test_now, false, profiler.get());
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(key_name1).size());
    expect(ret.at(key_name1).at(key_name1).error.description.empty());
    expect(ret.at(key_name1).at(key_name1).data.empty());
    expect_eq(metadata.archive_args[0].precision, ret.at(key_name1).at(key_name1).step);
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] write datapoint to existing series\n", store_name.c_str());
    auto ret = execute_write(m, s, write_data, false, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(key_name1).description.empty());
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] read from series with data\n", store_name.c_str());
    // note: we repeat this read twice since the first time is before the flush
    // (return value should be the same before and after flushing)
    for (int x = 0; x < 2; x++) {
      auto ret = execute_read(m, s, {key_name1}, test_now - 10 * 60, test_now, false, profiler.get());
      expect_eq(1, ret.size());
      expect_eq(1, ret.at(key_name1).size());
      expect(ret.at(key_name1).at(key_name1).error.description.empty());
      fprintf(stderr, ">> %zu\n", ret.at(key_name1).at(key_name1).data.size());
      expect_eq(1, ret.at(key_name1).at(key_name1).data.size());
      expect_eq((test_now / 60) * 60, ret.at(key_name1).at(key_name1).data[0].timestamp);
      expect_eq(2.0, ret.at(key_name1).at(key_name1).data[0].value);
      expect_eq(metadata.archive_args[0].precision, ret.at(key_name1).at(key_name1).step);
      expect(isfile(key_filename1));
      s->flush(m.get());
    }
  }

  {
    printf("-- [%s:basic_test] update_metadata (update) on existing series with data\n", store_name.c_str());
    metadata.x_files_factor = 1.0;
    auto ret = execute_update_metadata(m, s, metadata_map,
        Store::UpdateMetadataBehavior::Update, false, false, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(key_name1).description.empty());
    expect(isfile(key_filename1));
    s->flush(m.get());
  }

  {
    printf("-- [%s:basic_test] read from series with data after metadata update\n", store_name.c_str());
    auto ret = execute_read(m, s, {key_name1}, test_now - 10 * 60, test_now, false, profiler.get());
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(key_name1).size());
    expect(ret.at(key_name1).at(key_name1).error.description.empty());
    expect_eq(1, ret.at(key_name1).at(key_name1).data.size());
    expect_eq((test_now / 60) * 60, ret.at(key_name1).at(key_name1).data[0].timestamp);
    expect_eq(2.0, ret.at(key_name1).at(key_name1).data[0].value);
    expect_eq(metadata.archive_args[0].precision, ret.at(key_name1).at(key_name1).step);
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] update_metadata (recreate) on existing series with data\n", store_name.c_str());
    metadata.x_files_factor = 1.0;
    auto ret = execute_update_metadata(m, s, metadata_map,
        Store::UpdateMetadataBehavior::Recreate, false, false, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(key_name1).description.empty());
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] read from series with no data\n", store_name.c_str());
    auto ret = execute_read(m, s, {key_name1}, test_now - 10 * 60, test_now, false, profiler.get());
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(key_name1).size());
    expect(ret.at(key_name1).at(key_name1).error.description.empty());
    expect(ret.at(key_name1).at(key_name1).data.empty());
    expect_eq(metadata.archive_args[0].precision, ret.at(key_name1).at(key_name1).step);
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] find with directory result\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern1}, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(pattern1).error.description.empty());
    expect_eq(1, ret.at(pattern1).results.size());
    expect_eq("test.DiskStore.*", ret.at(pattern1).results[0]);
  }

  {
    printf("-- [%s:basic_test] find with non-matching directory result\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern2}, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(pattern2).error.description.empty());
    expect(ret.at(pattern2).results.empty());
  }

  {
    printf("-- [%s:basic_test] find with file result\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern4}, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(pattern4).error.description.empty());
    expect_eq(1, ret.at(pattern4).results.size());
    expect_eq(key_name1, ret.at(pattern4).results[0]);
  }

  {
    printf("-- [%s:basic_test] find with non-matching file result\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern3}, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(pattern3).error.description.empty());
    expect(ret.at(pattern3).results.empty());
  }

  {
    printf("-- [%s:basic_test] find all\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern5}, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(pattern5).error.description.empty());
    expect_eq(1, ret.at(pattern5).results.size());
    expect_eq(key_name1, ret.at(pattern5).results[0]);
  }

  {
    printf("-- [%s:basic_test] update_metadata mixed create and ignore\n", store_name.c_str());
    expect(!isfile(key_filename2));
    metadata_map[key_name2] = metadata_map.at(key_name1);
    auto ret1 = execute_update_metadata(m, s, metadata_map,
        Store::UpdateMetadataBehavior::Ignore, true, false, false, profiler.get());
    expect_eq(2, ret1.size());
    expect(!ret1.at(key_name1).description.empty());
    expect(ret1.at(key_name2).description.empty());
    expect(isfile(key_filename1));
    expect_eq(is_write_buffer, !isfile(key_filename2));
    s->flush(m.get());
    expect(isfile(key_filename2));
  }

  {
    printf("-- [%s:basic_test] find with mixed directory and file results\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern1, pattern4}, false, profiler.get());
    expect_eq(2, ret.size());

    auto& result1 = ret.at(pattern1);
    expect(result1.error.description.empty());
    expect_eq(2, result1.results.size());
    sort(result1.results.begin(), result1.results.end());
    expect_eq(pattern4, result1.results[0]);
    expect_eq(key_name2, result1.results[1]);

    auto& result4 = ret.at(pattern4);
    expect(result4.error.description.empty());
    expect_eq(1, result4.results.size());
    expect_eq(key_name1, result4.results[0]);
  }

  {
    printf("-- [%s:basic_test] find all\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern5}, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(pattern5).error.description.empty());
    auto& results = ret.at(pattern5).results;
    sort(results.begin(), results.end());
    expect_eq(2, results.size());
    expect_eq(key_name1, results[0]);
    expect_eq(key_name2, results[1]);
  }

  {
    printf("-- [%s:basic_test] read from nonexistent series (autocreate)\n", store_name.c_str());
    auto ret = execute_read(m, s, {autocreate_key_name1}, test_now - 10 * 60, test_now, false, profiler.get());
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(autocreate_key_name1).size());
    expect_eq("", ret.at(autocreate_key_name1).at(autocreate_key_name1).error.description);
    expect(ret.at(autocreate_key_name1).at(autocreate_key_name1).data.empty());
    expect_eq(ret.at(autocreate_key_name1).at(autocreate_key_name1).start_time, test_now - 10 * 60);
    expect_eq(ret.at(autocreate_key_name1).at(autocreate_key_name1).end_time, test_now);
    expect_eq(ret.at(autocreate_key_name1).at(autocreate_key_name1).step, 0);
    expect(!isfile(autocreate_key_filename1));
  }

  {
    printf("-- [%s:basic_test] write to nonexistent series (autocreate)\n", store_name.c_str());
    unordered_map<string, Series> this_write_data;
    this_write_data.emplace(autocreate_key_name1, write_data.at(key_name1));
    expect(!isfile(autocreate_key_name1));
    auto ret = execute_write(m, s, this_write_data, false, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(autocreate_key_name1).description.empty());
    expect(isfile(key_filename1));
    expect(isfile(key_filename2));
    expect_eq(is_write_buffer, !isfile(autocreate_key_filename1));
    s->flush(m.get());
    expect(isfile(autocreate_key_filename1));
    expect(!isfile(autocreate_key_filename2));
  }

  {
    printf("-- [%s:basic_test] read from series created by autocreate\n", store_name.c_str());
    auto ret = execute_read(m, s, {autocreate_key_name1}, test_now - 10 * 60, test_now, false, profiler.get());
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(autocreate_key_name1).size());
    expect(ret.at(autocreate_key_name1).at(autocreate_key_name1).error.description.empty());
    expect_eq(1, ret.at(autocreate_key_name1).at(autocreate_key_name1).data.size());
    expect_eq((test_now / 60) * 60, ret.at(autocreate_key_name1).at(autocreate_key_name1).data[0].timestamp);
    expect_eq(2.0, ret.at(autocreate_key_name1).at(autocreate_key_name1).data[0].value);
    metadata.x_files_factor = 0.0;
    expect_eq(metadata.archive_args[0].precision, ret.at(autocreate_key_name1).at(autocreate_key_name1).step);
  }

  {
    printf("-- [%s:basic_test] rename series\n", store_name.c_str());
    auto ret = execute_rename_series(m, s, {{autocreate_key_name1, rename_key_name}}, false, false, profiler.get());
    expect_eq(1, ret.size());
    expect_eq("", ret.at(autocreate_key_name1).description);
  }

  {
    printf("-- [%s:basic_test] read from renamed series\n", store_name.c_str());
    auto ret = execute_read(m, s, {rename_key_name}, test_now - 10 * 60, test_now, false, profiler.get());
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(rename_key_name).size());
    expect(ret.at(rename_key_name).at(rename_key_name).error.description.empty());
    expect_eq(1, ret.at(rename_key_name).at(rename_key_name).data.size());
    expect_eq((test_now / 60) * 60, ret.at(rename_key_name).at(rename_key_name).data[0].timestamp);
    expect_eq(2.0, ret.at(rename_key_name).at(rename_key_name).data[0].value);
    metadata.x_files_factor = 0.0;
    expect_eq(metadata.archive_args[0].precision, ret.at(rename_key_name).at(rename_key_name).step);
  }

  {
    printf("-- [%s:basic_test] find returns renamed series and not original series\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern5, autocreate_pattern, rename_pattern}, false, profiler.get());
    expect_eq(3, ret.size());
    expect(ret.at(pattern5).error.description.empty());
    expect(ret.at(autocreate_pattern).error.description.empty());
    expect(ret.at(rename_pattern).error.description.empty());

    // some other keys should exist here; we only check that the renamed key
    // appears under its new name and not its old name
    bool renamed_key_found = false;
    for (const auto& it : ret.at(pattern5).results) {
      expect_ne(autocreate_key_name1, it);
      if (it == rename_key_name) {
        renamed_key_found = true;
      }
    }
    expect(renamed_key_found);

    auto& autocreate_pattern_results = ret.at(autocreate_pattern).results;
    expect_eq(0, autocreate_pattern_results.size());

    auto& rename_pattern_results = ret.at(rename_pattern).results;
    expect_eq(1, rename_pattern_results.size());
    expect_eq(rename_key_name, rename_pattern_results[0]);
  }

  {
    printf("-- [%s:basic_test] rename series back\n", store_name.c_str());
    auto ret = execute_rename_series(m, s, {{rename_key_name, autocreate_key_name1}}, false, false, profiler.get());
    expect_eq(1, ret.size());
    expect_eq("", ret.at(rename_key_name).description);
  }

  {
    printf("-- [%s:basic_test] read_all from series created by autocreate\n", store_name.c_str());
    auto ret = execute_read_all(m, s, autocreate_key_name1, false, profiler.get());
    expect_eq("", ret.error.description);
    expect_eq(0, ret.metadata.x_files_factor);
    expect_eq(1, ret.metadata.agg_method);
    expect_eq(2, ret.metadata.archive_args.size());
    expect_eq(60, ret.metadata.archive_args[0].precision);
    expect_eq(43200, ret.metadata.archive_args[0].points);
    expect_eq(3600, ret.metadata.archive_args[1].precision);
    expect_eq(8760, ret.metadata.archive_args[1].points);
    expect_eq(1, ret.data.size());
    expect_eq(2, ret.data[0].value);
  }

  {
    printf("-- [%s:basic_test] find all\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern5}, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(pattern5).error.description.empty());
    auto& results = ret.at(pattern5).results;
    sort(results.begin(), results.end());
    expect_eq(3, results.size());
    expect_eq(key_name1, results[0]);
    expect_eq(autocreate_key_name1, results[1]);
    expect_eq(key_name2, results[2]);
  }

  {
    printf("-- [%s:basic_test] delete series\n", store_name.c_str());
    auto ret = execute_delete_series(m, s, {pattern4, autocreate_key_name1}, false, false,
        profiler.get());
    expect_eq(1, ret.at(pattern4).disk_series_deleted);
    expect_eq(1, ret.at(autocreate_key_name1).disk_series_deleted);
    expect(ret.at(pattern4).error.description.empty());
    expect(ret.at(autocreate_key_name1).error.description.empty());
    expect_eq(2, ret.size());
    expect(!isfile(key_filename1));
    expect(isfile(key_filename2));
    expect(!isfile(autocreate_key_filename1));
    expect(!isfile(autocreate_key_filename2));

    // empty directories should have been deleted too
    expect(!isdir(data_directory + "/test/DiskStore"));
    expect(!isdir(data_directory + "/test/autocreate/dir1/dir2/dir3"));
    expect(!isdir(data_directory + "/test/autocreate/dir1/dir2"));
    expect(!isdir(data_directory + "/test/autocreate/dir1"));
    expect(!isdir(data_directory + "/test/autocreate"));
    expect(isdir(data_directory + "/test"));
  }

  {
    printf("-- [%s:basic_test] find after deletion\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern1, pattern4}, false, profiler.get());
    expect(ret.at(pattern1).error.description.empty());
    expect_eq(key_name2, ret.at(pattern1).results[0]);
    expect_eq(1, ret.at(pattern1).results.size());
    expect(ret.at(pattern4).error.description.empty());
    expect(ret.at(pattern4).results.empty());
    expect_eq(2, ret.size());
  }

  {
    printf("-- [%s:basic_test] update_metadata (create) on mixed existing & nonexistent series\n", store_name.c_str());
    auto ret = execute_update_metadata(m, s, metadata_map,
        Store::UpdateMetadataBehavior::Ignore, true, false, false, profiler.get());
    expect_eq(2, ret.size());
    expect(ret.at(key_name1).description.empty());
    if (is_write_buffer) {
      expect(ret.at(key_name2).description.empty());
      expect(!isfile(key_filename1));
      expect(isfile(key_filename2));
      s->flush(m.get());
    } else {
      expect_eq("ignored", ret.at(key_name2).description);
      expect_eq(true, ret.at(key_name2).ignored);
    }
    expect(isfile(key_filename1));
    expect(isfile(key_filename2));
  }

  {
    printf("-- [%s:basic_test] update_metadata (resample)\n", store_name.c_str());

    printf("---- write\n");
    unordered_map<string, Series> write_data;
    for (size_t x = 0; x < 60 * 60 * 24; x += 300) {
      // write the past day's worth of data, five-minutely
      write_data[key_name1].emplace_back();
      write_data[key_name1].back().timestamp = test_now - x;
      write_data[key_name1].back().value = x;
    }
    execute_write(m, s, write_data, false, false, profiler.get());
    s->flush(m.get());

    printf("---- read\n");
    auto ret = execute_read(m, s, {key_name1}, test_now - 24 * 60 * 60 - 60,
        test_now, false, profiler.get());
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(key_name1).size());
    auto result = ret.at(key_name1).at(key_name1);
    expect_eq("", result.error.description);
    expect_eq(60, result.step);
    expect_eq(((test_now - 60 * 60 * 24) / 60) * 60, result.start_time);
    expect_eq(((test_now + 60) / 60) * 60, result.end_time);

    unordered_set<uint32_t> pending_timestamps;
    for (size_t x = 0; x < 60 * 60 * 24; x += 300) {
      pending_timestamps.insert(((test_now - x) / 60) * 60);
    }
    for (size_t x = 0; x < result.data.size(); x++) {
      expect(pending_timestamps.erase(result.data[x].timestamp));
      expect_eq((static_cast<int64_t>(test_now - result.data[x].value) / 60) * 60,
          result.data[x].timestamp);
    }
    expect(pending_timestamps.empty());

    printf("---- update_metadata\n");
    SeriesMetadataMap new_metadata_map;
    auto& metadata = new_metadata_map[key_name1];
    metadata.archive_args.emplace_back();
    metadata.archive_args.back().precision = 300;
    metadata.archive_args.back().points = 12 * 24 * 30;
    metadata.archive_args.emplace_back();
    metadata.archive_args.back().precision = 3600;
    metadata.archive_args.back().points = 24 * 365;
    metadata.x_files_factor = 1.0;
    metadata.agg_method = (int32_t)AggregationMethod::Average;
    execute_update_metadata(m, s, new_metadata_map,
        Store::UpdateMetadataBehavior::Update, true, false, false, profiler.get());
    s->flush(m.get());

    printf("---- read\n");
    ret = execute_read(m, s, {key_name1}, test_now - 24 * 60 * 60 - 300,
        test_now, false, profiler.get());
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(key_name1).size());
    result = ret.at(key_name1).at(key_name1);
    expect_eq("", result.error.description);
    expect_eq(300, result.step);
    expect_eq(((test_now - 60 * 60 * 24) / 300) * 300, result.start_time);
    expect_eq(((test_now + 300) / 300) * 300, result.end_time);

    for (size_t x = 0; x < 60 * 60 * 24; x += 300) {
      pending_timestamps.insert(((test_now - x) / 300) * 300);
    }
    for (size_t x = 0; x < result.data.size(); x++) {
      expect(pending_timestamps.erase(result.data[x].timestamp));
      expect_eq((static_cast<int64_t>(test_now - result.data[x].value) / 300) * 300,
          result.data[x].timestamp);
    }
    expect(pending_timestamps.empty());

    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] delete file with wildcard\n", store_name.c_str());
    auto ret = execute_delete_series(m, s, {pattern1}, false, false, profiler.get());
    expect_eq(1, ret.at(pattern1).disk_series_deleted);
    expect(ret.at(pattern1).error.description.empty());
    expect_eq(1, ret.size());
    expect(isfile(key_filename1));
    expect(!isfile(key_filename2));
    expect(!isfile(autocreate_key_filename1));
    expect(!isfile(autocreate_key_filename2));

    // empty directories should have been deleted too
    expect(isdir(data_directory + "/test/DiskStore"));
    expect(!isdir(data_directory + "/test/autocreate/dir1/dir2/dir3"));
    expect(!isdir(data_directory + "/test/autocreate/dir1/dir2"));
    expect(!isdir(data_directory + "/test/autocreate/dir1"));
    expect(!isdir(data_directory + "/test/autocreate"));
    expect(isdir(data_directory + "/test"));
  }

  {
    printf("-- [%s:basic_test] delete directory\n", store_name.c_str());
    auto ret = execute_delete_series(m, s, {pattern5}, false, false, profiler.get());
    expect_eq(1, ret.at(pattern5).disk_series_deleted);
    fprintf(stderr, "%s\n", ret.at(pattern5).error.description.c_str());
    expect(ret.at(pattern5).error.description.empty());
    expect_eq(1, ret.size());
    expect(!isfile(key_filename1));
    expect(!isfile(key_filename2));
    expect(!isfile(autocreate_key_filename1));
    expect(!isfile(autocreate_key_filename2));

    // empty directories should have been deleted too
    expect(!isdir(data_directory + "/test/DiskStore"));
    expect(!isdir(data_directory + "/test/autocreate/dir1/dir2/dir3"));
    expect(!isdir(data_directory + "/test/autocreate/dir1/dir2"));
    expect(!isdir(data_directory + "/test/autocreate/dir1"));
    expect(!isdir(data_directory + "/test/autocreate"));
    expect(!isdir(data_directory + "/test"));
  }

  {
    printf("-- [%s:basic_test] find all with no results\n", store_name.c_str());
    auto ret = execute_find(m, s, {pattern5}, false, profiler.get());
    expect_eq(1, ret.size());
    expect(ret.at(pattern5).error.description.empty());
    expect(ret.at(pattern5).results.empty());
  }
}

void run_rename_test(shared_ptr<StoreTaskManager> m, shared_ptr<Store> s,
    const string& store_name, const string& data_directory,
    bool is_write_buffer = false) {
  time_t test_now = time(NULL);
  auto profiler = create_profiler("StoreTest", "run_rename_test", 1000000000);

  for (size_t x = 0; x < 100; x++) {
    string autocreate_key_name1 = string_printf("test.autocreate.dir1.dir2.dir3.key%zu-1", x);
    string autocreate_key_name2 = string_printf("test.autocreate.dir1.dir2.dir3.key%zu-2", x);
    string rename_key_name = string_printf("test.rename.key%zu", x);
    string autocreate_key_filename1 = string_printf("%s/test/autocreate/dir1/dir2/dir3/key%zu-1.wsp", data_directory.c_str(), x);
    string autocreate_key_filename2 = string_printf("%s/test/autocreate/dir1/dir2/dir3/key%zu-2.wsp", data_directory.c_str(), x);
    string rename_key_filename = string_printf("/test/rename/key%zu.wsp", data_directory.c_str(), x);
    string pattern5 = "test.**";
    string autocreate_pattern = "test.autocreate.dir1.dir2.dir3.*";
    string rename_pattern = "test.rename.*";

    unordered_map<string, Series> write_data;
    {
      auto& data1 = write_data[autocreate_key_name1];
      data1.emplace_back();
      data1.back().timestamp = test_now;
      data1.back().value = 2.0;
      data1.emplace_back();
      data1.back().timestamp = test_now - 60;
      data1.back().value = 3.0;

      auto& data2 = write_data[autocreate_key_name2];
      data2.emplace_back();
      data2.back().timestamp = test_now - 60;
      data2.back().value = 1.0;
      data2.emplace_back();
      data2.back().timestamp = test_now - 120;
      data2.back().value = 0.0;
    }

    {
      printf("-- [%s:rename_test:%zu] rename nonexistent series\n", store_name.c_str(), x);
      auto ret = execute_rename_series(m, s, {{autocreate_key_name1, rename_key_name}}, false, false, profiler.get());
      expect_eq(1, ret.size());
      expect_ne("", ret.at(autocreate_key_name1).description);
    }

    {
      printf("-- [%s:rename_test:%zu] write to nonexistent series (autocreate)\n", store_name.c_str(), x);
      auto ret = execute_write(m, s, write_data, false, false, profiler.get());
      expect_eq(2, ret.size());
      expect(ret.at(autocreate_key_name1).description.empty());
      expect(ret.at(autocreate_key_name2).description.empty());
      expect_eq(is_write_buffer, !isfile(autocreate_key_filename1));
      expect_eq(is_write_buffer, !isfile(autocreate_key_filename2));
      s->flush(m.get());
      expect(isfile(autocreate_key_filename1));
    }

    {
      printf("-- [%s:rename_test:%zu] rename series\n", store_name.c_str(), x);
      auto ret = execute_rename_series(m, s, {{autocreate_key_name1, rename_key_name}}, false, false, profiler.get());
      expect_eq(1, ret.size());
      expect_eq("", ret.at(autocreate_key_name1).description);
    }

    {
      printf("-- [%s:rename_test:%zu] read from renamed series\n", store_name.c_str(), x);
      auto ret = execute_read(m, s, {rename_key_name}, test_now - 10 * 60, test_now, false, profiler.get());
      expect_eq(1, ret.size());
      expect_eq(1, ret.at(rename_key_name).size());
      expect(ret.at(rename_key_name).at(rename_key_name).error.description.empty());
      expect_eq(2, ret.at(rename_key_name).at(rename_key_name).data.size());
      expect_eq((test_now / 60) * 60 - 60, ret.at(rename_key_name).at(rename_key_name).data[0].timestamp);
      expect_eq(3.0, ret.at(rename_key_name).at(rename_key_name).data[0].value);
      expect_eq((test_now / 60) * 60, ret.at(rename_key_name).at(rename_key_name).data[1].timestamp);
      expect_eq(2.0, ret.at(rename_key_name).at(rename_key_name).data[1].value);
    }

    {
      printf("-- [%s:rename_test:%zu] find returns renamed series and not original series\n", store_name.c_str(), x);
      auto ret = execute_find(m, s, {pattern5, autocreate_pattern, rename_pattern}, false, profiler.get());
      expect_eq(3, ret.size());
      expect(ret.at(pattern5).error.description.empty());
      expect(ret.at(autocreate_pattern).error.description.empty());
      expect(ret.at(rename_pattern).error.description.empty());

      auto& pattern5_results = ret.at(pattern5).results;
      expect_eq(2, pattern5_results.size());
      sort(pattern5_results.begin(), pattern5_results.end());
      expect_eq(autocreate_key_name2, pattern5_results[0]);
      expect_eq(rename_key_name, pattern5_results[1]);

      auto& autocreate_pattern_results = ret.at(autocreate_pattern).results;
      expect_eq(1, autocreate_pattern_results.size());
      expect_eq(autocreate_key_name2, autocreate_pattern_results[0]);

      auto& rename_pattern_results = ret.at(rename_pattern).results;
      expect_eq(1, rename_pattern_results.size());
      expect_eq(rename_key_name, rename_pattern_results[0]);
    }

    {
      printf("-- [%s:rename_test:%zu] rename series (ignored)\n", store_name.c_str(), x);
      auto ret = execute_rename_series(m, s, {{rename_key_name, autocreate_key_name2}}, false, false, profiler.get());
      expect_eq(1, ret.size());
      expect(ret.at(rename_key_name).ignored);
    }

    {
      printf("-- [%s:rename_test:%zu] read from non-renamed series\n", store_name.c_str(), x);
      auto ret = execute_read(m, s, {rename_key_name}, test_now - 10 * 60, test_now, false, profiler.get());
      expect_eq(1, ret.size());
      expect_eq(1, ret.at(rename_key_name).size());
      expect(ret.at(rename_key_name).at(rename_key_name).error.description.empty());
      expect_eq(2, ret.at(rename_key_name).at(rename_key_name).data.size());
      expect_eq((test_now / 60) * 60 - 60, ret.at(rename_key_name).at(rename_key_name).data[0].timestamp);
      expect_eq(3.0, ret.at(rename_key_name).at(rename_key_name).data[0].value);
      expect_eq((test_now / 60) * 60, ret.at(rename_key_name).at(rename_key_name).data[1].timestamp);
      expect_eq(2.0, ret.at(rename_key_name).at(rename_key_name).data[1].value);
    }

    {
      printf("-- [%s:rename_test:%zu] rename series (merge)\n", store_name.c_str(), x);
      auto ret = execute_rename_series(m, s, {{rename_key_name, autocreate_key_name2}}, true, false, profiler.get());
      expect_eq(1, ret.size());
      expect(ret.at(rename_key_name).description.empty());
      expect(!ret.at(rename_key_name).ignored);
    }

    {
      printf("-- [%s:rename_test:%zu] read from merged series\n", store_name.c_str(), x);
      auto ret = execute_read(m, s, {autocreate_key_name2}, test_now - 10 * 60, test_now, false, profiler.get());
      expect_eq(1, ret.size());
      expect_eq(1, ret.at(autocreate_key_name2).size());
      auto& result = ret.at(autocreate_key_name2).at(autocreate_key_name2);
      expect(result.error.description.empty());
      expect_eq(3, result.data.size());
      expect_eq((test_now / 60) * 60 - 120, result.data[0].timestamp);
      expect_eq(0.0, result.data[0].value);
      expect_eq((test_now / 60) * 60 - 60, result.data[1].timestamp);
      expect_eq(3.0, result.data[1].value);
      expect_eq((test_now / 60) * 60, result.data[2].timestamp);
      expect_eq(2.0, result.data[2].value);
    }

    // deferred delete series is slow, so we only test it the first few times
    if ((x < 5) && is_write_buffer) {
      printf("-- [%s:rename_test:%zu] deferred delete series\n", store_name.c_str(), x);
      auto ret = execute_delete_series(m, s, {autocreate_key_name2}, true, false,
          profiler.get());
      expect_eq(1, ret.size());
      expect(ret.at(autocreate_key_name2).error.description.empty());
      if (is_write_buffer) {
        expect_eq(0, ret.at(autocreate_key_name2).disk_series_deleted);
        usleep(1100000);
      } else {
        expect_eq(1, ret.at(autocreate_key_name2).disk_series_deleted);
      }
      expect(!isfile(autocreate_key_filename2));

    } else {
      printf("-- [%s:rename_test:%zu] delete series\n", store_name.c_str(), x);
      auto ret = execute_delete_series(m, s, {autocreate_key_name2}, false, false,
          profiler.get());
      expect_eq(1, ret.size());
      expect(ret.at(autocreate_key_name2).error.description.empty());
      expect_eq(1, ret.at(autocreate_key_name2).disk_series_deleted);
      expect(!isfile(autocreate_key_filename2));
    }
  }
}

void reset_test_state(const string& dirname) {
  try {
    unlink(dirname, true);
  } catch (const runtime_error& e) { }
  mkdir(dirname.c_str(), 0755);

  WhisperArchive::clear_files_lru();
}

void reset_and_run_all_tests(shared_ptr<StoreTaskManager> m,
    shared_ptr<Store> s, const string& store_name, const string& data_directory,
    bool is_write_buffer = false) {
  reset_test_state(data_directory);
  run_basic_test(m, s, store_name, data_directory, is_write_buffer);
  reset_test_state(data_directory);
  run_rename_test(m, s, store_name, data_directory, is_write_buffer);
}

int main(int argc, char* argv[]) {
  int retcode = 0;
  string data_directory = "./StoreTest-data";

  SeriesMetadata autocreate_metadata;
  autocreate_metadata.archive_args.emplace_back();
  autocreate_metadata.archive_args.back().precision = 60;
  autocreate_metadata.archive_args.back().points = 60 * 24 * 30;
  autocreate_metadata.archive_args.emplace_back();
  autocreate_metadata.archive_args.back().precision = 3600;
  autocreate_metadata.archive_args.back().points = 24 * 365;
  autocreate_metadata.x_files_factor = 0.0;
  autocreate_metadata.agg_method = (int32_t)AggregationMethod::Average;
  vector<pair<string, SeriesMetadata>> autocreate_rules;
  autocreate_rules.emplace_back(make_pair("test.autocreate.**", autocreate_metadata));

  // try {
    run_internal_functions_test();

    vector<pair<shared_ptr<StoreTaskManager>, string>> task_managers({
      make_pair(shared_ptr<StoreTaskManager>(new StoreTaskManager()), "StoreTaskManager"),
    });

    for (auto& it : task_managers) {
      auto m = it.first;
      string name_prefix = it.second + "+";

      {
        shared_ptr<Store> disk_store(new DiskStore(data_directory));
        disk_store->set_autocreate_rules(autocreate_rules);
        reset_and_run_all_tests(m, disk_store, name_prefix + "DiskStore", data_directory);
      }

      {
        shared_ptr<Store> disk_store(new DiskStore(data_directory));
        shared_ptr<Store> multi_store(new MultiStore({{"store1", {disk_store, false}}}));
        multi_store->set_autocreate_rules(autocreate_rules);
        reset_and_run_all_tests(m, multi_store, name_prefix + "MultiStore(DiskStore)", data_directory);
      }

      {
        shared_ptr<Store> disk_store1(new DiskStore(data_directory));
        shared_ptr<Store> disk_store2(new DiskStore(data_directory));
        shared_ptr<Store> hash_store(new ConsistentHashMultiStore({{"store1", {disk_store1, false}}, {"store2", {disk_store2, false}}}, 18));
        hash_store->set_autocreate_rules(autocreate_rules);
        reset_and_run_all_tests(m, hash_store, name_prefix + "ConsistentHashMultiStore(DiskStore, DiskStore)", data_directory);
      }

      {
        shared_ptr<Store> disk_store(new DiskStore(data_directory));
        shared_ptr<Store> buffer_on_disk_store(new WriteBufferStore(disk_store, 0, 0, 0, 0, 0, false, true));
        buffer_on_disk_store->set_autocreate_rules(autocreate_rules);
        reset_and_run_all_tests(m, buffer_on_disk_store, name_prefix + "WriteBufferStore(DiskStore)", data_directory, true);
      }

      {
        shared_ptr<Store> cached_disk_store(new CachedDiskStore(data_directory, 1, 1));
        cached_disk_store->set_autocreate_rules(autocreate_rules);
        reset_and_run_all_tests(m, cached_disk_store, name_prefix + "CachedDiskStore", data_directory);
      }

      {
        shared_ptr<Store> cached_disk_store(new CachedDiskStore(data_directory, 1, 1));
        shared_ptr<Store> buffer_on_cached_disk_store(new WriteBufferStore(cached_disk_store, 0, 0, 0, 0, 0, false, true));
        buffer_on_cached_disk_store->set_autocreate_rules(autocreate_rules);
        reset_and_run_all_tests(m, buffer_on_cached_disk_store, name_prefix + "WriteBufferStore(CachedDiskStore)", data_directory, true);
      }

      // TODO: test RemoteStore, EmptyStore, ReadOnlyStore
    }

    printf("all tests passed\n");

  // } catch (const exception& e) {
  //   printf("failure: %s\n", e.what());
  //   retcode = 1;
  // }

  if (isdir(data_directory)) {
    unlink(data_directory, true);
  }
  return retcode;
}
