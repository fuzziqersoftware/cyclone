#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <phosg/Filesystem.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "DiskStore.hh"
#include "CachedDiskStore.hh"
#include "MultiStore.hh"
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

void run_basic_test(shared_ptr<Store> s, const string& store_name,
    const string& data_directory, bool is_write_buffer = false) {
  time_t test_now = time(NULL);

  string key_name1 = "test.DiskStore.key1";
  string key_name2 = "test.key2";
  string autocreate_key_name = "test.autocreate.dir1.dir2.dir3.key1";
  string key_filename1 = data_directory + "/test/DiskStore/key1.wsp";
  string key_filename2 = data_directory + "/test/key2.wsp";
  string autocreate_key_filename = data_directory + "/test/autocreate/dir1/dir2/dir3/key1.wsp";
  string pattern1 = "test.*";
  string pattern2 = "test.NoSuchDirectory*";
  string pattern3 = "test.DiskStore.no_such_key*";
  string pattern4 = "test.DiskStore.*";

  {
    printf("-- [%s:basic_test] read from nonexistent series\n", store_name.c_str());
    auto ret = s->read({key_name1}, test_now - 10 * 60, test_now);
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(key_name1).size());
    expect_eq("", ret.at(key_name1).at(key_name1).error);
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
    auto ret = s->write(write_data);
    expect_eq(1, ret.size());
    expect_eq(is_write_buffer, ret.at(key_name1).empty());
    s->flush();
    expect(!isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] find with no results\n", store_name.c_str());
    auto ret = s->find({pattern1});
    expect_eq(1, ret.size());
    expect(ret.at(pattern1).error.empty());
    expect(ret.at(pattern1).results.empty());
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
    auto ret1 = s->update_metadata(metadata_map, false, Store::UpdateMetadataBehavior::Ignore);
    expect_eq(1, ret1.size());
    expect_eq(is_write_buffer, ret1.at(key_name1).empty());
    s->flush();
    expect(!isfile(key_filename1));

    auto ret2 = s->update_metadata(metadata_map, false, Store::UpdateMetadataBehavior::Update);
    expect_eq(1, ret2.size());
    expect_eq(is_write_buffer, ret2.at(key_name1).empty());
    s->flush();
    expect(!isfile(key_filename1));

    auto ret3 = s->update_metadata(metadata_map, false, Store::UpdateMetadataBehavior::Recreate);
    expect_eq(1, ret3.size());
    expect_eq(is_write_buffer, ret3.at(key_name1).empty());
    s->flush();
    expect(!isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] update_metadata (create) on nonexistent series\n", store_name.c_str());
    auto ret = s->update_metadata(metadata_map, true, Store::UpdateMetadataBehavior::Ignore);
    expect_eq(1, ret.size());
    expect(ret.at(key_name1).empty());
    if (is_write_buffer) {
      expect(!isfile(key_filename1));
      s->flush();
    }
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] update_metadata (no-op) on existing series\n", store_name.c_str());
    auto ret1 = s->update_metadata(metadata_map, true, Store::UpdateMetadataBehavior::Ignore);
    expect_eq(1, ret1.size());
    expect_eq(is_write_buffer, ret1.at(key_name1).empty());
    s->flush();
    expect(isfile(key_filename1));

    auto ret2 = s->update_metadata(metadata_map, false, Store::UpdateMetadataBehavior::Ignore);
    expect_eq(1, ret2.size());
    expect_eq(is_write_buffer, ret2.at(key_name1).empty());
    s->flush();
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] update_metadata (recreate) on existing series\n", store_name.c_str());
    auto ret = s->update_metadata(metadata_map, false, Store::UpdateMetadataBehavior::Recreate);
    expect_eq(1, ret.size());
    expect(ret.at(key_name1).empty());
    s->flush();
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] read from series with no data\n", store_name.c_str());
    auto ret = s->read({key_name1}, test_now - 10 * 60, test_now);
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(key_name1).size());
    expect(ret.at(key_name1).at(key_name1).error.empty());
    expect(ret.at(key_name1).at(key_name1).data.empty());
    fprintf(stderr, "%" PRIu64 " %" PRIu64 "\n", metadata.archive_args[0].precision, ret.at(key_name1).at(key_name1).step);
    expect_eq(metadata.archive_args[0].precision, ret.at(key_name1).at(key_name1).step);
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] write datapoint to existing series\n", store_name.c_str());
    auto ret = s->write(write_data);
    expect_eq(1, ret.size());
    expect(ret.at(key_name1).empty());
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] read from series with data\n", store_name.c_str());
    // note: we repeat this read twice since the first time is before the flush
    // (return value should be the same before and after flushing)
    for (int x = 0; x < 2; x++) {
      auto ret = s->read({key_name1}, test_now - 10 * 60, test_now);
      expect_eq(1, ret.size());
      expect_eq(1, ret.at(key_name1).size());
      expect(ret.at(key_name1).at(key_name1).error.empty());
      expect_eq(1, ret.at(key_name1).at(key_name1).data.size());
      expect_eq((test_now / 60) * 60, ret.at(key_name1).at(key_name1).data[0].timestamp);
      expect_eq(2.0, ret.at(key_name1).at(key_name1).data[0].value);
      expect_eq(metadata.archive_args[0].precision, ret.at(key_name1).at(key_name1).step);
      expect(isfile(key_filename1));
      s->flush();
    }
  }

  {
    printf("-- [%s:basic_test] update_metadata (update) on existing series with data\n", store_name.c_str());
    metadata.x_files_factor = 1.0;
    auto ret = s->update_metadata(metadata_map, false, Store::UpdateMetadataBehavior::Update);
    expect_eq(1, ret.size());
    expect(ret.at(key_name1).empty());
    expect(isfile(key_filename1));
    s->flush();
  }

  {
    printf("-- [%s:basic_test] read from series with data after metadata update\n", store_name.c_str());
    auto ret = s->read({key_name1}, test_now - 10 * 60, test_now);
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(key_name1).size());
    expect(ret.at(key_name1).at(key_name1).error.empty());
    expect_eq(1, ret.at(key_name1).at(key_name1).data.size());
    expect_eq((test_now / 60) * 60, ret.at(key_name1).at(key_name1).data[0].timestamp);
    expect_eq(2.0, ret.at(key_name1).at(key_name1).data[0].value);
    expect_eq(metadata.archive_args[0].precision, ret.at(key_name1).at(key_name1).step);
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] update_metadata (recreate) on existing series with data\n", store_name.c_str());
    metadata.x_files_factor = 1.0;
    auto ret = s->update_metadata(metadata_map, false, Store::UpdateMetadataBehavior::Recreate);
    expect_eq(1, ret.size());
    expect(ret.at(key_name1).empty());
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] read from series with no data\n", store_name.c_str());
    auto ret = s->read({key_name1}, test_now - 10 * 60, test_now);
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(key_name1).size());
    expect(ret.at(key_name1).at(key_name1).error.empty());
    expect(ret.at(key_name1).at(key_name1).data.empty());
    expect_eq(metadata.archive_args[0].precision, ret.at(key_name1).at(key_name1).step);
    expect(isfile(key_filename1));
  }

  {
    printf("-- [%s:basic_test] find with directory result\n", store_name.c_str());
    auto ret = s->find({pattern1});
    expect_eq(1, ret.size());
    expect(ret.at(pattern1).error.empty());
    expect_eq(1, ret.at(pattern1).results.size());
    expect_eq("test.DiskStore.*", ret.at(pattern1).results[0]);
  }

  {
    printf("-- [%s:basic_test] find with non-matching directory result\n", store_name.c_str());
    auto ret = s->find({pattern2});
    expect_eq(1, ret.size());
    expect(ret.at(pattern2).error.empty());
    expect(ret.at(pattern2).results.empty());
  }

  {
    printf("-- [%s:basic_test] find with file result\n", store_name.c_str());
    auto ret = s->find({pattern4});
    expect_eq(1, ret.size());
    expect(ret.at(pattern4).error.empty());
    expect_eq(1, ret.at(pattern4).results.size());
    expect_eq("test.DiskStore.key1", ret.at(pattern4).results[0]);
  }

  {
    printf("-- [%s:basic_test] find with non-matching file result\n", store_name.c_str());
    auto ret = s->find({pattern3});
    expect_eq(1, ret.size());
    expect(ret.at(pattern3).error.empty());
    expect(ret.at(pattern3).results.empty());
  }

  {
    printf("-- [%s:basic_test] update_metadata mixed create and ignore\n", store_name.c_str());
    expect(!isfile(key_filename2));
    metadata_map[key_name2] = metadata_map.at(key_name1);
    auto ret1 = s->update_metadata(metadata_map, true, Store::UpdateMetadataBehavior::Ignore);
    expect_eq(2, ret1.size());
    expect(!ret1.at(key_name1).empty());
    expect(ret1.at(key_name2).empty());
    expect(isfile(key_filename1));
    expect_eq(is_write_buffer, !isfile(key_filename2));
    s->flush();
    expect(isfile(key_filename2));
  }

  {
    printf("-- [%s:basic_test] find with mixed directory and file results\n", store_name.c_str());
    auto ret = s->find({pattern1, pattern4});
    expect_eq(2, ret.size());

    auto& result1 = ret.at(pattern1);
    expect(result1.error.empty());
    expect_eq(2, result1.results.size());
    sort(result1.results.begin(), result1.results.end());
    expect_eq(pattern4, result1.results[0]);
    expect_eq(key_name2, result1.results[1]);

    auto& result4 = ret.at(pattern4);
    expect(result4.error.empty());
    expect_eq(1, result4.results.size());
    expect_eq(key_name1, result4.results[0]);
  }

  {
    printf("-- [%s:basic_test] read from nonexistent series (autocreate)\n", store_name.c_str());
    auto ret = s->read({autocreate_key_name}, test_now - 10 * 60, test_now);
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(autocreate_key_name).size());
    expect_eq("", ret.at(autocreate_key_name).at(autocreate_key_name).error);
    expect(ret.at(autocreate_key_name).at(autocreate_key_name).data.empty());
    expect_eq(ret.at(autocreate_key_name).at(autocreate_key_name).start_time, test_now - 10 * 60);
    expect_eq(ret.at(autocreate_key_name).at(autocreate_key_name).end_time, test_now);
    expect_eq(ret.at(autocreate_key_name).at(autocreate_key_name).step, 0);
  }

  {
    printf("-- [%s:basic_test] write to nonexistent series (autocreate)\n", store_name.c_str());
    unordered_map<string, Series> this_write_data;
    this_write_data.emplace(autocreate_key_name, write_data.at(key_name1));
    expect(!isfile(autocreate_key_name));
    auto ret = s->write(this_write_data);
    expect_eq(1, ret.size());
    expect(ret.at(autocreate_key_name).empty());
    expect(isfile(key_filename1));
    expect(isfile(key_filename2));
    expect_eq(is_write_buffer, !isfile(autocreate_key_filename));
    s->flush();
    expect(isfile(autocreate_key_filename));
  }

  {
    printf("-- [%s:basic_test] read from series created by autocreate\n", store_name.c_str());
    auto ret = s->read({autocreate_key_name}, test_now - 10 * 60, test_now);
    expect_eq(1, ret.size());
    expect_eq(1, ret.at(autocreate_key_name).size());
    expect(ret.at(autocreate_key_name).at(autocreate_key_name).error.empty());
    expect_eq(1, ret.at(autocreate_key_name).at(autocreate_key_name).data.size());
    expect_eq((test_now / 60) * 60, ret.at(autocreate_key_name).at(autocreate_key_name).data[0].timestamp);
    expect_eq(2.0, ret.at(autocreate_key_name).at(autocreate_key_name).data[0].value);
    metadata.x_files_factor = 0.0;
    expect_eq(metadata.archive_args[0].precision, ret.at(autocreate_key_name).at(autocreate_key_name).step);
  }

  {
    printf("-- [%s:basic_test] delete series\n", store_name.c_str());
    auto ret = s->delete_series({key_name1, autocreate_key_name});
    expect(ret.at(key_name1).empty());
    expect(ret.at(autocreate_key_name).empty());
    expect_eq(2, ret.size());
    expect(!isfile(key_filename1));
    expect(isfile(key_filename2));
    expect(!isfile(autocreate_key_filename));

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
    auto ret = s->find({pattern1, pattern4});
    expect(ret.at(pattern1).error.empty());
    expect_eq(key_name2, ret.at(pattern1).results[0]);
    expect_eq(1, ret.at(pattern1).results.size());
    expect(ret.at(pattern4).error.empty());
    expect(ret.at(pattern4).results.empty());
    expect_eq(2, ret.size());
  }
}

void recreate_directory(const string& dirname) {
  try {
    unlink(dirname, true);
  } catch (const runtime_error& e) { }
  mkdir(dirname.c_str(), 0755);
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

  try {
    run_internal_functions_test();

    recreate_directory(data_directory);
    shared_ptr<Store> disk_store(new DiskStore(data_directory));
    disk_store->set_autocreate_rules(autocreate_rules);
    run_basic_test(disk_store, "DiskStore", data_directory);

    recreate_directory(data_directory);
    shared_ptr<Store> multi_store_basic(new MultiStore({{"store1", disk_store}}));
    run_basic_test(multi_store_basic, "MultiStore(DiskStore)", data_directory);
    // TODO: test more complex MultiStores

    recreate_directory(data_directory);
    shared_ptr<Store> buffer_on_disk_store(new WriteBufferStore(disk_store, 0, 0));
    run_basic_test(buffer_on_disk_store, "WriteBufferStore(DiskStore)", data_directory, true);

    // note: order is important here. the buffer_on_disk_store test will fail if
    // it runs after any of the the cached_disk_store tests because the
    // cached_disk_store leaves files open.

    recreate_directory(data_directory);
    shared_ptr<Store> cached_disk_store(new CachedDiskStore(data_directory));
    cached_disk_store->set_autocreate_rules(autocreate_rules);
    run_basic_test(cached_disk_store, "CachedDiskStore", data_directory);

    // have to recreate the CachedDiskStore for this test because it will
    // remember test.key2
    recreate_directory(data_directory);
    cached_disk_store.reset(new CachedDiskStore(data_directory));
    cached_disk_store->set_autocreate_rules(autocreate_rules);
    shared_ptr<Store> buffer_on_cached_disk_store(new WriteBufferStore(cached_disk_store, 0, 0));
    run_basic_test(buffer_on_cached_disk_store, "WriteBufferStore(CachedDiskStore)", data_directory, true);

    // TODO: test RemoteStore, EmptyStore

    printf("all tests passed\n");
  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }

  if (isdir(data_directory)) {
    //unlink(data_directory, true);
  }
  return retcode;
}
