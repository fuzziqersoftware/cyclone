#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <algorithm>
#include <phosg/Filesystem.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "Whisper.hh"

using namespace std;


int main(int argc, char* argv[]) {

  int retcode = 0;

  time_t test_now = time(NULL);

  string filename = "WhisperTestArchive.wsp";
  try {
    unlink(filename);
  } catch (const runtime_error& e) { }

  try {
    {
      printf("-- try to open missing archive\n");
      try {
        WhisperArchive a(filename);
        expect(false);
      } catch (const cannot_open_file& e) {
        expect_eq(ENOENT, e.error);
      }
    }

    {
      printf("-- create archive\n");
      vector<ArchiveArg> archive_args;
      archive_args.emplace_back();
      archive_args.back().precision = 60;
      archive_args.back().points = 60 * 24 * 90;
      archive_args.emplace_back();
      archive_args.back().precision = 60 * 60;
      archive_args.back().points = 24 * 365 * 10;
      WhisperArchive a(filename, archive_args, 0.5, AggregationMethod::Average);
    }

    {
      printf("-- read from empty archive\n");
      WhisperArchive a(filename);
      auto metadata = a.get_metadata();
      expect_eq(2, metadata->num_archives);
      expect_eq(60, metadata->archives[0].seconds_per_point);
      expect_eq(60 * 24 * 90, metadata->archives[0].points);
      expect_eq(60 * 60, metadata->archives[1].seconds_per_point);
      expect_eq(24 * 365 * 10, metadata->archives[1].points);
      expect_eq(0.5, metadata->x_files_factor);
      expect_eq(AggregationMethod::Average, metadata->aggregation_method);

      auto x = a.read(test_now - 10 * 60, test_now);
      // step should be valid because the series exists
      expect_eq(60, x.step);
      expect(x.data.empty());
    }

    {
      printf("-- write to archive, read from same object\n");
      WhisperArchive a(filename);
      Series data;
      data.emplace_back();
      data.back().timestamp = test_now;
      data.back().value = 1.0;
      a.write(data);

      auto x = a.read(test_now - 10 * 60, test_now);
      expect_eq(1, x.data.size());
      // whisper shifts the intervals forward by 1
      expect_eq(((test_now / 60) - 9) * 60, x.start_time);
      expect_eq(((test_now / 60) + 1) * 60, x.end_time);
      expect_eq(60, x.step);
      expect_eq((test_now / 60) * 60, x.data[0].timestamp);
      expect_eq(1.0, x.data[0].value);
    }

    {
      printf("-- read from nonempty archive (new object)\n");
      WhisperArchive a(filename);
      auto metadata = a.get_metadata();
      expect_eq(2, metadata->num_archives);
      expect_eq(60, metadata->archives[0].seconds_per_point);
      expect_eq(60 * 24 * 90, metadata->archives[0].points);
      expect_eq(60 * 60, metadata->archives[1].seconds_per_point);
      expect_eq(24 * 365 * 10, metadata->archives[1].points);
      expect_eq(0.5, metadata->x_files_factor);
      expect_eq(AggregationMethod::Average, metadata->aggregation_method);

      auto x = a.read(test_now - 10 * 60, test_now);
      expect_eq(1, x.data.size());
      expect_eq(((test_now / 60) - 9) * 60, x.start_time);
      expect_eq(((test_now / 60) + 1) * 60, x.end_time);
      expect_eq(60, x.step);
      expect_eq((test_now / 60) * 60, x.data[0].timestamp);
      expect_eq(1.0, x.data[0].value);
    }

    {
      Series data;
      for (time_t x = 0; x < 90; x++) {
        data.emplace_back();
        data.back().timestamp = test_now - x * 60;
        data.back().value = x;
      }

      {
        printf("-- write historical data\n");
        WhisperArchive a(filename);
        a.write(data);
      }

      {
        printf("-- read historical data\n");
        WhisperArchive a(filename);
        auto read_data = a.read(test_now - 100 * 60, test_now + 60);

        // the data read will be in increasing order of timestamp, and the
        // timestamp resolution is the archive resolution
        WhisperArchive::ReadResult expected_data;
        expected_data.data = data;
        for (auto& it : expected_data.data) {
          it.timestamp = (it.timestamp / 60) * 60;
        }
        sort(expected_data.data.begin(), expected_data.data.end(), [](const Datapoint& a, const Datapoint& b) {
          return a.timestamp < b.timestamp;
        });
        expected_data.step = 60;
        expected_data.start_time = ((test_now / 60) - 99) * 60;
        expected_data.end_time = ((test_now / 60) + 1) * 60;
        expect_eq(expected_data, read_data);
      }
    }

    // TODO: test propagation and reading/writing older archives

    {
      printf("-- truncate archive\n");
      WhisperArchive(filename).truncate();
    }

    {
      printf("-- read from empty archive\n");
      WhisperArchive a(filename);
      auto metadata = a.get_metadata();
      expect_eq(2, metadata->num_archives);
      expect_eq(60, metadata->archives[0].seconds_per_point);
      expect_eq(60 * 24 * 90, metadata->archives[0].points);
      expect_eq(60 * 60, metadata->archives[1].seconds_per_point);
      expect_eq(24 * 365 * 10, metadata->archives[1].points);
      expect_eq(0.5, metadata->x_files_factor);
      expect_eq(AggregationMethod::Average, metadata->aggregation_method);

      auto x = a.read(test_now - 10 * 60, test_now);
      expect_eq(60, x.step);
      expect(x.data.empty());
    }

    printf("all tests passed\n");
  } catch (const out_of_range& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }

  unlink(filename);
  return retcode;
}
