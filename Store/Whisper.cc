#include "Whisper.hh"

#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include <phosg/Encoding.hh>
#include <phosg/Filesystem.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>

#include "../gen-cpp/Cyclone.h"

using namespace std;


static int64_t parse_time_length(const string& s, int64_t default_unit_factor = 1) {

  int64_t negative = 1;
  int64_t num_units = 0;

  unsigned offset = 0;
  if (s[offset] == '-') {
    negative = -1;
    offset++;
  }

  for (; offset < s.size() && isdigit(s[offset]); offset++) {
    num_units = (num_units * 10) + (s[offset] - '0');
  }

  if (offset < s.size()) {
    switch (s[offset]) {
      case 'y':
        return negative * num_units * (60 * 60 * 24 * 365);
      case 'w':
        return negative * num_units * (60 * 60 * 24 * 7);
      case 'd':
        return negative * num_units * (60 * 60 * 24);
      case 'h':
        return negative * num_units * (60 * 60);
      case 'm':
        return negative * num_units * 60;
      case 's':
        return negative * num_units;
    }
  }
  return num_units * default_unit_factor;
}


WhisperArchive::ReadResult::ReadResult() : start_time(0), end_time(0), step(0) { }

bool WhisperArchive::ReadResult::operator==(const ReadResult& other) const {
  return (this->start_time == other.start_time) &&
         (this->end_time == other.end_time) &&
         (this->step == other.step) &&
         (this->data == other.data);
}

bool WhisperArchive::ReadResult::operator!=(const ReadResult& other) const {
  return !this->operator==(other);
}

string WhisperArchive::ReadResult::str() const {
  string s = string_printf("WhisperArchive::ReadResult(start_time=%" PRIu64 ", end_time=%" PRIu64
      ", step=%" PRIu64 ", data=[", this->start_time, this->end_time, this->step);
  for (const auto& dp : this->data) {
    s += string_printf("(%g, %" PRIu64 "),", dp.value, dp.timestamp);
  }
  s += "])";
  return s;
}


WhisperArchive::WhisperArchive(const string& filename) : filename(filename) {
  auto lease = WhisperArchive::file_cache.lease(filename, 0);

  // guess: most files have <= 10 archives (we'll read more later if needed)
  // because this is a guess, we can't use preadx - it's ok to read fewer bytes
  size_t archives_read = 10;
  vector<uint8_t> data(sizeof(FileHeader) + archives_read * sizeof(ArchiveMetadata));
  ssize_t bytes_read = pread(lease.fd, data.data(), data.size(), 0);
  if (bytes_read < static_cast<ssize_t>(sizeof(FileHeader))) {
    throw runtime_error("can\'t read header for " + this->filename);
  }
  archives_read = (bytes_read - sizeof(FileHeader)) / sizeof(ArchiveMetadata);

  FileHeader* header = (FileHeader*)data.data();
  uint32_t archive_count = bswap32(header->archive_count);
  this->base_intervals.resize(archive_count, -1);

  // fill in the base metadata structure
  this->metadata = shared_ptr<Metadata>((Metadata*)malloc(sizeof(Metadata) + archive_count * sizeof(ArchiveMetadata)), free);
  this->metadata->aggregation_method = (AggregationMethod)bswap32(header->aggregation_type);
  this->metadata->max_retention = bswap32(header->max_retention);
  this->metadata->x_files_factor = bswap32f(header->x_files_factor);
  this->metadata->num_archives = archive_count;

  // if there were more than 10 archives, read the rest of their headers
  if (this->metadata->num_archives > archives_read) {
    data.resize(sizeof(FileHeader) + this->metadata->num_archives * sizeof(ArchiveMetadata));
    off_t off = sizeof(FileHeader) + archives_read * sizeof(ArchiveMetadata);
    size_t size = (this->metadata->num_archives - archives_read) * sizeof(ArchiveMetadata);
    preadx(lease.fd, &data[off], size, off);
  }

  // convert the archive headers into a usable format
  for (uint32_t x = 0; x < archive_count; x++) {
    ArchiveMetadata* sw_archive_metadata = (ArchiveMetadata*)&data[sizeof(FileHeader) + x * sizeof(ArchiveMetadata)];
    this->metadata->archives[x].offset = bswap32(sw_archive_metadata->offset);
    this->metadata->archives[x].seconds_per_point = bswap32(sw_archive_metadata->seconds_per_point);
    this->metadata->archives[x].points = bswap32(sw_archive_metadata->points);
  }

  // if there were fewer than 10 archives, then we also got the base interval
  // for the first archive in the initial read; might as well populate it
  // TODO: re-enable this later
  if (this->metadata->num_archives && (this->metadata->num_archives < 10)) {
    for (uint32_t x = 0; x < this->metadata->num_archives; x++) {
      if (this->metadata->archives[x].offset <= data.size() - sizeof(FilePoint)) {
        const FilePoint* first_pt = (const FilePoint*)(&data[this->metadata->archives[x].offset]);
        this->base_intervals[x] = bswap32(first_pt->time);
      }
    }
  }
}

WhisperArchive::WhisperArchive(const string& filename,
    const vector<ArchiveArg>& archive_args, float x_files_factor,
    uint32_t agg_method) : filename(filename) {
  this->update_metadata(archive_args, x_files_factor, agg_method, true);
}

WhisperArchive::WhisperArchive(const string& filename, const string& archive_args,
    float x_files_factor, uint32_t agg_method)
    : WhisperArchive(filename, WhisperArchive::parse_archive_args(archive_args),
        x_files_factor, agg_method) { }

WhisperArchive::~WhisperArchive() {
  WhisperArchive::file_cache.close(this->filename);
}

shared_ptr<const WhisperArchive::Metadata> WhisperArchive::get_metadata() const {
  return this->metadata;
}


vector<ArchiveArg> WhisperArchive::parse_archive_args(const string& s) {
  vector<ArchiveArg> ret;

  for (const auto& it : split(s, ',')) {
    vector<string> params = split(it, ':');
    if (params.size() != 2) {
      throw runtime_error("invalid archive definition: " + it);
    }

    ArchiveArg a;
    a.precision = parse_time_length(params[0]);
    a.points = parse_time_length(params[1], a.precision) / a.precision;
    ret.push_back(a);
  }

  return ret;
}

void WhisperArchive::validate_archive_args(const vector<ArchiveArg>& args) {
  // make sure the archive_args are valid
  if (args.empty()) {
    throw invalid_argument("no archives present");
  }

  int32_t previous_precision = 0;
  int32_t previous_points = 0;
  for (size_t x = 0; x < args.size(); x++) {
    const auto& arg = args[x];

    if (arg.precision <= 0) {
      throw invalid_argument(string_printf("archive %zu has a precision of zero or less", x));
    }
    if (arg.precision == previous_precision) {
      throw invalid_argument(string_printf("archive %zu has the same precision as a previous archive", x));
    }

    if (previous_precision) {
      if (previous_precision && arg.precision < previous_precision) {
        throw invalid_argument(string_printf("archive %zu is out of order", x));
      }
      if (previous_precision && (arg.precision % previous_precision != 0)) {
        throw invalid_argument(string_printf("archive %zu does not divide higher precisions", x));
      }
      if (previous_precision * previous_points >= arg.precision * arg.points) {
        throw invalid_argument(string_printf("archive %zu covers shorter time than higher precisions", x));
      }
      if (previous_points < arg.precision / previous_precision) {
        throw invalid_argument(string_printf("archive %zu can\'t consolidate higher precisions", x));
      }
    }
    previous_precision = arg.precision;
    previous_points = arg.points;
  }
}

void WhisperArchive::print(FILE* stream, bool print_data) {
  fprintf(stream, "WhisperArchive[%s, agg_method=%d, max_retention=%d, x_files_factor=%g, [\n",
      this->filename.c_str(), this->metadata->aggregation_method,
      this->metadata->max_retention, this->metadata->x_files_factor);

  if (print_data) {
    auto file = fopen_unique(filename.c_str(), "rb");

    for (uint32_t x = 0; x < this->metadata->num_archives; x++) {
      auto& archive = this->metadata->archives[x];
      fprintf(stream, "  Archive[offset=%d, seconds_per_point=%d, points=%d,\n",
        archive.offset, archive.seconds_per_point, archive.points);

      fseek(file.get(), archive.offset, SEEK_SET);
      for (uint32_t y = 0; y < archive.points; y++) {
        FilePoint point;
        fread(&point, sizeof(FilePoint), 1, file.get());
        if (point.time == 0) {
          continue;
        }
        fprintf(stream, "    Point[time=%d, value=%lg]\n", bswap32(point.time),
            bswap64f(point.value));
      }
    }

  } else {
    for (uint32_t x = 0; x < this->metadata->num_archives; x++) {
      auto& archive = this->metadata->archives[x];
      fprintf(stream, "  Archive[offset=%d, seconds_per_point=%d, points=%d]\n",
          archive.offset, archive.seconds_per_point, archive.points);
    }
  }
  fprintf(stream, "]\n");
}

WhisperArchive::ReadResult WhisperArchive::read(uint64_t start_time, uint64_t end_time) {

  if (start_time > end_time) {
    throw invalid_argument("invalid time interval");
  }

  uint32_t now = time(NULL);

  // make sure the range covers even part of this database file
  uint32_t oldest_time = now - this->metadata->max_retention;
  if (start_time > now) {
    return ReadResult();
  }
  if (end_time < oldest_time) {
    return ReadResult();
  }

  // make sure the entire range is within the scope of this db file
  if (start_time < oldest_time) {
    start_time = oldest_time;
  }
  if (end_time > now) {
    end_time = now;
  }

  uint32_t diff = now - start_time;
  uint32_t archive_index;
  for (archive_index = 0; archive_index < this->metadata->num_archives; archive_index++) {
    const auto& archive = this->metadata->archives[archive_index];
    if (archive.points * archive.seconds_per_point >= diff) {
      break;
    }
  }
  if (archive_index >= this->metadata->num_archives) {
    // no archive applies to this query
    return ReadResult();
  }

  const auto& archive = this->metadata->archives[archive_index];
  uint64_t start_interval = start_time - (start_time % archive.seconds_per_point) + archive.seconds_per_point;
  uint64_t end_interval = end_time - (end_time % archive.seconds_per_point) + archive.seconds_per_point;

  auto lease = WhisperArchive::file_cache.lease(this->filename, 0);

  // find out where to begin & end
  uint64_t archive_start_time = this->get_base_interval(lease.fd, archive_index);
  if (archive_start_time == 0) {
    // archive is blank
    // TODO: we should read from multiple archives in this case
    ReadResult ret;
    ret.step = archive.seconds_per_point;
    return ret;
  }

  uint32_t start_offset, end_offset;
  uint32_t archive_size = archive.points * sizeof(FilePoint);
  if (start_interval >= archive_start_time) {
    start_offset = archive.offset + ((((start_interval - archive_start_time) / archive.seconds_per_point) * sizeof(FilePoint)) % archive_size);
  } else {
    start_offset = archive.offset + (((archive.points - (archive_start_time - start_interval) / archive.seconds_per_point) * sizeof(FilePoint)) % archive_size);
  }

  if (end_interval >= archive_start_time) {
    end_offset = archive.offset + ((((end_interval - archive_start_time) / archive.seconds_per_point) * sizeof(FilePoint)) % archive_size);
  } else {
    end_offset = archive.offset + (((archive.points - (archive_start_time - end_interval) / archive.seconds_per_point) * sizeof(FilePoint)) % archive_size);
  }

  // read all points in the covered area
  uint32_t num_points;
  unique_ptr<FilePoint[]> raw_points;
  if (start_offset < end_offset) {
    num_points = (end_offset - start_offset) / sizeof(FilePoint);
    raw_points.reset(new FilePoint[num_points]);
    preadx(lease.fd, raw_points.get(), sizeof(FilePoint) * num_points, start_offset);

  } else {
    uint32_t num_points_first = archive.points - (start_offset - archive.offset) / sizeof(FilePoint);
    uint32_t num_points_second = (end_offset - archive.offset) / sizeof(FilePoint);
    num_points = num_points_first + num_points_second;

    raw_points.reset(new FilePoint[num_points]);
    preadx(lease.fd, raw_points.get(), sizeof(FilePoint) * num_points_first,
        start_offset);
    preadx(lease.fd, &(raw_points.get()[num_points_first]),
        sizeof(FilePoint) * num_points_second, archive.offset);
  }

  ReadResult ret;
  ret.start_time = start_interval;
  ret.end_time = end_interval;
  ret.step = archive.seconds_per_point;

  uint64_t current_interval = start_interval;
  for (uint32_t x = 0; x < num_points; x++) {
    uint32_t point_time = bswap32(raw_points[x].time);
    if (current_interval == point_time) {
      ret.data.emplace_back();
      auto& point = ret.data.back();
      point.timestamp = current_interval;
      point.value = bswap64f(raw_points[x].value);
    }
    current_interval += archive.seconds_per_point;
  }
  return ret;
}

void WhisperArchive::write(const Series& data) {
  // assumption: points are provided in decreasing order of time
  int64_t t = time(NULL);
  uint32_t archive_index = 0;
  uint32_t next_commit_point = 0;

  // TODO: wtf is going on here? after a create, this open call is super slow,
  // but subsequent open calls are fast
  auto lease = WhisperArchive::file_cache.lease(this->filename, 0);

  uint32_t x;
  for (x = 0; (x < data.size()) && (archive_index < this->metadata->num_archives); x++) {
    int64_t pt_age = t - static_cast<int64_t>(data.at(x).timestamp);

    // while we can't fit any more points into the current archive, commit
    while (archive_index < this->metadata->num_archives &&
        (this->metadata->archives[archive_index].points * this->metadata->archives[archive_index].seconds_per_point) < pt_age) {
      if (next_commit_point != x) {
        this->write_archive(lease.fd, archive_index, data, next_commit_point, x);
        next_commit_point = x;
      }
      archive_index++;
    }
  }

  if (archive_index < this->metadata->num_archives) {
    this->write_archive(lease.fd, archive_index, data, next_commit_point, x);
  }
}

void WhisperArchive::truncate() {
  auto lease = WhisperArchive::file_cache.lease(filename);
  this->create_file(lease.fd);
}

void WhisperArchive::update_metadata(const vector<ArchiveArg>& archive_args,
    float x_files_factor, uint32_t agg_method, bool truncate) {
  if (truncate) {
    this->validate_archive_args(archive_args);

    if (x_files_factor < 0 || x_files_factor > 1) {
      x_files_factor = 0;
    }

    // create metadata
    this->metadata = shared_ptr<Metadata>((Metadata*)malloc(sizeof(Metadata) + archive_args.size() * sizeof(ArchiveMetadata)), free);

    const auto& last_archive_args = archive_args.back();
    this->metadata->aggregation_method = (AggregationMethod)agg_method;
    this->metadata->max_retention = last_archive_args.precision * last_archive_args.points;
    this->metadata->x_files_factor = x_files_factor;
    this->metadata->num_archives = archive_args.size();

    int64_t offset = sizeof(FileHeader) + this->metadata->num_archives * sizeof(FileArchiveHeader);
    for (size_t x = 0; x < archive_args.size(); x++) {
      this->metadata->archives[x].offset = offset;
      this->metadata->archives[x].seconds_per_point = archive_args[x].precision;
      this->metadata->archives[x].points = archive_args[x].points;
      offset += archive_args[x].points * sizeof(FilePoint);
    }

    auto lease = WhisperArchive::file_cache.lease(filename);
    this->create_file(lease.fd);

  } else {
    // TODO: support changing archive_args (this will probably be very tedious)
    // for now, archive_args must either be empty (meaning no change) or exactly
    // match the existing archive config
    if (!archive_args.empty()) {
      if (archive_args.size() != this->metadata->num_archives) {
        throw runtime_error("can\'t change archive config for existing files");
      }
      const ArchiveMetadata* this_archives = &this->metadata->archives[0];
      const ArchiveArg* those_archives = archive_args.data();
      for (size_t x = 0; x < this->metadata->num_archives; x++) {
        if ((this_archives[x].seconds_per_point != static_cast<uint32_t>(those_archives[x].precision)) ||
            (this_archives[x].points != static_cast<uint32_t>(those_archives[x].points))) {
          throw runtime_error("can\'t change archive config for existing files");
        }
      }
    }

    bool should_write_header = false;
    if (x_files_factor != this->metadata->x_files_factor) {
      this->metadata->x_files_factor = x_files_factor;
      should_write_header = true;
    }
    if (agg_method != this->metadata->aggregation_method) {
      this->metadata->aggregation_method = (AggregationMethod)agg_method;
      should_write_header = true;
    }

    if (should_write_header) {
      auto lease = WhisperArchive::file_cache.lease(filename, 0);
      this->write_header(lease.fd);
    }
  }
}

size_t WhisperArchive::get_file_size() const {
  const ArchiveMetadata* last_archive = &this->metadata->archives[this->metadata->num_archives - 1];
  return last_archive->offset + sizeof(FilePoint) * last_archive->points;
}

void WhisperArchive::create_file(int fd) {
  ftruncate(fd, 0);
  this->write_header(fd);
  ftruncate(fd, this->get_file_size());

  this->base_intervals.clear();
  this->base_intervals.resize(this->metadata->num_archives, -1);
}

void WhisperArchive::write_header(int fd) {
  vector<uint8_t> data(sizeof(FileHeader) + this->metadata->num_archives * sizeof(FileArchiveHeader));

  FileHeader* sw_header = (FileHeader*)data.data();
  sw_header->aggregation_type = bswap32(this->metadata->aggregation_method);
  sw_header->max_retention = bswap32(this->metadata->max_retention);
  sw_header->x_files_factor = bswap32f(this->metadata->x_files_factor);
  sw_header->archive_count = bswap32(this->metadata->num_archives);

  FileArchiveHeader* sw_archive_header = (FileArchiveHeader*)(data.data() + sizeof(FileHeader));
  for (uint32_t x = 0; x < this->metadata->num_archives; x++) {
    sw_archive_header->offset = bswap32(this->metadata->archives[x].offset);
    sw_archive_header->seconds_per_point = bswap32(this->metadata->archives[x].seconds_per_point);
    sw_archive_header->points = bswap32(this->metadata->archives[x].points);
    sw_archive_header++;
  }

  pwritex(fd, data.data(), data.size(), 0);
}

uint32_t WhisperArchive::get_base_interval(int fd, uint32_t archive_index) {
  int64_t base_interval = this->base_intervals[archive_index];
  if (base_interval == -1) {
    FilePoint first_point;
    preadx(fd, &first_point, sizeof(first_point),
        this->metadata->archives[archive_index].offset);
    base_interval = bswap32(first_point.time);
    this->base_intervals[archive_index] = base_interval;
  }
  return base_interval;
}

void WhisperArchive::write_archive(int fd, uint32_t archive_index,
    const Series& data, uint32_t start_index, uint32_t end_index) {

  const ArchiveMetadata& archive = this->metadata->archives[archive_index];
  int32_t archive_size = archive.points * sizeof(FilePoint);

  // get the base interval. if it's not set, pretend it's equal to the first
  // point's interval so we'll write at the beginning of the archive
  uint64_t archive_start_interval = this->get_base_interval(fd, archive_index);
  if (archive_start_interval == 0) {
    int64_t first_ts = data[start_index].timestamp;
    archive_start_interval = first_ts - (first_ts % archive.seconds_per_point);
  }

  // write all the points to the archive first
  // TODO: don't call pwritex for every loop iteration; coalesce writes somehow
  for (auto pt : data) {
    int64_t point_interval = pt.timestamp - (pt.timestamp % archive.seconds_per_point);
    int64_t time_distance = point_interval - archive_start_interval;
    int32_t point_distance = time_distance / archive.seconds_per_point;
    // TODO: figure out negative modulus, you lazy bum
    while (point_distance < 0) {
      point_distance += archive.points;
    }
    int32_t point_offset = archive.offset + ((point_distance * sizeof(FilePoint)) % archive_size);

    FilePoint sw_pt;
    sw_pt.time = bswap32(point_interval);
    sw_pt.value = bswap64f(pt.value);
    pwritex(fd, &sw_pt, sizeof(FilePoint), point_offset);

    // if the point is at the start of the archive, the base interval was changed
    if (point_offset == static_cast<int32_t>(archive.offset)) {
      this->base_intervals[archive_index] = point_interval;
    }
  }

  // now propagate to lower-precision archives
  bool continue_propagation = true;
  for (uint32_t target_archive_index = archive_index + 1;
       target_archive_index < this->metadata->num_archives && continue_propagation;
       target_archive_index++) {

    unordered_set<uint64_t> lower_intervals;
    for (const auto& pt : data) {
      uint64_t pt_interval = pt.timestamp - (pt.timestamp % this->metadata->archives[target_archive_index].seconds_per_point);
      lower_intervals.insert(pt_interval);
    }

    continue_propagation = false;
    for (uint64_t interval : lower_intervals)
      continue_propagation |= this->propagate_write(fd, interval, archive_index, target_archive_index);
  }
}

bool WhisperArchive::propagate_write(int fd, uint64_t interval,
    uint32_t archive_index, uint32_t target_archive_index) {

  const auto& archive = this->metadata->archives[archive_index];
  const auto& target_archive = this->metadata->archives[target_archive_index];
  uint32_t archive_size = archive.points * sizeof(FilePoint);
  uint32_t target_archive_size = target_archive.points * sizeof(FilePoint);

  int64_t target_interval_start = interval - (interval % target_archive.seconds_per_point);

  uint64_t base_interval = this->get_base_interval(fd, archive_index);
  uint32_t start_offset;
  if (base_interval == 0) {
    start_offset = archive.offset;
  } else {
    uint32_t point_distance = (target_interval_start - base_interval) / archive.seconds_per_point;
    uint32_t byte_distance = point_distance * sizeof(FilePoint);
    start_offset = archive.offset + (byte_distance % archive_size);
  }

  uint32_t num_points = target_archive.seconds_per_point / archive.seconds_per_point;
  uint32_t end_offset = ((start_offset - archive.offset + num_points * sizeof(FilePoint)) % archive_size) + archive.offset;

  unique_ptr<FilePoint[]> raw_points(new FilePoint[num_points]);
  if (start_offset < end_offset) {
    preadx(fd, raw_points.get(), sizeof(FilePoint) * num_points, start_offset);

  } else {
    // the range spans the circular boundary - need to read 2 chunks
    uint32_t archive_end_offset = archive.offset + sizeof(FilePoint) * archive.points;
    uint32_t num_points_first = (archive_end_offset - start_offset) / sizeof(FilePoint);
    preadx(fd, raw_points.get(), sizeof(FilePoint) * num_points_first,
        start_offset);
    preadx(fd, &(raw_points.get())[num_points_first],
        sizeof(FilePoint) * (num_points - num_points_first), archive.offset);
  }

  // count known values; aggregate if there are enough
  uint32_t num_known = 0;
  uint64_t current_interval = target_interval_start;
  for (uint32_t x = 0; x < num_points; current_interval += archive.seconds_per_point, x++) {
    if (bswap32(raw_points[x].time) == current_interval) {
      num_known++;
    }
  }

  if (num_known >= this->metadata->x_files_factor * num_points) {
    double value = this->aggregate(target_interval_start,
        archive.seconds_per_point, raw_points.get(), num_points);
    uint64_t target_base_interval = this->get_base_interval(fd, target_archive_index);

    off_t offset;
    if (target_base_interval == 0) {
      offset = target_archive.offset;
      this->base_intervals[target_archive_index] = target_interval_start;

    } else {
      uint32_t point_distance = (target_interval_start - target_base_interval) / target_archive.seconds_per_point;
      uint32_t byte_distance = point_distance * sizeof(FilePoint);
      offset = target_archive.offset + (byte_distance % target_archive_size);
      if (offset == target_archive.offset) {
        this->base_intervals[target_archive_index] = target_interval_start;
      }
    }

    FilePoint sw_pt;
    sw_pt.time = bswap32(target_interval_start);
    sw_pt.value = bswap64f(value);
    pwritex(fd, &sw_pt, sizeof(FilePoint), offset);
    return true;
  }

  return false;
}

static double aggregate_min(double a, double b) {
  return (a < b) ? a : b;
}

static double aggregate_max(double a, double b) {
  return (a < b) ? b : a;
}

static double aggregate_sum(double a, double b) {
  return a + b;
}

static double aggregate_last(double a, double b) {
  return b;
}

double WhisperArchive::aggregate(uint64_t interval_start,
    uint64_t interval_step, const FilePoint* pts, uint32_t num_pts) const {

  double (*combine)(double, double) = aggregate_sum;
  if (this->metadata->aggregation_method == AggregationMethod::Min) {
    combine = aggregate_min;
  } else if (this->metadata->aggregation_method == AggregationMethod::Max) {
    combine = aggregate_max;
  } else if (this->metadata->aggregation_method == AggregationMethod::Last) {
    combine = aggregate_last;
  }

  double agg_value = 0;
  uint32_t num_known = 0;
  uint64_t current_interval = interval_start;
  for (uint32_t x = 0; x < num_pts; current_interval += interval_step, x++) {
    if (bswap32(pts[x].time) != current_interval) {
      continue;
    }

    double pt_value = bswap64f(pts[x].value);
    agg_value = (num_known == 0) ? pt_value : combine(agg_value, pt_value);
    num_known++;
  }

  // special case for averaging; it uses the sum combiner but needs a division
  // step after all the combines
  if (this->metadata->aggregation_method == AggregationMethod::Average) {
    agg_value /= num_known;
  }

  return agg_value;
}

size_t WhisperArchive::get_files_lru_size() {
  return WhisperArchive::file_cache.size();
}

void WhisperArchive::set_files_lru_max_size(size_t max) {
  WhisperArchive::file_cache.set_max_size(max);
}

void WhisperArchive::clear_files_lru() {
  WhisperArchive::file_cache.clear();
}

FileCache WhisperArchive::file_cache(1024);
