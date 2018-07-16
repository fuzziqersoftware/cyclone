#include "CachedDiskStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <deque>
#include <iostream>
#include <phosg/Filesystem.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>
#include <stdexcept>
#include <string>
#include <vector>

#include "Whisper.hh"

using namespace std;


CachedDiskStore::KeyPath::KeyPath(const string& key_name) :
    directories(split(key_name, '.')) {
  if (this->directories.empty()) {
    throw invalid_argument("empty key name");
  }
  this->basename = move(directories.back());
  this->directories.pop_back();
}

string CachedDiskStore::KeyPath::str() const {
  string ret;
  for (const string& dir : this->directories) {
    ret += dir;
    ret += '.';
  }
  ret += this->basename;
  return ret;
}

string CachedDiskStore::filename_for_path(const KeyPath& p) {
  string filename = this->directory;
  for (const auto& dirname : p.directories) {
    filename += '/';
    filename += dirname;
  }
  if (!p.basename.empty()) {
    filename += '/';
    filename += p.basename;
    filename += ".wsp";
  }
  return filename;
}

CachedDiskStore::CacheTraversal::CacheTraversal(CachedDirectoryContents* root,
    string directory, size_t expected_levels) : levels({root}), level(root),
    archive(NULL), filesystem_path(directory) {
  this->levels.reserve(expected_levels);
  this->guards.reserve(expected_levels);
}

void CachedDiskStore::CacheTraversal::move_to_level(const string& item) {
  rw_guard g(this->level->subdirectories_lock, false);
  CachedDirectoryContents* new_level = this->level->subdirectories.at(item).get();
  {
    lock_guard<mutex> g2(this->level->subdirectories_lru_lock);
    this->level->subdirectories_lru.touch(item);
  }
  this->level = new_level;
  this->levels.emplace_back(level);
  this->guards.emplace_back(move(g));
}

pair<size_t, size_t> CachedDiskStore::CachedDirectoryContents::get_counts() const {
  pair<size_t, size_t> ret = make_pair(1, 0);
  {
    rw_guard g(this->subdirectories_lock, false);
    for (const auto& it : this->subdirectories) {
      auto counts = it.second->get_counts();
      ret.first += counts.first;
      ret.second += counts.second;
    }
  }
  {
    rw_guard g(this->files_lock, false);
    ret.second += this->files.size();
  }
  return ret;
}

string CachedDiskStore::CachedDirectoryContents::str() const {
  // returns something like "[dir1=[...], dir2=[...], file3, file4]"
  string ret = "[";
  {
    rw_guard g(this->subdirectories_lock, false);
    for (const auto& it : this->subdirectories) {
      if (ret.size() > 1) {
        ret += ", ";
      }
      ret += it.first;
      ret += '=';
      ret += it.second->str();
    }
  }
  {
    rw_guard g(this->files_lock, false);
    for (const auto& it : this->files) {
      if (ret.size() > 1) {
        ret += ", ";
      }
      ret += it.first;
    }
  }
  ret += ']';
  return ret;
}

CachedDiskStore::CachedDiskStore(const string& directory,
    size_t directory_limit, size_t file_limit) : DiskStore(directory), stats(3),
    directory_count(0), file_count(0), directory_limit(directory_limit),
    file_limit(file_limit), should_exit(false),
    evict_items_thread(&CachedDiskStore::evict_items_thread_routine, this) { }

CachedDiskStore::~CachedDiskStore() {
  this->should_exit = true;
  this->evict_items_thread.join();
}

size_t CachedDiskStore::get_directory_limit() const {
  return this->directory_limit;
}

size_t CachedDiskStore::get_file_limit() const {
  return this->file_limit;
}

void CachedDiskStore::set_directory_limit(size_t new_value) {
  this->directory_limit = new_value;
}

void CachedDiskStore::set_file_limit(size_t new_value) {
  this->file_limit = new_value;
}

void CachedDiskStore::set_directory(const string& new_value) {
  DiskStore::set_directory(new_value);
  this->delete_from_cache("", true);
}

bool CachedDiskStore::create_cache_directory_locked(CachedDirectoryContents* level,
    const string& item) {
  // note: we can mess with the lru here without locking because we expect
  // subdirectories_lock to be already held for writing

  if (level->subdirectories.emplace(piecewise_construct,
      forward_as_tuple(item), forward_as_tuple(new CachedDirectoryContents())).second) {
    this->directory_count++;
    this->stats[0].directory_creates++;
    level->subdirectories_lru.insert(item, 0);
    return true;

  } else {
    level->subdirectories_lru.touch(item);
    return false;
  }
}

bool CachedDiskStore::create_cache_file_locked(CachedDirectoryContents* level,
    const string& item, const string& filesystem_path) {
  // note: we can mess with the lru here without locking because we expect
  // files_lru_lock to be already held for writing
  if (level->files.emplace(item, filesystem_path).second) {
    this->file_count++;
    this->stats[0].file_creates++;
    level->files_lru.insert(item, 0);
    return true;
  } else {
    level->files_lru.touch(item);
    return false;
  }
}

void CachedDiskStore::check_and_delete_cache_path(const KeyPath& path) {
  KeyPath p = path; // make a mutable copy

  // walk the cache to find the file entry
  // invariant: guards.size() == levels.size() - 1 (last level is not locked)
  vector<CachedDirectoryContents*> levels;
  vector<rw_guard> guards;
  levels.emplace_back(&this->cache_root);

  try {
    for (const auto& dirname : p.directories) {
      rw_guard g(levels.back()->subdirectories_lock, false);
      levels.emplace_back(levels.back()->subdirectories.at(dirname).get());
      this->stats[0].directory_hits++;
      guards.emplace_back(move(g));
    }

    // if we get here, then all the cache directories exist; delete the file
    if (!p.basename.empty()) {
      string filename = this->filename_for_path(p);
      rw_guard g(levels.back()->files_lock, true);
      if (!isfile(filename)) {
        if (levels.back()->files.erase(p.basename)) {
          this->stats[0].file_deletes++;
        }
        levels.back()->files_lru.erase(p.basename);
      }
    }
  } catch (const out_of_range& e) {
    this->stats[0].directory_misses++;

    // remove subdirectories we didn't visit
    assert(p.directories.size() >= levels.size() - 1);
    p.directories.resize(levels.size() - 1);
  }

  // now delete any empty directories in which the file resided. stop when
  // we reach the root directory or any directory isn't empty. note that we
  // can't just iterate upward through the cache tree looking for a non-empty
  // dir and delete everything below it at once - if someone creates a file/dir
  // during that procedure, we won't be able to delete the other empty dirs.
  while (!p.directories.empty()) {
    assert(p.directories.size() == levels.size() - 1);
    assert(guards.size() == levels.size() - 1);

    // TODO: don't rebuild filename for each loop iteration
    string filename = this->directory;
    for (const auto& dirname : p.directories) {
      filename += '/';
      filename += dirname;
    }

    if (rmdir(filename.c_str())) {
      if (errno == ENOTEMPTY) {
        break; // the directory contains other stuff; we're done
      }
      if (errno != ENOENT) {
        throw runtime_error("can\'t delete directory: " + filename);
      }

      // if we get here, the directory doesn't exist on disk - delete it
      // from the cache. we have to check its existence again while holding
      // the write lock to protect against data races.
      CachedDirectoryContents* parent_level = levels[levels.size() - 2];
      guards.pop_back(); // unlock the parent level

      // we create a reference to the cached object outside the lock scope so
      // that its destructor is called after we've released the write lock
      unique_ptr<CachedDirectoryContents> deleted_level;
      {
        rw_guard g(parent_level->subdirectories_lock, true);
        auto dir_it = parent_level->subdirectories.find(p.directories.back());
        if ((dir_it != parent_level->subdirectories.end()) && !isdir(filename)) {
          deleted_level = move(dir_it->second);
          parent_level->subdirectories.erase(dir_it);
          parent_level->subdirectories_lru.erase(p.directories.back());
        }
      }
      auto counts = deleted_level->get_counts();
      this->stats[0].report_directory_delete(counts.first, counts.second);
      this->directory_count -= counts.first;
      this->file_count -= counts.second;

      // we deleted a directory - move up to the previous one
      levels.pop_back();
      p.directories.pop_back();
    }
  }
}

unordered_map<string, string> CachedDiskStore::update_metadata(
    const SeriesMetadataMap& metadata_map, bool create_new,
    UpdateMetadataBehavior update_behavior, bool local_only) {

  unordered_map<string, string> ret;
  for (auto& it : metadata_map) {
    const auto& key_name = it.first;
    const auto& metadata = it.second;

    if (!this->key_name_is_valid(key_name)) {
      ret.emplace(key_name, "key contains invalid characters");
      continue;
    }

    try {
      KeyPath p(key_name);
      // TODO: can we use the metadata_to_create argument here? might help to
      // clean up some of the below logic
      CacheTraversal t = this->traverse_cache_tree(p.directories, create_new);

      // construct the full key filename
      t.filesystem_path += '/';
      t.filesystem_path += p.basename;
      t.filesystem_path += ".wsp";

      // check if the file exists in the cache
      bool file_exists;
      {
        rw_guard g(t.level->files_lock, false);
        file_exists = t.level->files.count(p.basename);
      }

      // if the file is missing from the cache, check if it exists on disk and add
      // it to the cache if necessary
      if (file_exists) {
        this->stats[0].file_hits++;
      } else {
        this->stats[0].file_misses++;

        file_exists = isfile(t.filesystem_path);
        if (file_exists) {
          rw_guard g(t.level->files_lock, true);
          this->create_cache_file_locked(t.level, p.basename, t.filesystem_path);
        }
      }

      // now the file exists or is missing from both the cache and filesystem.
      // if it exists, we'll apply the update behavior; if it doesn't, then
      // we'll create it if requested.
      if (file_exists) {
        rw_guard g(t.level->files_lock, false);
        try {
          WhisperArchive& d = t.level->files.at(p.basename);
          {
            lock_guard<mutex> g2(t.level->files_lru_lock);
            t.level->files_lru.touch(p.basename);
          }

          if (update_behavior == UpdateMetadataBehavior::Recreate) {
            d.update_metadata(metadata.archive_args, metadata.x_files_factor, metadata.agg_method, true);
            ret.emplace(key_name, "");
            this->stats[0].series_truncates++;

          } else if (update_behavior == UpdateMetadataBehavior::Update) {
            d.update_metadata(metadata.archive_args, metadata.x_files_factor, metadata.agg_method);
            ret.emplace(key_name, "");
            this->stats[0].series_update_metadatas++;

          } else if (update_behavior == UpdateMetadataBehavior::Ignore) {
            ret.emplace(key_name, "ignored");
          }

        } catch (const out_of_range& e) {
          ret.emplace(key_name, e.what());
          continue; // data race - the cache entry was deleted. just skip it
        }

      } else {
        if (create_new) {
          // note: we don't take the LRU lock because we're already holding the
          // write lock for the files map - nobody else can touch the files LRU
          // anyway
          rw_guard g(t.level->files_lock, true);
          t.level->files.erase(p.basename);
          t.level->files.emplace(piecewise_construct,
              forward_as_tuple(p.basename),
              forward_as_tuple(move(t.filesystem_path), metadata.archive_args,
                  metadata.x_files_factor, metadata.agg_method));
          t.level->files_lru.insert(p.basename, 0);
          ret.emplace(key_name, "");

          auto& s = this->stats[0];
          s.series_creates++;
          s.file_creates++;

        } else {
          ret.emplace(key_name, "ignored");
        }
      }

    } catch (const out_of_range& e) {
      ret.emplace(key_name, "ignored");

    } catch (const exception& e) {
      ret.emplace(key_name, e.what());
    }
  }

  return ret;
}

unordered_map<string, int64_t> CachedDiskStore::delete_series(
    const vector<string>& patterns, bool local_only) {
  unordered_map<string, int64_t> ret;

  // if a pattern ends in **, delete the entire tree. ** doesn't work in
  // resolve_patterns and isn't allowed anywhere else in patterns, so
  // special-case it here
  vector<string> determinate_patterns;
  for (const auto& pattern : patterns) {
    if (ends_with(pattern, ".**")) {
      string directory_pattern = pattern.substr(0, pattern.size() - 3);
      string directory_path = this->filename_for_key(directory_pattern, false);
      int64_t& deleted_count = ret[pattern];

      vector<string> paths_to_count;
      paths_to_count.emplace_back(directory_path);
      while (!paths_to_count.empty()) {
        string path = paths_to_count.back();
        paths_to_count.pop_back();

        for (const string& item : list_directory(path)) {
          string item_path = path + "/" + item;
          if (isdir(item_path)) {
            paths_to_count.emplace_back(item_path);
          } else {
            deleted_count += 1;
          }
        }
      }

      unlink(directory_path, true);

    } else {
      determinate_patterns.emplace_back(pattern);
    }
  }

  auto key_to_pattern = this->resolve_patterns(determinate_patterns, local_only);

  for (auto key_it : key_to_pattern) {
    // if the token is a pattern, don't delete it - directory trees must be
    // deleted with the .** form of this command instead
    if (this->token_is_pattern(key_it.first)) {
      continue;
    }

    try {
      KeyPath p(key_it.first);

      // delete the file on disk
      string filename = this->filename_for_key(key_it.first);
      try {
        unlink(filename);
        this->stats[0].series_deletes++;
      } catch (const runtime_error& e) { }

      // note: we don't need to explicitly close the fd in WhisperArchive's file
      // cache; it should be closed in the WhisperArchive destructor, which is
      // indirectly called in check_and_delete_cache_path
      this->check_and_delete_cache_path(p);

      ret[key_it.second]++;

    } catch (const exception& e) {
      log(INFO, "[CachedDiskStore] failed to delete series %s (%s)",
          key_it.first.c_str(), e.what());
    }
  }

  for (const string& pattern : determinate_patterns) {
    ret.emplace(pattern, 0);
  }
  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> CachedDiskStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time,
    bool local_only) {

  unordered_map<string, string> key_to_pattern = this->resolve_patterns(
      key_names, local_only);

  unordered_map<string, unordered_map<string, ReadResult>> ret;
  for (const auto& it : key_to_pattern) {
    const string& key_name = it.first;
    const string& pattern = it.second;

    unordered_map<string, ReadResult>& read_results = ret[pattern];
    ReadResult& r = read_results[key_name];

    KeyPath p(key_name);
    try {
      CacheTraversal t = this->traverse_cache_tree(p);
      if (start_time && end_time) {
        auto res = t.archive->read(start_time, end_time);
        r.data = move(res.data);
        r.start_time = res.start_time;
        r.end_time = res.end_time;
        r.step = res.step;
      }

    } catch (const out_of_range& e) {
      // one of the directories doesn't exist
      r.start_time = start_time;
      r.end_time = end_time;
      r.step = 0;

    } catch (const cannot_open_file& e) {
      if (e.error == ENOENT) {
        // apparently the file was deleted; remove it from the cache
        this->check_and_delete_cache_path(p);
        r.start_time = start_time;
        r.end_time = end_time;
        r.step = 0;
      } else {
        r.error = e.what();
      }

    } catch (const exception& e) {
      r.error = e.what();
    }
  }

  this->stats[0].report_read_request(ret);
  return ret;
}

unordered_map<string, string> CachedDiskStore::write(
    const unordered_map<string, Series>& data, bool local_only) {
  unordered_map<string, string> ret;
  for (auto& it : data) {
    if (!this->key_name_is_valid(it.first)) {
      ret.emplace(it.first, "key contains invalid characters");
      continue;
    }

    KeyPath p(it.first);
    try {
      try {
        CacheTraversal t = this->traverse_cache_tree(p);
        t.archive->write(it.second);
        ret.emplace(it.first, "");
        continue;

      } catch (const out_of_range& e) {
        // a directory doesn't exist; we'll create it below if needed

      } catch (const cannot_open_file& e) {
        if (e.error == ENOENT) {
          // apparently the file was deleted; remove it from the cache
          this->check_and_delete_cache_path(p);
        } else {
          throw;
        }
      }

      // if we get here, then we should autocreate the cache path if applicable
      auto m = this->get_autocreate_metadata_for_key(it.first);
      if (m.archive_args.empty()) {
        log(INFO, "[CachedDiskStore] no autocreate metadata for %s", it.first.c_str());
        ret.emplace(it.first, "series does not exist");

      } else {
        CacheTraversal t = this->traverse_cache_tree(p, &m);
        t.archive->write(it.second);
        ret.emplace(it.first, "");
      }

    } catch (const exception& e) {
      ret.emplace(it.first, e.what());
    }
  }

  this->stats[0].report_write_request(ret, data);
  return ret;
}

static string path_join(const string& a, const string& b) {
  return a.empty() ? b : (a + "." + b);
}

unordered_map<string, FindResult> CachedDiskStore::find(
    const vector<string>& patterns, bool local_only) {

  unordered_map<string, FindResult> ret;
  for (const auto& pattern : patterns) {
    auto emplace_ret = ret.emplace(piecewise_construct,
        forward_as_tuple(pattern), forward_as_tuple());
    if (!emplace_ret.second) {
      continue;
    }
    FindResult& r = emplace_ret.first->second;

    if (this->pattern_is_indeterminate(pattern)) {
      r.error = "pattern is indeterminate";
      continue;
    }

    try {
      KeyPath p(pattern);

      unordered_map<CachedDirectoryContents*, string> current_levels, next_levels;
      current_levels.emplace(&this->cache_root, "");
      vector<rw_guard> guards;

      for (const auto& item : p.directories) {
        for (auto& level_it : current_levels) {
          CachedDirectoryContents* level = level_it.first;
          const auto& current_level_path = level_it.second;

          // if the current token is a pattern, we'll have to scan the current level
          if (this->token_is_pattern(item)) {
            // make sure the current level is complete
            this->populate_cache_level(level, this->filename_for_key(current_level_path, false));

            // scan through its directories for the ones we want
            bool keep_lock = false;
            rw_guard g(level->subdirectories_lock, false);
            for (auto& it : level->subdirectories) {
              if (this->name_matches_pattern(it.first, item)) {
                string next_level_path = path_join(current_level_path, it.first);
                next_levels.emplace(piecewise_construct, forward_as_tuple(it.second.get()),
                    forward_as_tuple(next_level_path));
                {
                  lock_guard<mutex> g2(level->subdirectories_lru_lock);
                  level->subdirectories_lru.touch(it.first);
                }
                keep_lock = true;
              }
            }

            // if we found anything, keep the read lock so we can safely examine the
            // subtree
            if (keep_lock) {
              guards.emplace_back(move(g));
            }

          // this token isn't a pattern; we can directly look up the subdirectory
          // (and not populate + scan the current directory)
          } else {
            try {
              rw_guard g(level->subdirectories_lock, false);
              CachedDirectoryContents* next_level = level->subdirectories.at(item).get();

              {
                lock_guard<mutex> g2(level->subdirectories_lru_lock);
                level->subdirectories_lru.touch(item);
              }
              guards.emplace_back(move(g));

              string next_level_path = path_join(current_level_path, item);
              next_levels.emplace(next_level, next_level_path);
              this->stats[0].directory_hits++;

            } catch (const out_of_range& e) {
              this->stats[0].directory_misses++;

              // if the subdirectory isn't in the cache and the list isn't complete
              // for this level, check the filesystem
              if (!level->list_complete) {
                string new_level_path = path_join(current_level_path, item);
                if (isdir(this->filename_for_key(new_level_path, false))) {
                  rw_guard g(level->subdirectories_lock, true);
                  this->create_cache_directory_locked(level, item);
                }

                // it's possible that a delete occurred between the emplace above
                // and this line - if so, just pretend it doesn't exist (since it
                // was deleted anyway)
                try {
                  guards.emplace_back(level->subdirectories_lock, false);
                  next_levels.emplace(level->subdirectories.at(item).get(), new_level_path);
                } catch (const out_of_range& e) { }
              }
            }
          }
        }

        // move down to the next layer of the tree
        current_levels.clear();
        next_levels.swap(current_levels);
      }

      // at this point current_levels is the set of directories that (should)
      // contain the files we're looking for; scan through their directories and
      // files to find key names to return
      for (auto& it : current_levels) {
        CachedDirectoryContents* level = it.first;
        const string& level_path = it.second;

        if (this->token_is_pattern(p.basename)) {
          this->populate_cache_level(level, this->filename_for_key(level_path, false));

          {
            rw_guard g(level->subdirectories_lock, false);
            for (const auto& it : level->subdirectories) {
              if (this->name_matches_pattern(it.first, p.basename)) {
                {
                  lock_guard<mutex> g2(level->subdirectories_lru_lock);
                  level->subdirectories_lru.touch(it.first);
                }
                r.results.emplace_back(path_join(level_path, it.first) + ".*");
              }
            }
          }

          {
            rw_guard g(level->files_lock, false);
            for (const auto& it : level->files) {
              if (this->name_matches_pattern(it.first, p.basename)) {
                r.results.emplace_back(path_join(level_path, it.first));
              }
            }
          }

        } else {
          bool directory_in_cache = false, file_in_cache = false;

          {
            rw_guard g(level->subdirectories_lock, false);
            directory_in_cache = level->subdirectories.count(p.basename);
            if (directory_in_cache) {
              this->stats[0].directory_hits++;

              lock_guard<mutex> g2(level->subdirectories_lru_lock);
              level->subdirectories_lru.touch(p.basename);
            } else {
              this->stats[0].directory_misses++;
            }
          }
          if (directory_in_cache) {
            r.results.emplace_back(path_join(level_path, p.basename) + ".*");
          } else if (!level->list_complete && isdir(this->filename_for_key(path_join(level_path, p.basename), false))) {
            r.results.emplace_back(path_join(level_path, p.basename) + ".*");
            rw_guard g(level->subdirectories_lock, true);
            this->create_cache_directory_locked(level, p.basename);
          }

          {
            rw_guard g(level->files_lock, false);
            file_in_cache = level->files.count(p.basename);
          }
          if (file_in_cache) {
            this->stats[0].file_hits++;
            r.results.emplace_back(path_join(level_path, p.basename));

          } else if (!level->list_complete) {
            this->stats[0].file_misses++;

            string filename = this->filename_for_key(path_join(level_path, p.basename));
            if (isfile(filename)) {
              r.results.emplace_back(path_join(level_path, p.basename));
              rw_guard g(level->files_lock, true);
              this->create_cache_file_locked(level, p.basename, filename);
            }
          }
        }
      }

    } catch (const exception& e) {
      r.error = e.what();
    }
  }

  this->stats[0].report_find_request(ret);
  return ret;
}

unordered_map<string, int64_t> CachedDiskStore::get_stats(bool rotate) {
  const Stats& current_stats = this->stats[0];

  if (rotate) {
    this->stats.rotate();

    uint64_t n = now();
    this->stats[0].start_time = n;
    this->stats[1].duration = n - this->stats[1].start_time;
  }

  auto ret = current_stats.to_map();
  ret.emplace("cache_directory_count", this->directory_count.load());
  ret.emplace("cache_file_count", this->file_count.load());
  ret.emplace("cache_directory_limit", this->directory_limit.load());
  ret.emplace("cache_file_limit", this->file_limit.load());
  return ret;
}

string CachedDiskStore::restore_series(const string& key_name,
      const string& data, bool combine_from_existing, bool local_only) {
  if (!this->key_name_is_valid(key_name)) {
    return "key contains invalid characters";
  }

  KeyPath p(key_name);
  try {
    try {
      CacheTraversal t = this->traverse_cache_tree(p);

      // if we do have to combine, make a temp file so we can read from both
      // series without conflicts
      string temp_filename = string_printf("%s.restore-%" PRId64,
          t.archive->get_filename().c_str(), now());
      WhisperArchive temp_series(temp_filename, data);
      try {
        auto metadata = temp_series.get_metadata();
        const auto& archive_metadata = metadata->archives[0];

        // read the entire first archive of the restored series, and take the latest
        // non-null datapoint
        uint64_t end_time = now() / 1000000;
        uint64_t start_time = end_time - archive_metadata.seconds_per_point * archive_metadata.points;
        auto read_result = temp_series.read(start_time, end_time, 0);

        uint32_t latest_datapoint_time = 0;
        if (!read_result.data.empty()) {
          latest_datapoint_time = read_result.data[read_result.data.size() - 1].timestamp;
        }

        // if there's data, copy it over to the new file
        if (latest_datapoint_time) {
          auto original_read_result = t.archive->read(
              latest_datapoint_time, end_time);
          temp_series.write(original_read_result.data);
        }

        // rename the temp series into place
        rename(temp_filename, t.archive->get_filename());

        // update the cache directory - we need to reopen the file. for now
        // we'll just close it, delete the cache file, and clear the list flag
        {
          rw_guard g(t.level->files_lock, true);
          t.level->files.erase(p.basename);
          t.level->files_lru.erase(p.basename);
          t.level->list_complete = false;
        }

        return "";

      } catch (const exception& e) {
        unlink(temp_filename);
        throw;
      }

    } catch (const out_of_range& e) {
      // a directory doesn't exist; we'll create it below if needed

    } catch (const cannot_open_file& e) {
      if (e.error == ENOENT) {
        // apparently the file was deleted; remove it from the cache
        this->check_and_delete_cache_path(p);
      } else {
        throw;
      }
    }

    // if we get here, then we should create the key (it doesn't exist)
    this->traverse_cache_tree(p, NULL, &data);
    return "";

  } catch (const exception& e) {
    return e.what();
  }
}

string CachedDiskStore::serialize_series(const string& key_name,
    bool local_only) {
  KeyPath p(key_name);
  try {
    CacheTraversal t = this->traverse_cache_tree(p);
    return t.archive->serialize();

  } catch (const out_of_range& e) {
    // one of the directories doesn't exist
    return "";

  } catch (const cannot_open_file& e) {
    if (e.error == ENOENT) {
      // apparently the file was deleted; remove it from the cache
      this->check_and_delete_cache_path(p);
    }
    return "";

  } catch (const exception& e) {
    return "";
  }
}

int64_t CachedDiskStore::delete_from_cache(const string& path, bool local_only) {
  // if the path is empty, delete the ENTIRE cache
  if (path.empty() || (path == "*")) {
    CachedDirectoryContents old_root;

    {
      vector<rw_guard> guards;
      guards.emplace_back(this->cache_root.subdirectories_lock, true);
      guards.emplace_back(this->cache_root.files_lock, true);

      this->cache_root.subdirectories.swap(old_root.subdirectories);
      this->cache_root.files.swap(old_root.files);
      this->cache_root.subdirectories_lru.swap(old_root.subdirectories_lru);
      this->cache_root.files_lru.swap(old_root.files_lru);

      this->directory_count = 0;
      this->file_count = 0;
    }

    auto counts = old_root.get_counts();
    return counts.first + counts.second;
  }

  // path isn't empty - we're deleting only part of the cache
  KeyPath p(path);
  vector<rw_guard> guards;
  CachedDirectoryContents* level = &this->cache_root;

  // move to the directory containing the key we want to delete
  for (const auto& item : p.directories) {
    try {
      rw_guard g(level->subdirectories_lock, false);
      level = level->subdirectories.at(item).get();
      guards.emplace_back(move(g));
    } catch (const out_of_range& e) {
      return 0; // path already doesn't exist in the cache
    }
  }

  // now, the basename can be a subdirectory or a file, or even both
  int64_t items_deleted = 0;

  // check if a subdirectory exists with the given name and delete it if so
  bool exists;
  {
    rw_guard g(level->subdirectories_lock, false);
    exists = level->subdirectories.count(p.basename);
  }
  if (exists) {
    CachedDirectoryContents old_level;
    {
      rw_guard g(level->subdirectories_lock, true);
      auto it = level->subdirectories.find(p.basename);
      if (it != level->subdirectories.end()) {
        // we don't need to lock it->second because we already hold a write lock
        // for its parent directory - no other thread can access it right now
        it->second->subdirectories.swap(old_level.subdirectories);
        it->second->subdirectories_lru.swap(old_level.subdirectories_lru);
        it->second->files.swap(old_level.files);
        it->second->files_lru.swap(old_level.files_lru);

        level->subdirectories.erase(it);
        level->subdirectories_lru.erase(it->first);
        level->list_complete = false;
      }
    }
    auto counts = old_level.get_counts();
    this->directory_count -= counts.first;
    this->file_count -= counts.second;
    items_deleted += (counts.first + counts.second);
  }

  // check if a file exists with the given name and delete it if so
  {
    rw_guard g(level->files_lock, false);
    exists = level->files.count(p.basename);
  }
  if (exists) {
    // not worth swapping the WhisperArchive out; just delete it inline
    rw_guard g(level->files_lock, true);
    bool deleted = level->files.erase(p.basename);
    if (deleted) {
      level->files_lru.erase(p.basename);
      level->list_complete = false;
      this->file_count--;
      items_deleted++;
    }
  }

  return items_deleted;
}

string CachedDiskStore::str() const {
  string s = "CachedDiskStore(" + this->directory + ", cache_state=" + this->cache_root.str() + ")";
  return s;
}

void CachedDiskStore::populate_cache_level(CachedDirectoryContents* level,
    const string& filesystem_path) {
  if (level->list_complete) {
    return;
  }

  // unfortunately we have to hold the write lock while listing the directory to
  // prevent create/delete races causing cache inconsistency
  rw_guard subdirectories_g(level->subdirectories_lock, true);
  rw_guard files_g(level->files_lock, true);

  // add missing subdirectories and files
  auto filesystem_items = list_directory(filesystem_path);
  for (const auto& item : filesystem_items) {
    string item_filesystem_path = filesystem_path + "/" + item;

    try {
      auto st = stat(item_filesystem_path);
      // note that we already hold both locks for writing
      if (ends_with(item, ".wsp") && isfile(st)) {
        string key_name = item.substr(0, item.size() - 4);
        this->create_cache_file_locked(level, key_name, item_filesystem_path);
      } else if (isdir(st)) {
        this->create_cache_directory_locked(level, item);
      }
    } catch (const cannot_stat_file& e) {
      // this can happen if a file is deleted or renamed during this process, or
      // if it has an invalid name. just ignore it entirely (don't populate it)
    }
  }

  // remove subdirectories and files that have been deleted from the filesystem.
  // we don't take the LRU locks because we're already holding the write locks
  // for the subdirectories and files maps - nobody else can touch the LRUs
  // anyway
  for (auto it = level->files.begin(); it != level->files.end();) {
    string filename = it->first + ".wsp";
    if (!filesystem_items.count(filename)) {
      level->files_lru.erase(it->first);
      it = level->files.erase(it);
      this->stats[0].file_deletes++;
      this->file_count--;
    } else {
      it++;
    }
  }
  for (auto it = level->subdirectories.begin(); it != level->subdirectories.end();) {
    if (!filesystem_items.count(it->first)) {
      level->subdirectories_lru.erase(it->first);
      auto counts = it->second->get_counts();
      it = level->subdirectories.erase(it);

      this->stats[0].report_directory_delete(counts.first, counts.second);
      this->directory_count -= counts.first;
      this->file_count -= counts.second;
    } else {
      it++;
    }
  }

  // we don't have to do this again
  level->list_complete = true;
  this->stats[0].directory_populates++;
}

CachedDiskStore::CacheTraversal CachedDiskStore::traverse_cache_tree(
    const vector<string>& cache_path, bool create) {

  CacheTraversal t(&this->cache_root, this->directory, cache_path.size() + 1);

  // walk the cache layers to find the directory that contains this key
  for (const auto& item : cache_path) {
    t.filesystem_path += '/';
    t.filesystem_path += item;

    try {
      t.move_to_level(item);
      this->stats[0].directory_hits++;

    } catch (const out_of_range& e) {
      this->stats[0].directory_misses++;

      if (create) {
        // the subdirectory doesn't exist - we'll have to create it. first try
        // to create it on the filesystem, so other threads don't see the cache
        // entry and assume the directory exists
        if (mkdir(t.filesystem_path.c_str(), 0755) == -1) {
          if (errno != EEXIST) {
            string error_str = string_for_error(errno);
            throw runtime_error(string_printf("can\'t create directory \"%s\" (%s)",
                t.filesystem_path.c_str(), error_str.c_str()));
          }
        }
        this->stats[0].directory_creates++;

      } else {
        // if this cache level is complete (contains a full list of files), then
        // we don't need to check the filesystem
        if (t.level->list_complete) {
          throw out_of_range(t.filesystem_path + " does not exist (cached)");
        }

        // the subdirectory doesn't exist in the cache, but it may exist on disk
        if (!isdir(t.filesystem_path)) {
          throw out_of_range(t.filesystem_path + " does not exist (uncached)");
        }
      }

      // if we get here, then the directory exists on the filesystem but not in
      // the cache - create a cache node for it and move there
      {
        rw_guard g(t.level->subdirectories_lock, true);
        this->create_cache_directory_locked(t.level, item);
      }
      t.move_to_level(item);
    }
  }

  return t;
}

CachedDiskStore::CacheTraversal CachedDiskStore::traverse_cache_tree(
    const KeyPath& p, const SeriesMetadata* metadata_to_create,
    const string* serialized_data_to_create) {
  CacheTraversal t = this->traverse_cache_tree(p.directories,
      (metadata_to_create != NULL) || (serialized_data_to_create != NULL));

  t.filesystem_path += "/";
  t.filesystem_path += p.basename;
  t.filesystem_path += ".wsp";

  try {
    // get the archive object; if we do so successfully, keep holding the read
    // lock
    rw_guard g(t.level->files_lock, false);
    t.archive = &t.level->files.at(p.basename);
    {
      lock_guard<mutex> g2(t.level->files_lru_lock);
      t.level->files_lru.touch(p.basename);
    }
    t.guards.emplace_back(move(g));
    this->stats[0].file_hits++;

  } catch (const out_of_range& e) {
    this->stats[0].file_misses++;

    // if the list isn't complete and the file exists, populate it in the cache
    // and return it
    if (!t.level->list_complete && isfile(t.filesystem_path)) {
      {
        rw_guard g(t.level->files_lock, true);
        this->create_cache_file_locked(t.level, p.basename, t.filesystem_path);
      }
      t.guards.emplace_back(t.level->files_lock, false);
      t.archive = &t.level->files.at(p.basename);
      return t;
    }

    // at this point the presence/absence of the file in the cache and on the
    // filesystem are the same - it exists in both or neither. and since we
    // already checked the cache, we know it exists in neither.
    if (!metadata_to_create && !serialized_data_to_create) {
      throw out_of_range(string_printf("%s does not exist (%s)",
          t.filesystem_path.c_str(), t.level->list_complete ? "cached" : "uncached"));
    }

    // yay we can autocreate it
    if (log_level() >= INFO) {
      string key_path_str = p.str();
      log(INFO, "[CachedDiskStore] autocreating %s", key_path_str.c_str());
    }
    {
      rw_guard g(t.level->files_lock, true);

      bool was_created;
      if (metadata_to_create) {
        was_created = t.level->files.emplace(piecewise_construct,
            forward_as_tuple(p.basename),
            forward_as_tuple(t.filesystem_path, metadata_to_create->archive_args,
                metadata_to_create->x_files_factor, metadata_to_create->agg_method)).second;
      } else { // serialized_data_to_create was given
        was_created = t.level->files.emplace(piecewise_construct,
            forward_as_tuple(p.basename),
            forward_as_tuple(t.filesystem_path, *serialized_data_to_create)).second;
      }

      if (was_created) {
        this->file_count++;
        this->stats[0].file_creates++;
        this->stats[0].series_autocreates++;
        t.level->files_lru.insert(p.basename, 0);
      } else {
        t.level->files_lru.touch(p.basename);
      }
    }
    t.guards.emplace_back(t.level->files_lock, false);
    t.archive = &t.level->files.at(p.basename);
  }

  return t;
}

CachedDiskStore::Stats::Stats() : DiskStore::Stats::Stats(),
    directory_hits(0),
    directory_misses(0),
    directory_creates(0),
    directory_deletes(0),
    directory_populates(0),
    file_hits(0),
    file_misses(0),
    file_creates(0),
    file_deletes(0) { }

CachedDiskStore::Stats& CachedDiskStore::Stats::operator=(
      const CachedDiskStore::Stats& other) {
  DiskStore::Stats::operator=(other);

  this->directory_hits = other.directory_hits.load();
  this->directory_misses = other.directory_misses.load();
  this->directory_creates = other.directory_creates.load();
  this->directory_deletes = other.directory_deletes.load();
  this->directory_populates = other.directory_populates.load();
  this->file_hits = other.file_hits.load();
  this->file_misses = other.file_misses.load();
  this->file_creates = other.file_creates.load();
  this->file_deletes = other.file_deletes.load();

  return *this;
}

unordered_map<string, int64_t> CachedDiskStore::Stats::to_map() const {
  auto ret = this->DiskStore::Stats::to_map();
  ret.emplace("cache_directory_hits", this->directory_hits.load());
  ret.emplace("cache_directory_misses", this->directory_misses.load());
  ret.emplace("cache_directory_creates", this->directory_creates.load());
  ret.emplace("cache_directory_deletes", this->directory_deletes.load());
  ret.emplace("cache_directory_populates", this->directory_populates.load());
  ret.emplace("cache_file_hits", this->file_hits.load());
  ret.emplace("cache_file_misses", this->file_misses.load());
  ret.emplace("cache_file_creates", this->file_creates.load());
  ret.emplace("cache_file_deletes", this->file_deletes.load());
  return ret;
}



bool CachedDiskStore::evict_items() {
  ssize_t evict_directories = (this->directory_limit == 0) ? 0 :
      (static_cast<ssize_t>(this->directory_count) -
       static_cast<ssize_t>(this->directory_limit));
  ssize_t evict_files = (this->file_limit == 0) ? 0 :
      (static_cast<ssize_t>(this->file_count) -
       static_cast<ssize_t>(this->file_limit));
  if ((evict_directories <= 0) && (evict_files <= 0)) {
    return false;
  }

  vector<string> directory_names;
  vector<CachedDirectoryContents*> levels({&this->cache_root});
  vector<rw_guard> guards;

  // basic idea: go to the leaf directory based on the lrus and delete stuff
  // from it
  for (;;) {
    // if this directory has subdirectories, don't delete it (yet). instead
    // move into it to find something else to delete
    CachedDirectoryContents* level = levels.back();
    rw_guard g(level->subdirectories_lock, false);
    if (level->subdirectories.empty()) {
      break;
    }

    string subdir_name;
    {
      lock_guard<mutex> g2(level->subdirectories_lru_lock);
      subdir_name = level->subdirectories_lru.peek().first;
    }
    level = level->subdirectories.at(subdir_name).get();
    directory_names.emplace_back(subdir_name);
    levels.emplace_back(level);
    guards.emplace_back(move(g));
  }

  // the current level has no subdirectories at this point. delete files out
  // of it first (and we may delete the entire directory later if it becomes
  // empty)
  bool delete_current_directory = false;
  if (evict_files > 0) {
    // delete N files out of the current directory. note that this may still
    // be all of them; some files may have been deleted between the time we
    // checked above and now
    CachedDirectoryContents* level = levels.back();

    unordered_map<string, WhisperArchive> deleted_files;
    LRUSet<string> deleted_files_lru;
    {
      rw_guard g(level->files_lock, true);
      ssize_t target_size = level->files.size() - evict_files;
      if (target_size <= 0) {
        level->files.swap(deleted_files);
        level->files_lru.swap(deleted_files_lru);
        delete_current_directory = true;
      } else {
        while (level->files.size() > static_cast<size_t>(target_size)) {
          string filename = level->files_lru.evict_object().first;
          level->files.erase(filename);
        }
      }
      level->list_complete = false;
    }
  }

  // if we need to delete directories, delete this one
  if (delete_current_directory || (evict_directories > 0)) {
    if (levels.empty() || directory_names.empty() || guards.empty()) {
      log(ERROR, "[CachedDiskStore] cannot evict directories: path is empty");
      return false;
    }

    while (!directory_names.empty()) {
      assert(directory_names.size() == levels.size() - 1);
      assert(guards.size() == levels.size() - 1);

      // move up a directory, and remember which subdirectory we're going to
      // delete
      levels.pop_back();
      CachedDirectoryContents* level = levels.back();
      string dir_to_delete = directory_names.back();
      directory_names.pop_back();
      guards.pop_back(); // this is the read lock on the parent dir

      // we create a reference to the cached object outside the lock scope so
      // that its destructor is called after we've released the write lock
      unique_ptr<CachedDirectoryContents> deleted_level;
      {
        rw_guard g(level->subdirectories_lock, true);
        auto dir_it = level->subdirectories.find(dir_to_delete);
        if (dir_it != level->subdirectories.end()) {
          deleted_level = move(dir_it->second);
          level->subdirectories.erase(dir_it);
          level->subdirectories_lru.erase(dir_to_delete);
          level->list_complete = false;
        }
      }
      auto counts = deleted_level->get_counts();
      this->stats[0].report_directory_delete(counts.first, counts.second);
      this->directory_count -= counts.first;
      this->file_count -= counts.second;

      // if we're at the root, we're done (the root is allowed to be empty)
      if (level == &this->cache_root) {
        break;
      }

      // if the directory we just deleted from is not empty, we're done
      bool is_empty;
      {
        rw_guard g(level->files_lock, false);
        is_empty = level->files.empty();
      }
      if (is_empty) {
        rw_guard g(level->subdirectories_lock, false);
        is_empty = level->subdirectories.empty();
      }
      if (!is_empty) {
        break;
      }
    }
  }

  return true;
}

void CachedDiskStore::evict_items_thread_routine() {
  while (!this->should_exit) {
    try {
      if (!this->evict_items()) {
        usleep(100000);
      }
    } catch (const exception& e) {
      log(ERROR, "[CachedDiskStore] failure in eviction: %s", e.what());
    }
  }
}
