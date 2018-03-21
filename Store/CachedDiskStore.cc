#include "CachedDiskStore.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

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

string CachedDiskStore::filename_for_path(const KeyPath& p) {
  string filename = this->root_directory;
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
    string root_directory, size_t expected_levels) : levels({root}),
    level(root), archive(NULL), filesystem_path(root_directory),
    guards(expected_levels) {
  levels.reserve(expected_levels);
}

void CachedDiskStore::CacheTraversal::move_to_level(const string& item) {
  rw_guard g(this->level->subdirectories_lock, false);
  CachedDirectoryContents* new_level = this->level->subdirectories.at(item).get();
  this->level->touch_subdirectory(item);
  this->level = new_level;
  this->levels.emplace_back(level);
  this->guards.add(move(g));
}

void CachedDiskStore::CachedDirectoryContents::touch_subdirectory(const string& item) {
  lock_guard<mutex> g(this->subdirectories_lru_lock);
  this->subdirectories_lru.touch(item);
}

void CachedDiskStore::CachedDirectoryContents::touch_file(const string& item) {
  lock_guard<mutex> g(this->files_lru_lock);
  this->files_lru.touch(item);
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

CachedDiskStore::CachedDiskStore(const string& root_directory) :
    DiskStore(root_directory), cache_root(), stats(3), cache_directory_count(0),
    cache_file_count(0) { }

bool CachedDiskStore::create_cache_subdirectory_locked(CachedDirectoryContents* level,
    const string& item) {
  if (level->subdirectories.emplace(piecewise_construct,
      forward_as_tuple(item), forward_as_tuple(new CachedDirectoryContents())).second) {
    this->cache_directory_count++;
    this->stats[0].cache_directory_creates++;
    level->subdirectories_lru.insert(item, 0);
    return true;

  } else {
    level->subdirectories_lru.touch(item);
    return false;
  }
}

bool CachedDiskStore::create_cache_file_locked(CachedDirectoryContents* level,
    const string& item, const string& filesystem_path) {
  if (level->files.emplace(item, filesystem_path).second) {
    this->cache_file_count++;
    this->stats[0].cache_file_creates++;
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
      this->stats[0].cache_directory_hits++;
      guards.emplace_back(move(g));
    }

    // if we get here, then all the cache directories exist; delete the file
    if (!p.basename.empty()) {
      string filename = this->filename_for_path(p);
      rw_guard g(levels.back()->files_lock, true);
      if (!isfile(filename)) {
        if (levels.back()->files.erase(p.basename)) {
          this->stats[0].cache_file_deletes++;
        }
        levels.back()->files_lru.erase(p.basename);
      }
    }
  } catch (const out_of_range& e) {
    this->stats[0].cache_directory_misses++;

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
    string filename = this->root_directory;
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
      {
        rw_guard g(parent_level->subdirectories_lock, true);
        auto dir_it = parent_level->subdirectories.find(p.directories.back());
        if ((dir_it != parent_level->subdirectories.end()) && !isdir(filename)) {
          auto counts = dir_it->second->get_counts();
          this->stats[0].report_directory_delete(counts.first, counts.second);
          this->cache_directory_count -= counts.first;
          this->cache_file_count -= counts.second;
          parent_level->subdirectories.erase(dir_it);
          parent_level->subdirectories_lru.erase(p.directories.back());
        }
      }

      // we deleted a directory - move up to the previous one
      levels.pop_back();
      p.directories.pop_back();
    }
  }
}

unordered_map<string, string> CachedDiskStore::update_metadata(
    const SeriesMetadataMap& metadata_map, bool create_new,
    UpdateMetadataBehavior update_behavior) {

  unordered_map<string, string> ret;
  for (auto& it : metadata_map) {
    const auto& key_name = it.first;
    const auto& metadata = it.second;

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
        this->stats[0].cache_file_hits++;
      } else {
        this->stats[0].cache_file_misses++;

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
          t.level->touch_file(p.basename);

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
          s.cache_file_creates++;

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

unordered_map<string, string> CachedDiskStore::delete_series(
    const vector<string>& key_names) {

  unordered_map<string, string> ret;
  for (auto key_name : key_names) {

    try {
      KeyPath p(key_name);

      // delete the file on disk
      string filename = this->filename_for_key(key_name);
      try {
        unlink(filename);
        this->stats[0].series_deletes++;
      } catch (const runtime_error& e) { }

      // note: we don't need to explicitly close the fd in WhisperArchive's file
      // cache; it should be closed in the WhisperArchive destructor, which is
      // indirectly called in check_and_delete_cache_path
      this->check_and_delete_cache_path(p);

      ret.emplace(key_name, "");

    } catch (const exception& e) {
      ret.emplace(key_name, e.what());
    }
  }

  return ret;
}

unordered_map<string, unordered_map<string, ReadResult>> CachedDiskStore::read(
    const vector<string>& key_names, int64_t start_time, int64_t end_time) {

  unordered_map<string, string> key_to_pattern = this->resolve_patterns(key_names);

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
        r.data = t.archive->read(start_time, end_time);
      }
      r.metadata = this->convert_metadata_to_thrift(*t.archive->get_metadata());

    } catch (const out_of_range& e) {
      // one of the directories doesn't exist
      r.error = "series does not exist";

    } catch (const cannot_open_file& e) {
      if (e.error == ENOENT) {
        // apparently the file was deleted; remove it from the cache
        this->check_and_delete_cache_path(p);
        r.error = "series does not exist";
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
    const unordered_map<string, Series>& data) {
  unordered_map<string, string> ret;
  for (auto& it : data) {
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
    const vector<string>& patterns) {

  unordered_map<string, FindResult> ret;
  for (const auto& pattern : patterns) {
    // fprintf(stderr, "[CachedDiskStore:find:%s] begin\n", pattern.c_str());

    auto emplace_ret = ret.emplace(piecewise_construct,
        forward_as_tuple(pattern), forward_as_tuple());
    if (!emplace_ret.second) {
      // fprintf(stderr, "[CachedDiskStore:find:%s] duplicate\n", pattern.c_str());
      continue;
    }
    FindResult& r = emplace_ret.first->second;

    if (this->pattern_is_indeterminate(pattern)) {
      r.error = "pattern is indeterminate";
      // fprintf(stderr, "[CachedDiskStore:find:%s] indeterminate\n", pattern.c_str());
      continue;
    }

    try {
      KeyPath p(pattern);

      unordered_map<CachedDirectoryContents*, string> current_levels, next_levels;
      current_levels.emplace(&this->cache_root, "");
      rw_guard_multi guards;

      for (const auto& item : p.directories) {
        // fprintf(stderr, "[CachedDiskStore:find:%s] token: %s\n", pattern.c_str(), item.c_str());

        for (auto& level_it : current_levels) {
          CachedDirectoryContents* level = level_it.first;
          const auto& current_level_path = level_it.second;

          // fprintf(stderr, "[CachedDiskStore:find:%s:%s] moving from level %p\n", pattern.c_str(), item.c_str(), level);

          // if the current token is a pattern, we'll have to scan the current level
          if (this->token_is_pattern(item)) {
            // make sure the current level is complete
            // fprintf(stderr, "[CachedDiskStore:find:%s:%s] populating level %p\n", pattern.c_str(), item.c_str(), level);
            this->populate_cache_level(level, this->filename_for_key(current_level_path, false));

            // scan through its directories for the ones we want
            bool keep_lock = false;
            rw_guard g(level->subdirectories_lock, false);
            for (auto& it : level->subdirectories) {
              if (this->name_matches_pattern(it.first, item)) {
                string next_level_path = path_join(current_level_path, it.first);
                next_levels.emplace(piecewise_construct, forward_as_tuple(it.second.get()),
                    forward_as_tuple(next_level_path));
                // fprintf(stderr, "[CachedDiskStore:find:%s:%s] %p -> %p\n", pattern.c_str(), item.c_str(), level, next_level_path.c_str());
                level->touch_subdirectory(it.first);
                keep_lock = true;
              }
            }

            // if we found anything, keep the read lock so we can safely examine the
            // subtree
            if (keep_lock) {
              guards.add(move(g));
            }

            // fprintf(stderr, "[CachedDiskStore:find:%s:%s] %s lock on %p\n", pattern.c_str(), item.c_str(), keep_lock ? "holding" : "releasing", level);

          // this token isn't a pattern; we can directly look up the subdirectory
          // (and not populate + scan the current directory)
          } else {
            // fprintf(stderr, "[CachedDiskStore:find:%s:%s] token is not a pattern\n", pattern.c_str(), item.c_str());
            try {
              rw_guard g(level->subdirectories_lock, false);
              CachedDirectoryContents* next_level = level->subdirectories.at(item).get();

              // fprintf(stderr, "[CachedDiskStore:find:%s:%s] cache directory hit\n", pattern.c_str(), item.c_str());
              level->touch_subdirectory(item);
              guards.add(move(g));

              string next_level_path = path_join(current_level_path, item);
              // fprintf(stderr, "[CachedDiskStore:find:%s:%s] %p -> %p\n", pattern.c_str(), item.c_str(), level, next_level_path.c_str());
              next_levels.emplace(next_level, next_level_path);
              this->stats[0].cache_directory_hits++;

            } catch (const out_of_range& e) {
              this->stats[0].cache_directory_misses++;
              // fprintf(stderr, "[CachedDiskStore:find:%s:%s] cache directory miss\n", pattern.c_str(), item.c_str());

              // if the subdirectory isn't in the cache and the list isn't complete
              // for this level, check the filesystem
              if (!level->list_complete) {
                string new_level_path = path_join(current_level_path, item);
                if (isdir(this->filename_for_key(new_level_path, false))) {
                  rw_guard g(level->subdirectories_lock, true);
                  // fprintf(stderr, "[CachedDiskStore:find:%s:%s] creating cache directory %s\n", pattern.c_str(), item.c_str(), new_level_path.c_str());
                  this->create_cache_subdirectory_locked(level, item);
                } else {
                  // fprintf(stderr, "[CachedDiskStore:find:%s:%s] cache directory %s exists\n", pattern.c_str(), item.c_str(), new_level_path.c_str());
                }

                // it's possible that a delete occurred between the emplace above
                // and this line - if so, just pretend it doesn't exist (since it
                // was deleted anyway)
                try {
                  guards.add(level->subdirectories_lock, false);
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

        // fprintf(stderr, "[CachedDiskStore:find:%s] examining level %p with token %s\n", pattern.c_str(), level, p.basename.c_str());

        if (this->token_is_pattern(p.basename)) {
          // fprintf(stderr, "[CachedDiskStore:find:%s] token is a pattern; populating level\n", pattern.c_str());
          this->populate_cache_level(level, this->filename_for_key(level_path, false));

          {
            rw_guard g(level->subdirectories_lock, false);
            for (const auto& it : level->subdirectories) {
              if (this->name_matches_pattern(it.first, p.basename)) {
                // fprintf(stderr, "[CachedDiskStore:find:%s] check dir: %s (match)\n", pattern.c_str(), it.first.c_str());
                level->touch_subdirectory(it.first);
                r.results.emplace_back(path_join(level_path, it.first) + ".*");
              } else {
                // fprintf(stderr, "[CachedDiskStore:find:%s] check dir: %s (no match)\n", pattern.c_str(), it.first.c_str());
              }
            }
          }

          {
            rw_guard g(level->files_lock, false);
            for (const auto& it : level->files) {
              if (this->name_matches_pattern(it.first, p.basename)) {
                // fprintf(stderr, "[CachedDiskStore:find:%s] check file: %s (match)\n", pattern.c_str(), it.first.c_str());
                r.results.emplace_back(path_join(level_path, it.first));
              } else {
                // fprintf(stderr, "[CachedDiskStore:find:%s] check file: %s (no match)\n", pattern.c_str(), it.first.c_str());
              }
            }
          }

        } else {
          bool directory_in_cache = false, file_in_cache = false;

          {
            rw_guard g(level->subdirectories_lock, false);
            directory_in_cache = level->subdirectories.count(p.basename);
            if (directory_in_cache) {
              this->stats[0].cache_directory_hits++;
              level->touch_subdirectory(p.basename);
            } else {
              this->stats[0].cache_directory_misses++;
            }
          }
          if (directory_in_cache) {
            r.results.emplace_back(path_join(level_path, p.basename) + ".*");
          } else if (!level->list_complete && isdir(this->filename_for_key(path_join(level_path, p.basename), false))) {
            r.results.emplace_back(path_join(level_path, p.basename) + ".*");
            rw_guard g(level->subdirectories_lock, true);
            this->create_cache_subdirectory_locked(level, p.basename);
          }

          {
            rw_guard g(level->files_lock, false);
            file_in_cache = level->files.count(p.basename);
          }
          if (file_in_cache) {
            this->stats[0].cache_file_hits++;
            r.results.emplace_back(path_join(level_path, p.basename));

          } else if (!level->list_complete) {
            this->stats[0].cache_file_misses++;

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
  const CacheStats& current_stats = this->stats[0];

  if (rotate) {
    this->stats.rotate();

    uint64_t n = now();
    this->stats[0].start_time = n;
    this->stats[1].duration = n - this->stats[1].start_time;
  }

  return current_stats.to_map();
}

int64_t CachedDiskStore::delete_from_cache(const string& path) {
  // if the path is empty, delete the ENTIRE cache
  if (path.empty() || (path == "*")) {
    CachedDirectoryContents old_root;

    {
      rw_guard_multi guards;
      guards.add(this->cache_root.subdirectories_lock, true);
      guards.add(this->cache_root.files_lock, true);

      this->cache_root.subdirectories.swap(old_root.subdirectories);
      this->cache_root.files.swap(old_root.files);
      this->cache_root.subdirectories_lru.swap(old_root.subdirectories_lru);
      this->cache_root.files_lru.swap(old_root.files_lru);
    }

    auto counts = old_root.get_counts();
    return counts.first + counts.second;
  }

  // path isn't empty - we're deleting only part of the cache
  KeyPath p(path);
  rw_guard_multi guards;
  CachedDirectoryContents* level = &this->cache_root;

  // move to the directory containing the key we want to delete
  for (const auto& item : p.directories) {
    try {
      rw_guard g(level->subdirectories_lock, false);
      level = level->subdirectories.at(item).get();
      guards.add(move(g));
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
      items_deleted++;
    }
  }

  return items_deleted;
}

string CachedDiskStore::str() const {
  string s = "CachedDiskStore(" + this->root_directory + ", cache_state=" + this->cache_root.str() + ")";
  return s;
}

void CachedDiskStore::populate_cache_level(CachedDirectoryContents* level,
    const string& filesystem_path) {
  if (level->list_complete) {
    return;
  }

  // unfortunately we have to hold the write lock while listing the directory to
  // prevent create/delete races causing cache inconsistency
  rw_guard_multi g(2);
  g.add(level->subdirectories_lock, true);
  g.add(level->files_lock, true);

  // add missing subdirectories and files
  auto filesystem_items = list_directory(filesystem_path);
  for (const auto& item : filesystem_items) {
    string item_filesystem_path = filesystem_path + "/" + item;
    // fprintf(stderr, "[populate_cache_level:%p] examining %s\n", level, item_filesystem_path.c_str());

    auto st = stat(item_filesystem_path);
    // note that we already hold both locks for writing
    if (ends_with(item, ".wsp") && isfile(st)) {
      string key_name = item.substr(0, item.size() - 4);
      // fprintf(stderr, "[populate_cache_level:%p] creating cache file %s -> %s\n", level, key_name.c_str(), item_filesystem_path.c_str());
      this->create_cache_file_locked(level, key_name, item_filesystem_path);
    } else if (isdir(st)) {
      // fprintf(stderr, "[populate_cache_level:%p] creating cache directory %s\n", level, item.c_str());
      this->create_cache_subdirectory_locked(level, item);
    }
  }

  // remove subdirectories and files that have been deleted from the filesystem.
  // we don't take the LRU locks because we're already holding the write locks
  // for the subdirectories and files maps - nobody else can touch the LRUs
  // anyway
  for (auto it = level->files.begin(); it != level->files.end();) {
    string filename = it->first + ".wsp";
    if (!filesystem_items.count(filename)) {
      // fprintf(stderr, "[populate_cache_level:%p] file %s was deleted\n", level, filename.c_str());
      level->files_lru.erase(it->first);
      it = level->files.erase(it);
      this->stats[0].cache_file_deletes++;
      this->cache_file_count--;
    } else {
      // fprintf(stderr, "[populate_cache_level:%p] file %s still exists\n", level, filename.c_str());
      it++;
    }
  }
  for (auto it = level->subdirectories.begin(); it != level->subdirectories.end();) {
    if (!filesystem_items.count(it->first)) {
      // fprintf(stderr, "[populate_cache_level:%p] directory %s was deleted\n", level, it->first.c_str());
      level->subdirectories_lru.erase(it->first);
      auto counts = it->second->get_counts();
      it = level->subdirectories.erase(it);

      this->stats[0].report_directory_delete(counts.first, counts.second);
      this->cache_directory_count -= counts.first;
      this->cache_file_count -= counts.second;
    } else {
      // fprintf(stderr, "[populate_cache_level:%p] directory %s still exists\n", level, it->first.c_str());
      it++;
    }
  }

  // we don't have to do this again
  level->list_complete = true;
  this->stats[0].cache_directory_populates++;
}

CachedDiskStore::CacheTraversal CachedDiskStore::traverse_cache_tree(
    const vector<string>& cache_path, bool create) {

  CacheTraversal t(&this->cache_root, this->root_directory, cache_path.size() + 1);

  // walk the cache layers to find the directory that contains this key
  for (const auto& item : cache_path) {
    t.filesystem_path += '/';
    t.filesystem_path += item;

    try {
      t.move_to_level(item);
      this->stats[0].cache_directory_hits++;

    } catch (const out_of_range& e) {
      this->stats[0].cache_directory_misses++;

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
        this->create_cache_subdirectory_locked(t.level, item);
      }
      t.move_to_level(item);
    }
  }

  return t;
}

CachedDiskStore::CacheTraversal CachedDiskStore::traverse_cache_tree(
    const KeyPath& p, const SeriesMetadata* metadata_to_create) {
  CacheTraversal t = this->traverse_cache_tree(p.directories,
      metadata_to_create != NULL);

  t.filesystem_path += "/";
  t.filesystem_path += p.basename;
  t.filesystem_path += ".wsp";

  try {
    // get the archive object; if we do so successfully, keep holding the read
    // lock
    rw_guard g(t.level->files_lock, false);
    t.archive = &t.level->files.at(p.basename);
    t.level->touch_file(p.basename);
    t.guards.add(move(g));
    this->stats[0].cache_file_hits++;

  } catch (const out_of_range& e) {
    this->stats[0].cache_file_misses++;

    // if the list isn't complete and the file exists, populate it in the cache
    // and return it
    if (!t.level->list_complete && isfile(t.filesystem_path)) {
      {
        rw_guard g(t.level->files_lock, true);
        this->create_cache_file_locked(t.level, p.basename, t.filesystem_path);
      }
      t.guards.add(t.level->files_lock, false);
      t.archive = &t.level->files.at(p.basename);
      return t;
    }

    // at this point the presence/absence of the file in the cache and on the
    // filesystem are the same - it exists in both or neither. and since we
    // already checked the cache, we know it exists in neither.
    if (!metadata_to_create) {
      throw out_of_range(string_printf("%s does not exist (%s)",
          t.filesystem_path.c_str(), t.level->list_complete ? "cached" : "uncached"));
    }

    // yay we can autocreate it
    {
      rw_guard g(t.level->files_lock, true);
      if (t.level->files.emplace(piecewise_construct, forward_as_tuple(p.basename),
          forward_as_tuple(t.filesystem_path, metadata_to_create->archive_args,
              metadata_to_create->x_files_factor, metadata_to_create->agg_method)).second) {
        this->cache_file_count++;
        this->stats[0].cache_file_creates++;
        this->stats[0].series_autocreates++;
        t.level->files_lru.insert(p.basename, 0);
      } else {
        t.level->files_lru.touch(p.basename);
      }
    }
    t.guards.add(t.level->files_lock, false);
    t.archive = &t.level->files.at(p.basename);
  }

  return t;
}

CachedDiskStore::CacheStats::CacheStats() : Stats(),
    cache_directory_hits(0),
    cache_directory_misses(0),
    cache_directory_creates(0),
    cache_directory_deletes(0),
    cache_directory_populates(0),
    cache_file_hits(0),
    cache_file_misses(0),
    cache_file_creates(0),
    cache_file_deletes(0) { }

CachedDiskStore::CacheStats& CachedDiskStore::CacheStats::operator=(
      const CachedDiskStore::CacheStats& other) {
  Stats::operator=(other);

  this->cache_directory_hits = other.cache_directory_hits.load();
  this->cache_directory_misses = other.cache_directory_misses.load();
  this->cache_directory_creates = other.cache_directory_creates.load();
  this->cache_directory_deletes = other.cache_directory_deletes.load();
  this->cache_directory_populates = other.cache_directory_populates.load();
  this->cache_file_hits = other.cache_file_hits.load();
  this->cache_file_misses = other.cache_file_misses.load();
  this->cache_file_creates = other.cache_file_creates.load();
  this->cache_file_deletes = other.cache_file_deletes.load();

  return *this;
}

unordered_map<string, int64_t> CachedDiskStore::CacheStats::to_map() const {
  auto ret = this->Stats::to_map();
  ret.emplace("cache_directory_hits", this->cache_directory_hits.load());
  ret.emplace("cache_directory_misses", this->cache_directory_misses.load());
  ret.emplace("cache_directory_creates", this->cache_directory_creates.load());
  ret.emplace("cache_directory_deletes", this->cache_directory_deletes.load());
  ret.emplace("cache_directory_populates", this->cache_directory_populates.load());
  ret.emplace("cache_file_hits", this->cache_file_hits.load());
  ret.emplace("cache_file_misses", this->cache_file_misses.load());
  ret.emplace("cache_file_creates", this->cache_file_creates.load());
  ret.emplace("cache_file_deletes", this->cache_file_deletes.load());
  return ret;
}
