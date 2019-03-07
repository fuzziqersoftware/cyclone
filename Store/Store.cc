#include "Store.hh"

#include <algorithm>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>

#include "Formats/Whisper.hh"
#include "Utils/Errors.hh"

using namespace std;


void Store::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>>& autocreate_rules) {
  this->validate_autocreate_rules(autocreate_rules);
  auto new_rules = autocreate_rules;
  {
    rw_guard g(this->autocreate_rules_lock, true);
    this->autocreate_rules.swap(new_rules);
  }
}

void Store::flush(StoreTaskManager* m) { }

unordered_map<string, int64_t> Store::get_stats(bool rotate) {
  return unordered_map<string, int64_t>();
}



std::shared_ptr<Store::UpdateMetadataTask> Store::update_metadata(
    StoreTaskManager* m, const SeriesMetadataMap& metadata,
    UpdateMetadataBehavior behavior, bool create_new, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  shared_ptr<UpdateMetadataArguments> args(new UpdateMetadataArguments());
  args->metadata = metadata;
  args->behavior = behavior;
  args->create_new = create_new;
  args->skip_buffering = skip_buffering;
  args->local_only = local_only;
  if (m) {
    return this->update_metadata(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->update_metadata(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::UpdateMetadataTask> Store::update_metadata(
    StoreTaskManager* m, SeriesMetadataMap&& metadata,
    UpdateMetadataBehavior behavior, bool create_new, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  shared_ptr<UpdateMetadataArguments> args(new UpdateMetadataArguments());
  args->metadata = move(metadata);
  args->behavior = behavior;
  args->create_new = create_new;
  args->skip_buffering = skip_buffering;
  args->local_only = local_only;
  if (m) {
    return this->update_metadata(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->update_metadata(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::DeleteSeriesTask> Store::delete_series(
    StoreTaskManager* m, const std::vector<std::string>& patterns,
    bool deferred, bool local_only, BaseFunctionProfiler* profiler) {
  shared_ptr<DeleteSeriesArguments> args(new DeleteSeriesArguments());
  args->patterns = patterns;
  args->deferred = deferred;
  args->local_only = local_only;
  if (m) {
    return this->delete_series(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->delete_series(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::DeleteSeriesTask> Store::delete_series(
    StoreTaskManager* m, std::vector<std::string>&& patterns, bool deferred,
    bool local_only, BaseFunctionProfiler* profiler) {
  shared_ptr<DeleteSeriesArguments> args(new DeleteSeriesArguments());
  args->patterns = move(patterns);
  args->deferred = deferred;
  args->local_only = local_only;
  if (m) {
    return this->delete_series(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->delete_series(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::RenameSeriesTask> Store::rename_series(
    StoreTaskManager* m,
    const std::unordered_map<std::string, std::string>& renames, bool merge,
    bool local_only, BaseFunctionProfiler* profiler) {
  shared_ptr<RenameSeriesArguments> args(new RenameSeriesArguments());
  args->renames = renames;
  args->merge = merge;
  args->local_only = local_only;
  if (m) {
    return this->rename_series(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->rename_series(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::RenameSeriesTask> Store::rename_series(
    StoreTaskManager* m,
    std::unordered_map<std::string, std::string>&& renames, bool merge,
    bool local_only, BaseFunctionProfiler* profiler) {
  shared_ptr<RenameSeriesArguments> args(new RenameSeriesArguments());
  args->renames = move(renames);
  args->merge = merge;
  args->local_only = local_only;
  if (m) {
    return this->rename_series(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->rename_series(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::ReadTask> Store::read(
    StoreTaskManager* m, const std::vector<std::string>& key_names,
    int64_t start_time, int64_t end_time, bool local_only,
    BaseFunctionProfiler* profiler) {
  shared_ptr<ReadArguments> args(new ReadArguments());
  args->key_names = key_names;
  args->start_time = start_time;
  args->end_time = end_time;
  args->local_only = local_only;
  if (m) {
    return this->read(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->read(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::ReadTask> Store::read(
    StoreTaskManager* m, std::vector<std::string>&& key_names,
    int64_t start_time, int64_t end_time, bool local_only,
    BaseFunctionProfiler* profiler) {
  shared_ptr<ReadArguments> args(new ReadArguments());
  args->key_names = move(key_names);
  args->start_time = start_time;
  args->end_time = end_time;
  args->local_only = local_only;
  if (m) {
    return this->read(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->read(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::ReadAllTask> Store::read_all(
    StoreTaskManager* m, const std::string& key_name, bool local_only,
    BaseFunctionProfiler* profiler) {
  shared_ptr<ReadAllArguments> args(new ReadAllArguments());
  args->key_name = key_name;
  args->local_only = local_only;
  if (m) {
    return this->read_all(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->read_all(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::ReadAllTask> Store::read_all(
    StoreTaskManager* m, std::string&& key_name, bool local_only,
    BaseFunctionProfiler* profiler) {
  shared_ptr<ReadAllArguments> args(new ReadAllArguments());
  args->key_name = move(key_name);
  args->local_only = local_only;
  if (m) {
    return this->read_all(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->read_all(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::WriteTask> Store::write(
    StoreTaskManager* m,
    const std::unordered_map<std::string, Series>& data, bool skip_buffering,
    bool local_only, BaseFunctionProfiler* profiler) {
  shared_ptr<WriteArguments> args(new WriteArguments());
  args->data = data;
  args->skip_buffering = skip_buffering;
  args->local_only = local_only;
  if (m) {
    return this->write(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->write(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::WriteTask> Store::write(
    StoreTaskManager* m, std::unordered_map<std::string, Series>&& data,
    bool skip_buffering, bool local_only, BaseFunctionProfiler* profiler) {
  shared_ptr<WriteArguments> args(new WriteArguments());
  args->data = move(data);
  args->skip_buffering = skip_buffering;
  args->local_only = local_only;
  if (m) {
    return this->write(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->write(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::FindTask> Store::find(
    StoreTaskManager* m, const std::vector<std::string>& patterns,
    bool local_only, BaseFunctionProfiler* profiler) {
  shared_ptr<FindArguments> args(new FindArguments());
  args->patterns = patterns;
  args->local_only = local_only;
  if (m) {
    return this->find(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->find(&m, args, profiler);
    m.run(task);
    return task;
  }
}

std::shared_ptr<Store::FindTask> Store::find(
    StoreTaskManager* m, std::vector<std::string>&& patterns,
    bool local_only, BaseFunctionProfiler* profiler) {
  shared_ptr<FindArguments> args(new FindArguments());
  args->patterns = move(patterns);
  args->local_only = local_only;
  if (m) {
    return this->find(m, args, profiler);
  } else {
    StoreTaskManager m;
    auto task = this->find(&m, args, profiler);
    m.run(task);
    return task;
  }
}



static const char* string_for_update_metadata_behavior(
    Store::UpdateMetadataBehavior b) {
  switch (b) {
    case Store::UpdateMetadataBehavior::Ignore:
      return "Ignore";
    case Store::UpdateMetadataBehavior::Update:
      return "Update";
    case Store::UpdateMetadataBehavior::Recreate:
      return "Recreate";
  }
  return "Unknown";
}

static const char* string_for_bool(bool b) {
  return (b ? "true" : "false");
}

template <typename V>
static string comma_list_limit(const unordered_map<string, V>& m, size_t limit) {
  if (m.size() == 0) {
    return "[]";
  }

  auto it = m.begin();
  string ret = "[" + it->first;
  size_t count = 1;
  for (it++; (it != m.end()) && (count < limit); it++, count++) {
    ret += ", ";
    ret += it->first;
  }
  if (count < m.size()) {
    return ret + string_printf("] + <%zu more items>", m.size() - count);
  }
  return ret + "]";
}

static string comma_list_limit(const vector<string>& m, size_t limit) {
  if (m.size() == 0) {
    return "[]";
  }

  auto it = m.begin();
  string ret = "[" + *it;
  size_t count = 1;
  for (it++; (it != m.end()) && (count < limit); it++, count++) {
    ret += ", ";
    ret += *it;
  }
  if (count < m.size()) {
    return ret + string_printf("] + <%zu more items>", m.size() - count);
  }
  return ret + "]";
}

string Store::string_for_update_metadata(const SeriesMetadataMap& metadata,
    bool create_new, UpdateMetadataBehavior update_behavior,
    bool skip_buffering, bool local_only) {
  string series_list = comma_list_limit(metadata, 10);
  return string_printf("update_metadata(%s, create_new=%s, update_behavior=%s, skip_buffering=%s, local_only=%s)",
      series_list.c_str(), string_for_bool(create_new),
      string_for_update_metadata_behavior(update_behavior),
      string_for_bool(skip_buffering), string_for_bool(local_only));
}

string Store::string_for_delete_series(const vector<string>& patterns,
    bool deferred, bool local_only) {
  string series_list = comma_list_limit(patterns, 10);
  return string_printf("delete_series(%s, deferred=%s, local_only=%s)",
      series_list.c_str(), string_for_bool(deferred),
      string_for_bool(local_only));
}

string Store::string_for_rename_series(
    const unordered_map<string, string>& renames, bool merge, bool local_only) {
  string series_list = comma_list_limit(renames, 10);
  return string_printf("rename_series(%s, merge=%s, local_only=%s)",
      series_list.c_str(), string_for_bool(merge), string_for_bool(local_only));
}

string Store::string_for_read(const vector<string>& key_names,
    int64_t start_time, int64_t end_time, bool local_only) {
  string series_list = comma_list_limit(key_names, 10);
  return string_printf("read(%s, %" PRId64 ", %" PRId64 ", local_only=%s)",
      series_list.c_str(), start_time, end_time, string_for_bool(local_only));
}

string Store::string_for_read_all(const string& key_name, bool local_only) {
  return string_printf("read_all(%s, local_only=%s)", key_name.c_str(),
      string_for_bool(local_only));
}

string Store::string_for_write(const unordered_map<string, Series>& data,
    bool skip_buffering, bool local_only) {
  string series_list = comma_list_limit(data, 10);
  return string_printf("write(%s, skip_buffering=%s, local_only=%s)",
      series_list.c_str(), string_for_bool(skip_buffering),
      string_for_bool(local_only));
}

string Store::string_for_find(const vector<string>& patterns, bool local_only) {
  string series_list = comma_list_limit(patterns, 10);
  return string_printf("find(%s, local_only=%s)", series_list.c_str(),
      string_for_bool(local_only));
}



bool Store::token_is_pattern(const string& token) {
  return token.find_first_of("[]{}*") != string::npos;
}

bool Store::pattern_is_basename(const string& pattern) {
  return (pattern.find('.') == string::npos) &&
      !Store::pattern_is_indeterminate(pattern);
}

bool Store::pattern_is_indeterminate(const string& pattern) {
  return pattern.find("**") != string::npos;
}

bool Store::name_matches_pattern(const string& name, const string& pattern,
    size_t name_offset, size_t pattern_offset) {

  bool pattern_is_blank = (pattern.size() <= pattern_offset);
  bool name_is_blank = (name.size() <= name_offset);
  if (pattern_is_blank && name_is_blank) {
    return true;
  }
  if (pattern_is_blank || name_is_blank) {
    return false;
  }

  while (name_offset < name.size()) {
    if (pattern[pattern_offset] == '{') {
      // multi-group: {ab,cd}ef matches abef, cdef
      size_t substr_match_len = 0;
      while (pattern_offset < pattern.size() && pattern[pattern_offset] != '}') {
        pattern_offset++;

        size_t end_offset;
        for (end_offset = pattern_offset; end_offset < pattern.size() && pattern[end_offset] != ',' && pattern[end_offset] != '}'; end_offset++);

        size_t substr_length = end_offset - pattern_offset;
        if (!name.compare(name_offset, substr_length, pattern, pattern_offset, substr_length)) {
          substr_match_len = max(substr_match_len, substr_length);
        }

        pattern_offset = end_offset;
      }
      if (pattern_offset == pattern.size()) {
        throw runtime_error("pattern has unterminated substring set");
      }
      if (!substr_match_len) {
        return false;
      }
      pattern_offset++;
      name_offset += substr_match_len;

    } else if (pattern[pattern_offset] == '[') {
      // char-group: [abcd]ef matches aef, bef, cef, def
      bool char_matched = false;
      for (pattern_offset++; pattern_offset < pattern.size() && pattern[pattern_offset] != ']'; pattern_offset++) {
        if (pattern[pattern_offset] == name[name_offset]) {
          char_matched = true;
        }
      }
      if (pattern_offset == pattern.size()) {
        throw runtime_error("pattern has unterminated character class");
      }
      if (!char_matched) {
        return false;
      }
      pattern_offset++;
      name_offset++;

    } else if (pattern[pattern_offset] == '*') {
      // * matches anything except . (no subdirectories)
      // ** matches anything, even through subdirectories
      bool match_directories = (pattern.size() > pattern_offset + 1) &&
          (pattern[pattern_offset + 1] == '*');
      pattern_offset += (1 + match_directories);

      for (; name_offset < name.size(); name_offset++) {
        if (Store::name_matches_pattern(name, pattern, name_offset, pattern_offset)) {
          return true;
        }
        // stop if we hit a directory boundary and the pattern is * (not **)
        if (!match_directories && (name[name_offset] == '.')) {
          return false;
        }
      }

    } else {
      if (pattern[pattern_offset] != name[name_offset]) {
        return false;
      }
      pattern_offset++;
      name_offset++;
    }
  }

  // if we get to the end of the name, then it's a match if we also got to the
  // end of the pattern
  return pattern_offset == pattern.size();
}

vector<bool> valid_chars() {
  vector<bool> ret(0x100);

  // allow alphanumeric characters
  for (char ch = 'a'; ch <= 'z'; ch++) {
    ret[ch] = true;
  }
  for (char ch = 'A'; ch <= 'Z'; ch++) {
    ret[ch] = true;
  }
  for (char ch = '0'; ch <= '9'; ch++) {
    ret[ch] = true;
  }

  // allow path separators
  ret['.'] = true;

  // allow some special chars
  ret['_'] = true;
  ret['-'] = true;
  ret[':'] = true;
  ret['@'] = true;
  ret['#'] = true;
  ret['='] = true;

  return ret;
}

static const vector<bool> VALID_CHARS = valid_chars();

bool Store::key_char_is_valid(char ch) {
  return VALID_CHARS[ch];
}

bool Store::key_name_is_valid(const string& key_name) {
  // the empty string is not valid
  if (key_name.empty()) {
    return false;
  }

  // allowed characters are [a-zA-Z0-9.:_-], but . may not appear next to
  // another . and the key cannot begin or end with .
  uint8_t last_ch = '.';
  for (uint8_t ch : key_name) {
    if (!VALID_CHARS[ch]) {
      return false;
    }
    if ((last_ch == '.') && (ch == '.')) {
      return false;
    }
    last_ch = ch;
  }
  return (last_ch != '.');
}

void Store::validate_autocreate_rules(
    const std::vector<std::pair<std::string, SeriesMetadata>> autocreate_rules) {
  for (const auto& it : autocreate_rules) {
    try {
      WhisperArchive::validate_archive_args(it.second.archive_args);
    } catch (const invalid_argument& e) {
      throw invalid_argument(string_printf("Autocreate rule %s is invalid: %s",
          it.first.c_str(), e.what()));
    }
  }
}

SeriesMetadata Store::get_autocreate_metadata_for_key(const string& key_name) {
  {
    rw_guard g(this->autocreate_rules_lock, false);
    for (const auto& rule : this->autocreate_rules) {
      if (this->name_matches_pattern(key_name, rule.first)) {
        return rule.second;
      }
    }
  }

  return SeriesMetadata();
}



Store::ResolvePatternsTask::ResolvePatternsTask(
    StoreTaskManager* m, Store* s, const vector<string>& key_names, bool local_only,
    BaseFunctionProfiler* profiler) : TypedStoreTask(m) {

  // if some of the key names are patterns, execute find queries on them to get
  // the actual key names
  vector<string> patterns;
  for (const string& key_name : key_names) {
    if (Store::token_is_pattern(key_name)) {
      patterns.emplace_back(key_name);
    } else {
      this->return_value[key_name].emplace_back(key_name);
    }
  }

  if (!patterns.empty()) {
    shared_ptr<Store::FindArguments> args(new Store::FindArguments());
    args->patterns = move(patterns);
    args->local_only = local_only;
    this->find_task = s->find(this->manager, args, profiler);
    this->delegate(this->find_task, bind(&ResolvePatternsTask::on_find_complete, this));
  } else {
    this->set_complete();
  }
}

void Store::ResolvePatternsTask::on_find_complete() {
  for (auto& it : this->find_task->value()) {
    if (!it.second.error.description.empty()) {
      continue;
    }
    for (auto& k : it.second.results) {
      this->return_value[k].emplace_back(it.first);
    }
  }
  this->set_complete();
}

shared_ptr<Store::ResolvePatternsTask> Store::resolve_patterns(StoreTaskManager* m,
    const std::vector<std::string>& key_names, bool local_only,
    BaseFunctionProfiler* profiler) {
  return shared_ptr<ResolvePatternsTask>(new ResolvePatternsTask(m, this,
      key_names, local_only, profiler));
}



Store::EmulateRenameSeriesTask::EmulateRenameSeriesTask(StoreTaskManager* m,
    Store* from_store, const std::string& from_key_name, Store* to_store,
    const std::string& to_key_name, bool merge, BaseFunctionProfiler* profiler)
    : TypedStoreTask(m, profiler), from_store(from_store),
      from_key_name(from_key_name), to_store(to_store),
      to_key_name(to_key_name), merge(merge) {
  shared_ptr<Store::ReadAllArguments> args(new Store::ReadAllArguments());
  args->key_name = from_key_name;
  args->local_only = false;
  this->read_all_task = this->from_store->read_all(this->manager, args, this->profiler);
  this->delegate(this->read_all_task, bind(&EmulateRenameSeriesTask::on_read_all_complete, this));
}

void Store::EmulateRenameSeriesTask::on_read_all_complete() {
  this->profiler->checkpoint("read_all_" + this->from_key_name);
  auto& read_all_result = this->read_all_task->value();

  if (!read_all_result.error.description.empty()) {
    this->set_value(move(read_all_result.error));
    return;
  }
  if (read_all_result.metadata.archive_args.empty()) {
    this->set_value(make_error("series does not exist"));
    return;
  }

  // create the series in the remote store if it doesn't exist already. if merge
  // is true, then proceed even if the series exists; if merge is false, fail if
  // the series exists.
  shared_ptr<Store::UpdateMetadataArguments> args(new Store::UpdateMetadataArguments());
  args->metadata.emplace(this->to_key_name, read_all_result.metadata);
  args->behavior = Store::UpdateMetadataBehavior::Ignore;
  args->create_new = true;
  args->skip_buffering = true;
  args->local_only = false;
  this->update_metadata_task = this->to_store->update_metadata(this->manager,
      args, this->profiler);
  this->delegate(this->update_metadata_task, bind(
      &EmulateRenameSeriesTask::on_update_metadata_complete, this));
}

void Store::EmulateRenameSeriesTask::on_update_metadata_complete() {
  profiler->checkpoint("update_metadata_" + this->to_key_name);
  auto& update_metadata_ret = this->update_metadata_task->value();

  try {
    Error& error = update_metadata_ret.at(to_key_name);
    if (!error.description.empty() && (!error.ignored || !merge)) {
      this->set_value(move(error));
      return;
    }
  } catch (const out_of_range&) {
    this->set_value(make_error("update_metadata returned no results"));
    return;
  }

  // write all the data from the from series into the to series
  auto& read_all_result = this->read_all_task->value();
  shared_ptr<Store::WriteArguments> args(new Store::WriteArguments());
  args->data.emplace(this->to_key_name, move(read_all_result.data));
  args->skip_buffering = true;
  args->local_only = false;
  this->write_task = this->to_store->write(this->manager, args, this->profiler);
  this->delegate(this->write_task, bind(
      &EmulateRenameSeriesTask::on_write_complete, this));
}

void Store::EmulateRenameSeriesTask::on_write_complete() {
  profiler->checkpoint("write_" + this->to_key_name);
  auto& write_ret = this->write_task->value();

  try {
    Error& error = write_ret.at(to_key_name);
    if (!error.description.empty()) {
      this->set_value(move(error));
      return;
    }
  } catch (const out_of_range&) {
    this->set_value(make_error("write returned no results"));
    return;
  }

  // delete the original series
  shared_ptr<Store::DeleteSeriesArguments> args(new Store::DeleteSeriesArguments());
  args->patterns.emplace_back(this->from_key_name);
  args->deferred = false;
  args->local_only = false;
  this->delete_task = this->from_store->delete_series(this->manager, args, profiler);
  this->delegate(this->delete_task, bind(&EmulateRenameSeriesTask::on_delete_complete, this));
}

void Store::EmulateRenameSeriesTask::on_delete_complete() {
  profiler->checkpoint("delete_series_" + this->from_key_name);
  auto& delete_ret = this->delete_task->value();

  auto& res = delete_ret[from_key_name];
  if (res.disk_series_deleted + res.buffer_series_deleted <= 0) {
    this->set_value(make_error("move successful, but delete failed"));
  }

  this->set_value(make_success());
}

std::shared_ptr<Store::EmulateRenameSeriesTask> Store::emulate_rename_series(
    StoreTaskManager* m, Store* from_store, const std::string& from_key_name,
    Store* to_store, const std::string& to_key_name, bool merge,
    BaseFunctionProfiler* profiler) {
  return shared_ptr<EmulateRenameSeriesTask>(new EmulateRenameSeriesTask(m,
      from_store, from_key_name, to_store, to_key_name, merge, profiler));
}



Store::Stats::Stats() : start_time(now()), duration(0) { }

Store::Stats& Store::Stats::operator=(const Stats& other) {
  this->start_time = other.start_time.load();
  this->duration = other.duration.load();
  return *this;
}

unordered_map<string, int64_t> Store::Stats::to_map() const {
  unordered_map<string, int64_t> ret;
  ret.emplace("start_time", this->start_time.load());
  ret.emplace("duration", this->duration.load());
  return ret;
}



void Store::combine_simple_results(unordered_map<string, Error>& into,
    unordered_map<string, Error>&& from) {

  for (auto& from_it : from) {
    const string& from_key = from_it.first;
    auto& from_error = from_it.second;

    auto emplace_ret = into.emplace(from_key, from_error);
    if (!emplace_ret.second) {

      // errors take precedence over success, which takes precedence over "ignored"
      auto& into_error = emplace_ret.first->second;
      if ((into_error.ignored) ||
          (into_error.description.empty() && !from_error.ignored)) {
        into_error = from_error;
      }
    }
  }
}

void Store::combine_delete_results(unordered_map<string, DeleteResult>& into,
    unordered_map<string, DeleteResult>&& from) {

  for (auto& from_it : from) {
    const string& from_key = from_it.first;
    auto& from_res = from_it.second;

    auto emplace_ret = into.emplace(from_key, from_res);
    if (!emplace_ret.second) {
      auto& into_res = emplace_ret.first->second;
      into_res.disk_series_deleted += from_res.disk_series_deleted;
      into_res.buffer_series_deleted += from_res.buffer_series_deleted;

      // errors take precedence over success, which takes precedence over "ignored"
      if ((into_res.error.ignored) ||
          (into_res.error.description.empty() && !from_res.error.ignored)) {
        into_res.error = from_res.error;
      }
    }
  }
}

void Store::combine_read_results(
    unordered_map<string, unordered_map<string, ReadResult>>& into,
    unordered_map<string, unordered_map<string, ReadResult>>&& from) {
  // the maps are {pattern: {key_name: result}}
  for (auto& from_query_it : from) { // (pattern, {key_name: result})
    const string& from_query = from_query_it.first;
    auto& from_series_map = from_query_it.second;

    auto into_query_it = into.find(from_query);
    if (into_query_it == into.end()) {
      into.emplace(from_query, move(from_series_map));

    } else {
      auto& into_series_map = into_query_it->second;
      for (auto& from_series_it : from_series_map) { // (key_name, result)

        // attempt to insert the result. if it's already there, then merge the
        // data manually
        auto emplace_ret = into_series_map.emplace(from_series_it.first,
            move(from_series_it.second));
        if (!emplace_ret.second) {
          auto& existing_result = emplace_ret.first->second;
          auto& new_result = from_series_it.second;

          if (!existing_result.error.description.empty()) {
            // the existing result has an error; just leave it there

          } else if (existing_result.step == 0) {
            // the existing result is a missing series result. just replace it
            // entirely with the new result (which might have data)
            existing_result = move(new_result);

          } else if (new_result.step != 0) {
            // both results have data. seriously? dammit

            if (new_result.step != existing_result.step) {
              existing_result.error = make_error("merged results with different schemas");
            } else {
              existing_result.data.insert(existing_result.data.end(),
                  new_result.data.begin(), new_result.data.end());
              sort(existing_result.data.begin(), existing_result.data.end(),
                  [](const Datapoint& a, const Datapoint& b) {
                    return a.timestamp < b.timestamp;
                  });
              if (new_result.start_time < existing_result.start_time) {
                existing_result.start_time = new_result.start_time;
              }
              if (new_result.end_time > existing_result.end_time) {
                existing_result.end_time = new_result.end_time;
              }
            }
          }
        }
      }
    }
  }
}

void Store::combine_find_results(unordered_map<string, FindResult>& into,
    unordered_map<string, FindResult>&& from) {
  for (auto& from_query_it : from) {
    const string& from_query = from_query_it.first;
    auto& from_result = from_query_it.second;
    auto into_query_it = into.find(from_query);
    if (into_query_it == into.end()) {
      into.emplace(from_query, move(from_result));

    } else {
      auto& into_result = into_query_it->second;
      if (!into_result.error.description.empty()) {
        continue;
      } else if (!from_result.error.description.empty()) {
        into_result.error = move(from_result.error);
        into_result.results.clear();
        continue;
      } else {
        bool needs_deduplication = !into_result.results.empty();
        into_result.results.insert(into_result.results.end(),
            make_move_iterator(from_result.results.begin()),
            make_move_iterator(from_result.results.end()));

        if (needs_deduplication) {
          auto& r = into_result.results;
          sort(r.begin(), r.end());
          size_t write_offset = 0;
          for (size_t read_offset = 0; read_offset < r.size();) {
            size_t run_start_offset = read_offset;
            for (read_offset++; (read_offset < r.size()) && (r[read_offset] == r[run_start_offset]); read_offset++);
            if (write_offset != run_start_offset) {
              r[write_offset] = r[run_start_offset];
            }
            write_offset++;
          }
          r.resize(write_offset);
        }
      }
    }
  }
}
