#include "Store.hh"

#include <phosg/Strings.hh>
#include <phosg/Time.hh>

#include "Whisper.hh"

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

void Store::flush() { }

unordered_map<string, int64_t> Store::get_stats(bool rotate) {
  return unordered_map<string, int64_t>();
}

int64_t Store::delete_from_cache(const std::string& paths, bool local_only) {
  return 0;
}

int64_t Store::delete_pending_writes(const std::string& paths, bool local_only) {
  return 0;
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
  if (it != m.end()) {
    return ret + string_printf("] + <%zu more items>", limit - count);
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
  if (it != m.end()) {
    return ret + string_printf("] + <%zu more items>", limit - count);
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
    bool local_only) {
  string series_list = comma_list_limit(patterns, 10);
  return string_printf("delete_series(%s, local_only=%s)", series_list.c_str(),
      string_for_bool(local_only));
}

string Store::string_for_rename_series(
    const unordered_map<string, string>& renames, bool local_only) {
  string series_list = comma_list_limit(renames, 10);
  return string_printf("rename_series(%s, local_only=%s)", series_list.c_str(),
      string_for_bool(local_only));
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
  // allowed characters are [a-zA-Z0-9.:_-]
  // if the key contains any other character, it's not valid
  for (uint8_t ch : key_name) {
    if (!VALID_CHARS[ch]) {
      return false;
    }
  }
  return true;
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

unordered_map<string, vector<string>> Store::resolve_patterns(
    const vector<string>& key_names, bool local_only,
    BaseFunctionProfiler* profiler) {

  // if some of the key names are patterns, execute find queries on them to get
  // the actual key names
  unordered_map<string, vector<string>> key_to_patterns;
  vector<string> patterns;
  for (const string& key_name : key_names) {
    if (this->token_is_pattern(key_name)) {
      patterns.emplace_back(key_name);
    } else {
      key_to_patterns[key_name].emplace_back(key_name);
    }
  }

  if (!patterns.empty()) {
    for (auto it : this->find(patterns, local_only, profiler)) {
      if (!it.second.error.empty()) {
        continue;
      }
      for (auto& k : it.second.results) {
        key_to_patterns[k].emplace_back(it.first);
      }
    }
  }

  return key_to_patterns;
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
