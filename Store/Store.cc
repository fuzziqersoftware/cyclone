#include "Store.hh"

#include <phosg/Strings.hh>

#include "Whisper.hh"

using namespace std;


void Store::set_autocreate_rules(
    const vector<pair<string, SeriesMetadata>> autocreate_rules) {
  this->validate_autocreate_rules(autocreate_rules);
  auto new_rules = autocreate_rules;
  {
    rw_guard g(this->autocreate_rules_lock, true);
    this->autocreate_rules.swap(new_rules);
  }
}

void Store::flush()  { }

unordered_map<string, int64_t> Store::get_stats(bool rotate) {
  return unordered_map<string, int64_t>();
}

int64_t Store::delete_from_cache(const std::string& paths) {
  return 0;
}

int64_t Store::delete_pending_writes(const std::string& paths) {
  return 0;
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

unordered_map<string, string> Store::resolve_patterns(
    const vector<string>& key_names) {

  // if some of the key names are patterns, execute find queries on them to get
  // the actual key names
  unordered_map<string, string> key_to_pattern;
  vector<string> patterns;
  for (const string& key_name : key_names) {
    if (this->token_is_pattern(key_name)) {
      patterns.emplace_back(key_name);
    } else {
      key_to_pattern.emplace(key_name, key_name);
    }
  }

  if (!patterns.empty()) {
    for (auto it : this->find(patterns)) {
      if (!it.second.error.empty()) {
        continue;
      }
      for (auto& k : it.second.results) {
        key_to_pattern.emplace(k, it.first);
      }
    }
  }

  return key_to_pattern;
}
