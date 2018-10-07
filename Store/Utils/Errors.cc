#include "Errors.hh"

using namespace std;


Error make_error(const string& description, bool recoverable, bool ignored) {
  Error e;
  e.description = description;
  e.recoverable = recoverable;
  e.ignored = ignored;
  return e;
}

Error make_error(const char* description, bool recoverable, bool ignored) {
  Error e;
  e.description = description;
  e.recoverable = recoverable;
  e.ignored = ignored;
  return e;
}

Error make_ignored(const char* description) {
  Error e;
  e.description = description;
  e.recoverable = false;
  e.ignored = true;
  return e;
}

Error make_success() {
  Error e;
  e.description = "";
  e.recoverable = false;
  e.ignored = false;
  return e;
}

string string_for_error(const Error& e) {
  if (e.ignored) {
    if (e.description == "ignored") {
      return e.description;
    } else {
      return e.description + " (ignored)";
    }
  }
  if (e.recoverable) {
    return e.description + " (recoverable)";
  }
  return e.description;
}
