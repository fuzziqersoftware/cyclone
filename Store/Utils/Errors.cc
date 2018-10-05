#include "Errors.hh"

Error make_error(const std::string& description, bool recoverable, bool ignored) {
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

Error make_ignored() {
  Error e;
  e.description = "ignored";
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
