#pragma once

#include <string>

#include "../../gen-cpp/Cyclone.h"


Error make_error(const std::string& description, bool recoverable = false,
    bool ignored = false);
Error make_error(const char* description, bool recoverable = false,
    bool ignored = false);
Error make_ignored(const char* description = "ignored");
Error make_success();
std::string string_for_error(const Error& e);
