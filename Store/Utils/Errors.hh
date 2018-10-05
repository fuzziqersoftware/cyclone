#pragma once

#include <string>

#include "../../gen-cpp/Cyclone.h"


Error make_error(const std::string& description, bool recoverable = false,
    bool ignored = false);
Error make_error(const char* description, bool recoverable = false,
    bool ignored = false);
Error make_ignored();
Error make_success();
