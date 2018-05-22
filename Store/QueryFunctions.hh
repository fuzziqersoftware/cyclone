#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "../gen-cpp/Cyclone.h"

#include "QueryParser.hh"


typedef std::unordered_map<std::string, ReadResult> (*SeriesFunction)(std::vector<Query>&);

SeriesFunction get_query_function(const std::string& function_name);
