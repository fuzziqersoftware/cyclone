#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "../../gen-cpp/Cyclone.h"


struct QueryToken {
  enum class Type {
    Dynamic = 0,
    Integer,
    Float,
    String,
    OpenParenthesis,
    CloseParenthesis,
    Comma,
  };

  Type type;
  union {
    int64_t int_data;
    double float_data;
  };
  std::string string_data;

  QueryToken() = delete;
  QueryToken(Type type);
  QueryToken(Type type, int64_t int_data);
  QueryToken(Type type, double float_data);
  QueryToken(Type type, std::string&& string_data);

  std::string str() const;
};

struct Query {
  enum Type {
    // patterns become function calls; there's a function that just reads from
    // the store
    FunctionCall    = 0x01,
    SeriesReference = 0x02,
    Integer         = 0x04,
    Float           = 0x08,
    String          = 0x10,

    Numeric         = 0x06,
    Constant        = 0x1C,
    SeriesOrCall    = 0x03,
  };

  Type type;
  union {
    int64_t int_data;
    double float_data;
  };
  std::string string_data; // for FunctionCall, this is the function name
  std::vector<Query> function_call_args;

  bool computed;
  std::unordered_map<std::string, ReadResult> series_data; // used during execution

  Query() = delete;
  Query(Type type, int64_t int_data);
  Query(Type type, double float_data);
  Query(Type type, const std::string& string_data);

  std::string str() const;
};

std::vector<QueryToken> tokenize_query(const std::string& query);
Query parse_query(const std::vector<QueryToken>& tokens);
