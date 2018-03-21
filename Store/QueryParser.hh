#pragma once

#include <memory>
#include <unordered_map>

#include "Store.hh"


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

  QueryToken(Type type);
  QueryToken(Type type, int64_t int_data);
  QueryToken(Type type, double float_data);
  QueryToken(Type type, std::string&& string_data);

  std::string str() const;
};

std::vector<QueryToken> tokenize_query(const std::string& query);


struct Value {
  enum class Type {
    // patterns become function calls; there's a function that just reads from
    // the store
    FunctionCall = 0,
    Integer,
    Float,
    String,
  };

  Type type;
  int64_t int_data;
  double float_data;
  std::string string_data; // for FunctionCall, this is the function name
  std::unique_ptr<Value> function_call_args;
};

Value parse_query(const std::vector<QueryToken>& tokens);
