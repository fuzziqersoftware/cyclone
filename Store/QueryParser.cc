#include "QueryParser.hh"

#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <phosg/Strings.hh>
#include <stdexcept>
#include <string>
#include <vector>

using namespace std;


QueryToken::QueryToken(Type type) : type(type) { }
QueryToken::QueryToken(Type type, int64_t int_data) : type(type), int_data(int_data) { }
QueryToken::QueryToken(Type type, double float_data) : type(type), float_data(float_data) { }
QueryToken::QueryToken(Type type, std::string&& string_data) : type(type), string_data(string_data) { }

string QueryToken::str() const {
  switch (this->type) {
    case Type::Dynamic:
      return string_printf("Dynamic(%s)", this->string_data.c_str());
    case Type::Integer:
      return string_printf("Integer(%" PRId64 ")", this->int_data);
    case Type::Float:
      return string_printf("Float(%g)", this->float_data);
    case Type::String:
      return string_printf("String(%s)", this->string_data.c_str());
    case Type::OpenParenthesis:
      return "OpenParenthesis";
    case Type::CloseParenthesis:
      return "CloseParenthesis";
    case Type::Comma:
      return "Comma";
  }
  return "Unknown";
}



Query::Query(Type type, int64_t int_data) : type(type), int_data(int_data), computed(false) { }
Query::Query(Type type, double float_data) : type(type), float_data(float_data), computed(false) { }
Query::Query(Type type, const string& string_data) : type(type), string_data(string_data), computed(false) { }

std::string Query::str() const {
  switch (this->type) {
    case Type::FunctionCall: {
      string s = this->string_data + "(";
      bool after_first_arg = false;
      for (const auto& arg : this->function_call_args) {
        if (after_first_arg) {
          s += ',';
        } else {
          after_first_arg = true;
        }
        s += arg.str();
      }
      return s + ")";
    }

    case Type::SeriesReference:
      return this->string_data;

    case Type::Integer:
      return string_printf("%" PRId64, this->int_data);

    case Type::Float:
      return string_printf("%g", this->float_data);

    case Type::String:
      // TODO: we should handle escape sequences here
      return string_printf("\"%s\"", this->string_data.c_str());

    default:
      return "<unknown query node>";
  }
}



vector<QueryToken> tokenize_query(const string& query) {
  vector<QueryToken> ret;

  for (size_t offset = 0; offset < query.size();) {

    // check for symbols
    if (query[offset] == '(') {
      ret.emplace_back(QueryToken::Type::OpenParenthesis);
      offset++;

    } else if (query[offset] == ')') {
      ret.emplace_back(QueryToken::Type::CloseParenthesis);
      offset++;

    } else if (query[offset] == ',') {
      ret.emplace_back(QueryToken::Type::Comma);
      offset++;

    // check for string constants
    } else if ((query[offset] == '\"') || (query[offset] == '\'')) {
      // TODO: we should handle escape sequences here
      char quote = query[offset];
      size_t length = 0;
      for (; (offset + length + 1 < query.size()) && (query[offset + length + 1] != quote); length++);
      if (offset + length + 1 >= query.size()) {
        throw runtime_error("unterminated string constant");
      }

      ret.emplace_back(QueryToken::Type::String, query.substr(offset + 1, length));
      offset += (length + 2);

    // check for numbers
    // hack: if it starts with +, -, ., or a number, then it's a number. allowed
    // chars in ints are only [0-9+-]; in floats we allow [0-9.eE+-]
    } else if (isdigit(query[offset]) || (query[offset] == '+') ||
        (query[offset] == '-') || (query[offset] == '.')) {
      size_t length = 0;
      bool is_float = false;
      for (; offset + length < query.size(); length++) {
        char ch = query[offset + length];
        if ((ch == '.') || (ch == 'e') || (ch == 'E')) {
          is_float = true;
        } else if (!isdigit(ch) && (ch != '+') && (ch != '-')) {
          break;
        }
      }

      char* conv_end = NULL;
      if (is_float) {
        ret.emplace_back(QueryToken::Type::Float, strtod(query.data() + offset, &conv_end));
      } else {
        ret.emplace_back(QueryToken::Type::Integer, static_cast<int64_t>(
            strtoll(query.data() + offset, &conv_end, 0)));
      }
      if (conv_end == query.data() + offset) {
        throw runtime_error("incomplete numeric conversion");
      }
      offset += length;

    // check for dynamics
    // dynamics must begin with a letter or underscore, but after that, may
    // contain any of [a-zA-Z0-9_\[\]{}*.-<>]. watch out: they may contain
    // commas, but only if between {}
    } else if (isalpha(query[offset])) {
      string data;
      size_t brace_level = 0;
      for (; offset < query.size(); offset++) {
        char ch = query[offset];
        if (ch == '{') {
          brace_level++;
        }
        if (ch == '}') {
          if (brace_level == 0) {
            throw runtime_error("unbalanced braces");
          }
          brace_level--;
        }

        if (isalnum(ch) || (ch == '_') || (ch == '[') || (ch == ']') ||
            (ch == '{') || (ch == '}') || (ch == '*') || (ch == '.') ||
            (ch == '-') || (ch == '<') || (ch == '>') ||
            (brace_level && (ch == ','))) {
          data += ch;
        } else {
          break;
        }
      }

      ret.emplace_back(QueryToken::Type::Dynamic, move(data));

    // skip whitespace
    } else if (isblank(query[offset])) {
      offset++;
      continue;

    // anything else is a bad character
    } else {
      throw runtime_error(string_printf("invalid character %c in query",
          query[offset]));
    }
  }
  return ret;
}

static Query parse_query(const std::vector<QueryToken>& tokens, size_t offset,
    size_t end_offset) {
  if (end_offset <= offset) {
    throw logic_error("query does not contain any tokens");
  }

  // queries must be of one of the following forms:
  // Dynamic/Integer/Float/String
  // Dynamic OpenParenthesis [arguments] CloseParenthesis
  const QueryToken& head = tokens[offset];

  if (end_offset - offset == 1) {
    if (head.type == QueryToken::Type::Dynamic) {
      return Query(Query::Type::SeriesReference, head.string_data);
    } else if (head.type == QueryToken::Type::Integer) {
      return Query(Query::Type::Integer, head.int_data);
    } else if (head.type == QueryToken::Type::Float) {
      return Query(Query::Type::Float, head.float_data);
    } else if (head.type == QueryToken::Type::String) {
      return Query(Query::Type::String, head.string_data);
    } else {
      throw runtime_error("unexpected token: " + head.str());
    }
  }

  // for a function call, there must be at least three tokens
  if (end_offset - offset < 3) {
    throw runtime_error("function call segment is too short");
  }

  if (head.type != QueryToken::Type::Dynamic) {
    throw runtime_error("expected function name: " + head.str());
  }
  if (tokens[offset + 1].type != QueryToken::Type::OpenParenthesis) {
    throw runtime_error("expected open parenthesis: " + tokens[offset + 1].str());
  }

  Query ret(Query::Type::FunctionCall, head.string_data);
  size_t argument_start_offset = offset + 2;
  size_t paren_stack_size = 0;
  for (offset = argument_start_offset; offset < end_offset; offset++) {
    if (tokens[offset].type == QueryToken::Type::OpenParenthesis) {
      paren_stack_size++;
      continue;
    }
    if ((tokens[offset].type == QueryToken::Type::CloseParenthesis) &&
        (paren_stack_size > 0)) {
      paren_stack_size--;
      continue;
    }

    if (((tokens[offset].type == QueryToken::Type::Comma) ||
         (tokens[offset].type == QueryToken::Type::CloseParenthesis)) &&
        (paren_stack_size == 0)) {
      ret.function_call_args.emplace_back(parse_query(tokens, argument_start_offset, offset));
      argument_start_offset = offset + 1;
      if (tokens[offset].type == QueryToken::Type::CloseParenthesis) {
        break;
      }
    }
  }
  if (offset != end_offset - 1) {
    throw runtime_error("argument parsing is incomplete");
  }
  if (tokens[offset].type != QueryToken::Type::CloseParenthesis) {
    throw runtime_error("final token was not a close parenthesis: " + tokens[offset].str());
  }
  return ret;
}

Query parse_query(const std::vector<QueryToken>& tokens) {
  return parse_query(tokens, 0, tokens.size());
}
