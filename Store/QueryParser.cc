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
      return string_printf("Integer(%s)", this->int_data);
    case Type::Float:
      return string_printf("Float(%s)", this->float_data);
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

      size_t pos = offset;
      if (is_float) {
        ret.emplace_back(QueryToken::Type::Float, stod(query, &pos));
      } else {
        ret.emplace_back(QueryToken::Type::Integer, stoll(query, &pos, 0));
      }
      if (pos != offset + length) {
        throw runtime_error("incomplete numeric conversion");
      }

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
      continue;

    // anything else is a bad character
    } else {
      throw runtime_error(string_printf("invalid character %c in query",
          query[offset]));
    }
  }
  return ret;
}
