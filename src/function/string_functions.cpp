//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions.cpp
//
// Identification: src/function/string_functions.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "function/string_functions.h"

#include <string>
#include "common/macros.h"
#include "executor/executor_context.h"
namespace peloton {
namespace function {

uint32_t StringFunctions::Ascii(UNUSED_ATTRIBUTE executor::ExecutorContext &ctx,
                                const char *str, uint32_t length) {
  PL_ASSERT(str != nullptr);
  return length <= 1 ? 0 : static_cast<uint32_t>(str[0]);
}

#define NextByte(p, plen) ((p)++, (plen)--)

bool StringFunctions::Like(UNUSED_ATTRIBUTE executor::ExecutorContext &ctx,
                           const char *t, uint32_t tlen, const char *p,
                           uint32_t plen) {
  PL_ASSERT(t != nullptr);
  PL_ASSERT(p != nullptr);
  if (plen == 1 && *p == '%') return true;

  while (tlen > 0 && plen > 0) {
    if (*p == '\\') {
      NextByte(p, plen);
      if (plen <= 0) return false;
      if (tolower(*p) != tolower(*t)) return false;
    } else if (*p == '%') {
      char firstpat;
      NextByte(p, plen);

      while (plen > 0) {
        if (*p == '%')
          NextByte(p, plen);
        else if (*p == '_') {
          if (tlen <= 0) return false;
          NextByte(t, tlen);
          NextByte(p, plen);
        } else
          break;
      }

      if (plen <= 0) return true;

      if (*p == '\\') {
        if (plen < 2) return false;
        firstpat = tolower(p[1]);
      } else
        firstpat = tolower(*p);

      while (tlen > 0) {
        if (tolower(*t) == firstpat) {
          int matched = Like(ctx, t, tlen, p, plen);

          if (matched != false) return matched;
        }

        NextByte(t, tlen);
      }
      return false;
    } else if (*p == '_') {
      NextByte(t, tlen);
      NextByte(p, plen);
      continue;
    } else if (tolower(*p) != tolower(*t)) {
      return false;
    }
    NextByte(t, tlen);
    NextByte(p, plen);
  }

  if (tlen > 0) return false;

  while (plen > 0 && *p == '%') NextByte(p, plen);
  if (plen <= 0) return true;

  return false;
}

#undef NextByte

StringFunctions::StrWithLen StringFunctions::Substr(
    UNUSED_ATTRIBUTE executor::ExecutorContext &ctx, const char *str,
    uint32_t str_length, int32_t from, int32_t len) {
  int32_t signed_end = from + len - 1;
  if (signed_end < 0 || str_length == 0) {
    return StringFunctions::StrWithLen{nullptr, 0};
  }

  uint32_t begin = from > 0 ? unsigned(from) - 1 : 0;
  uint32_t end = unsigned(signed_end);

  if (end > str_length) {
    end = str_length;
  }

  if (begin > end) {
    return StringFunctions::StrWithLen{nullptr, 0};
  }

  return StringFunctions::StrWithLen{str + begin, end - begin + 1};
}

StringFunctions::StrWithLen StringFunctions::Repeat(
    executor::ExecutorContext &ctx, const char *str, uint32_t length,
    uint32_t num_repeat) {
  // Determine the number of bytes we need
  uint32_t total_len = ((length - 1) * num_repeat) + 1;

  // Allocate new memory
  auto *pool = ctx.GetPool();
  auto *new_str = reinterpret_cast<char *>(pool->Allocate(total_len));

  // Perform repeat
  char *ptr = new_str;
  for (uint32_t i = 0; i < num_repeat; i++) {
    PL_MEMCPY(ptr, str, length - 1);
    ptr += (length - 1);
  }

  // We done
  return StringFunctions::StrWithLen{new_str, total_len};
}

StringFunctions::StrWithLen StringFunctions::LTrim(
    UNUSED_ATTRIBUTE executor::ExecutorContext &ctx, const char *str,
    uint32_t str_len, const char *from, UNUSED_ATTRIBUTE uint32_t from_len) {
  PL_ASSERT(str != nullptr && from != nullptr);

  // llvm expects the len to include the terminating '\0'
  if (str_len == 1) {
    return StringFunctions::StrWithLen{nullptr, 1};
  }

  str_len -= 1;
  int tail = str_len - 1, head = 0;

  while (head < (int)str_len && strchr(from, str[head]) != nullptr) {
    head++;
  }

  // Determine length and return
  auto new_len = static_cast<uint32_t>(std::max(tail - head + 1, 0) + 1);
  return StringFunctions::StrWithLen{str + head, new_len};
}

StringFunctions::StrWithLen StringFunctions::RTrim(
    UNUSED_ATTRIBUTE executor::ExecutorContext &ctx, const char *str,
    uint32_t str_len, const char *from, UNUSED_ATTRIBUTE uint32_t from_len) {
  PL_ASSERT(str != nullptr && from != nullptr);

  // llvm expects the len to include the terminating '\0'
  if (str_len == 1) {
    return StringFunctions::StrWithLen{nullptr, 1};
  }

  str_len -= 1;
  int tail = str_len - 1, head = 0;
  while (tail >= 0 && strchr(from, str[tail]) != nullptr) {
    tail--;
  }

  auto new_len = static_cast<uint32_t>(std::max(tail - head + 1, 0) + 1);
  return StringFunctions::StrWithLen{str + head, new_len};
}

StringFunctions::StrWithLen StringFunctions::Trim(
    UNUSED_ATTRIBUTE executor::ExecutorContext &ctx, const char *str,
    uint32_t str_len) {
  return BTrim(ctx, str, str_len, " ", 2);
}

StringFunctions::StrWithLen StringFunctions::BTrim(
    UNUSED_ATTRIBUTE executor::ExecutorContext &ctx, const char *str,
    uint32_t str_len, const char *from, UNUSED_ATTRIBUTE uint32_t from_len) {
  PL_ASSERT(str != nullptr && from != nullptr);

  // Skip the tailing 0
  str_len--;

  if (str_len == 0) {
    return StringFunctions::StrWithLen{str, 1};
  }

  int head = 0;
  int tail = str_len - 1;

  // Trim tail
  while (tail >= 0 && strchr(from, str[tail]) != nullptr) {
    tail--;
  }

  // Trim head
  while (head < (int)str_len && strchr(from, str[head]) != nullptr) {
    head++;
  }

  // Done
  auto new_len = static_cast<uint32_t>(std::max(tail - head + 1, 0) + 1);
  return StringFunctions::StrWithLen{str + head, new_len};
}

uint32_t StringFunctions::Length(
    UNUSED_ATTRIBUTE executor::ExecutorContext &ctx,
    UNUSED_ATTRIBUTE const char *str, uint32_t length) {
  PL_ASSERT(str != nullptr);
  return length;
}

char *StringFunctions::Upper(UNUSED_ATTRIBUTE executor::ExecutorContext &ctx,
                             UNUSED_ATTRIBUTE const char *str,
                             UNUSED_ATTRIBUTE uint32_t str_len) {
  PL_ASSERT(str != nullptr);

  auto *pool = ctx.GetPool();
  auto *new_str = reinterpret_cast<char *>(pool->Allocate(str_len + 1));

  uint32_t index = 0;
  while (index < str_len) {
    if (str[index] >= 'a' && str[index] <= 'z')
      new_str[index] = str[index] - 'a' + 'A';
    else
      new_str[index] = str[index];

    index++;
  }
  new_str[index] = '\0';
  return new_str;
}

char *StringFunctions::Lower(UNUSED_ATTRIBUTE executor::ExecutorContext &ctx,
                             UNUSED_ATTRIBUTE const char *str,
                             UNUSED_ATTRIBUTE uint32_t str_len) {
  PL_ASSERT(str != nullptr);
  auto *pool = ctx.GetPool();
  auto *new_str = reinterpret_cast<char *>(pool->Allocate(str_len + 1));

  uint32_t index = 0;
  while (index < str_len) {
    if (str[index] >= 'A' && str[index] <= 'Z')
      new_str[index] = str[index] - 'A' + 'a';
    else
      new_str[index] = str[index];

    index++;
  }
  new_str[index] = '\0';
  return new_str;
}

type::Value StringFunctions::__Upper(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  executor::ExecutorContext ctx{nullptr};
  LOG_DEBUG("CALLED __UPPER");
  std::string ret_string(StringFunctions::Upper(
      ctx, args[0].GetAs<const char *>(), args[0].GetLength()));
  return type::ValueFactory::GetVarcharValue(ret_string);
}

type::Value StringFunctions::__Lower(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  executor::ExecutorContext ctx{nullptr};
  LOG_DEBUG("Called __LOWER");
  std::string ret_string(StringFunctions::Lower(
      ctx, args[0].GetAs<const char *>(), args[0].GetLength()));
  return type::ValueFactory::GetVarcharValue(ret_string);
}

type::Value StringFunctions::__Concat(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() > 0);
  executor::ExecutorContext ctx{nullptr};
  auto *pool = ctx.GetPool();

  char **concat_strs =
      reinterpret_cast<char **>(pool->Allocate(args.size() * sizeof(char **)));
  uint32_t *attr_lens = reinterpret_cast<uint32_t *>(
      pool->Allocate(args.size() * sizeof(uint32_t)));
  for (uint32_t i = 0; i < args.size(); i++) {
    if (args[i].IsNull()) {
      attr_lens[i] = 0;
      concat_strs[i] = nullptr;
      continue;
    }

    attr_lens[i] = args[i].GetLength();
    concat_strs[i] = args[i].GetAs<char *>();
  }
  LOG_DEBUG("called __CONCAT");
  StrWithLen tmp = StringFunctions::Concat(ctx, (const char **)concat_strs,
                                           attr_lens, args.size());
  
  
  if (tmp.length == 1)
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);

  std::string ret_string(tmp.str);
  return type::ValueFactory::GetVarcharValue(ret_string);
}

StringFunctions::StrWithLen StringFunctions::Concat(
    UNUSED_ATTRIBUTE executor::ExecutorContext &ctx,
    UNUSED_ATTRIBUTE const char **concat_strs,
    UNUSED_ATTRIBUTE uint32_t *attr_lens, UNUSED_ATTRIBUTE uint32_t attr_len) {
  PL_ASSERT(concat_strs != nullptr);
  PL_ASSERT(attr_lens != nullptr);

  uint32_t len_all = 0;
  uint32_t iter = 0;

  while (iter < attr_len) len_all += attr_lens[iter++];

  if (len_all == 0) {
    return StringFunctions::StrWithLen(nullptr, 1);
  }

  auto *pool = ctx.GetPool();
  char *ret_str = reinterpret_cast<char *>(pool->Allocate(len_all + 1));
  PL_MEMSET(ret_str,0,len_all+1);
  char *tail = ret_str;

  iter = 0;
  while (iter < attr_len) {
    if (concat_strs[iter] == nullptr || attr_lens[iter] == 1) {
      iter++;
      continue;
    }
    PL_MEMCPY(tail, concat_strs[iter], attr_lens[iter] - 1);
    tail += attr_lens[iter] - 1;
    iter++;
  }
  ret_str[tail - ret_str] = '\0';
  return StringFunctions::StrWithLen(ret_str, len_all + 1);
}

}  // namespace function
}  // namespace peloton
