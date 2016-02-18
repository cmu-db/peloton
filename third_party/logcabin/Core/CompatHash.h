/* Copyright (c) 2015 Diego Ongaro
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <memory>

#ifndef LOGCABIN_CORE_COMPATHASH_H
#define LOGCABIN_CORE_COMPATHASH_H

// missing std::hash<std::shared_ptr<T>> implementation on gcc 4.4 and 4.5
// Clang has it but but has defines like gcc 4.2.
#if __GNUC__ == 4 && __GNUC_MINOR__ < 6 && !__clang__

namespace std {

template<typename T>
struct hash<shared_ptr<T>>
{
  size_t operator()(const shared_ptr<T>& s) const {
      return hash<T*>()(s.get());
  }
};

} // namespace std

#endif // gcc < 4.6

#endif /* LOGCABIN_CORE_COMPATHASH_H */
