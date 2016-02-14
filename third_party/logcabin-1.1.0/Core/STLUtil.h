/* Copyright (c) 2011-2012 Stanford University
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

#include <algorithm>
#include <vector>

#ifndef LOGCABIN_CORE_STLUTIL_H
#define LOGCABIN_CORE_STLUTIL_H

namespace LogCabin {
namespace Core {
namespace STLUtil {

/**
 * Sort an R-value in place.
 * \param container
 *      An R-value to sort.
 * \return
 *      The sorted input.
 */
template<typename Container>
Container
sorted(Container container)
{
    std::sort(container.begin(), container.end());
    return container;
}

/**
 * Return a copy of the keys of a map.
 */
template<typename Map>
std::vector<typename Map::key_type>
getKeys(const Map& map)
{
    std::vector<typename Map::key_type> keys;
    for (auto it = map.begin(); it != map.end(); ++it)
        keys.push_back(it->first);
    return keys;
}

/**
 * Return a copy of the values of a map.
 */
template<typename Map>
std::vector<typename Map::mapped_type>
getValues(const Map& map)
{
    std::vector<typename Map::mapped_type> values;
    for (auto it = map.begin(); it != map.end(); ++it)
        values.push_back(it->second);
    return values;
}

/**
 * Return a copy of the key-value pairs of a map.
 */
template<typename Map>
std::vector<std::pair<typename Map::key_type,
                      typename Map::mapped_type>>
getItems(const Map& map)
{
    std::vector<std::pair<typename Map::key_type,
                          typename Map::mapped_type>> items;
    for (auto it = map.begin(); it != map.end(); ++it)
        items.push_back(*it);
    return items;
}

} // namespace LogCabin::Core::STLUtil
} // namespace LogCabin::Core
} // namespace LogCabin

#endif /* LOGCABIN_CORE_STLUTIL_H */
