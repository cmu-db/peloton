/*
 * This file was downloaded from:
 * http://www-personal.umich.edu/~wagnerr/ConfigFile.html
 *
 * Copyright (c) 2004 Richard J. Wagner
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 * ------------------------------------------------
 *
 * It was subsequently modified:
 *
 * Copyright (c) 2012 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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

#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <typeinfo>

#ifndef LOGCABIN_CORE_CONFIG_H
#define LOGCABIN_CORE_CONFIG_H

namespace LogCabin {
namespace Core {

/**
 * Reads and writes configuration files.
 */
class Config {
    typedef std::string string;

  public:
    /**
     * Base class for Config exceptions.
     */
    struct Exception : public std::runtime_error {
        explicit Exception(const std::string& error);
    };


    struct FileNotFound : public Exception {
        explicit FileNotFound(const string& filename);
        virtual ~FileNotFound() throw() {}
        string filename;
    };

    // thrown only by T read(key) variant of read()
    struct KeyNotFound : public Exception {
        explicit KeyNotFound(const string& key);
        virtual ~KeyNotFound() throw() {}
        string key;
    };

    struct ConversionError : public Exception {
        ConversionError(const string& key,
                        const string& value,
                        const string& typeName);
        virtual ~ConversionError() throw() {}
        string key;
        string value;
        string typeName;
    };

    /**
     * Construct an empty Config.
     */
    explicit Config(const string& delimiter = "=",
                    const string& comment = "#");

    /**
     * Construct a Config from the given map of options.
     */
    explicit Config(const std::map<string, string>& options);

    /**
     * Load a Config from a file.
     * This is a convenience wrapper around operator>>.
     * \throw FileNotFound
     */
    void readFile(const string& filename);

    /**
     * Read configuration.
     */
    friend std::istream& operator>>(std::istream& is, Config& cf);

    /**
     * Write configuration.
     */
    friend std::ostream& operator<<(std::ostream& os, const Config& cf);

    /**
     * Read the value corresponding to a key.
     * \return
     *      The desired value, converted to a T.
     * \throw KeyNotFound
     *      If the key does not exist.
     * \throw ConversionError
     *      If the value could not be converted to a T.
     */
    template<class T = string>
    T read(const string& key) const;

    /**
     * Return the value corresponding to key or given default value if key is
     * not found.
     * \throw ConversionError
     *      If the value could not be converted to a T.
     */
    template<class T = string>
    T read(const string& key, const T& value) const;

    /**
     * Check whether key exists in configuration.
     */
    bool keyExists(const string& key) const;

    /**
     * Set a key to the given value.
     */
    template<class T> void set(const string& key, const T& value);

    /**
     * Set a key to the given string value.
     */
    void set(const string& key, const string& value);

    /**
     * Remove a key and its value.
     * If the key does not exist, this will do nothing.
     */
    void remove(const string& key);

  private:
    /**
     * Convert from a T to a string.
     * Type T must support << operator.
     */
    template<class T>
    static string toString(const T& t);

    /**
     * Convert from a string to a T.
     * Type T must support >> operator.
     *
     * For boolean conversions, "false", "f", "no", "n", "0" are false, and
     * "true", "t", "yes", "y", "1" are true.
     *
     * \throw ConversionError
     */
    template<class T>
    static T fromString(const string& key, const string& s);

    /**
     * Read a line, strip comments, and trim it.
     */
    std::string readLine(std::istream& is) const;

    /// Separator between key and value, usually "=".
    const string delimiter;

    /// Starts a comment, usually "#".
    const string comment;

    /// Extracted keys and values.
    std::map<string, string> contents;
};

template<class T>
std::string
Config::toString(const T& t)
{
    std::ostringstream ost;
    ost.setf(std::ostringstream::boolalpha);
    ost << t;
    return ost.str();
}

template<class T>
T
Config::fromString(const string& key, const string& s)
{
    T t;
    std::istringstream ist(s);
    ist >> t;
    if (!ist || !ist.eof())
        throw ConversionError(key, s, typeid(T).name());
    return t;
}

template<>
std::string
Config::fromString<std::string>(const string& key, const string& s);

template<>
bool
Config::fromString<bool>(const string& key, const string& s);


template<class T>
T
Config::read(const string& key) const
{
    auto p = contents.find(key);
    if (p == contents.end())
        throw KeyNotFound(key);
    return fromString<T>(key, p->second);
}

template<class T>
T
Config::read(const string& key, const T& value) const
{
    auto p = contents.find(key);
    if (p == contents.end())
        return value;
    return fromString<T>(key, p->second);
}

template<class T>
void
Config::set(const string& key, const T& value)
{
    set(key, toString(value));
}

} // namespace LogCabin::Core
} // namespace LogCabin

#endif /* LOGCABIN_CORE_CONFIG_H */
