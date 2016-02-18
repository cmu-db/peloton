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

#include <cxxabi.h>
#include <fstream>

#include "Core/Config.h"
#include "Core/StringUtil.h"

namespace LogCabin {
namespace Core {

using Core::StringUtil::format;
using std::string;

namespace {

/// The set of whitespace characters.
static const char whitespace[] = " \n\t\v\r\f";

/// Remove leading whitespace.
void
ltrim(string& s)
{
    s.erase(0, s.find_first_not_of(whitespace));
}

/// Remove trailing whitespace.
void
rtrim(string& s)
{
    s.erase(s.find_last_not_of(whitespace) + 1);
}

/// Remove leading and trailing whitespace.
void
trim(string& s)
{
    ltrim(s);
    rtrim(s);
}

/// Demangle a C++ type name.
string
demangle(const string& name)
{
    char* result = abi::__cxa_demangle(name.c_str(),
                                       NULL, NULL, NULL);
    if (result == NULL)
        return name;
    string ret(result);
    free(result);
    return ret;
}

} // anonymous namespace


// exceptions

Config::Exception::Exception(const std::string& error)
    : std::runtime_error(error)
{
}


Config::FileNotFound::FileNotFound(const string& filename)
    : Exception(format(
        "The config file %s could not be opened",
        filename.c_str()))
    , filename(filename)
{
}

Config::KeyNotFound::KeyNotFound(const string& key)
    : Exception(format(
        "The configuration does not specify %s",
        key.c_str()))
    , key(key)
{
}

Config::ConversionError::ConversionError(const string& key,
                                         const string& value,
                                         const string& typeName)
    : Exception(format(
        "The value %s for key %s could not be converted to a %s",
        key.c_str(), value.c_str(), demangle(typeName).c_str()))
    , key(key)
    , value(value)
    , typeName(typeName)
{
}

// class Config

Config::Config(const string& delimiter,
               const string& comment)
    : delimiter(delimiter)
    , comment(comment)
    , contents()
{
}

Config::Config(const std::map<string, string>& options)
    : delimiter("=")
    , comment("#")
    , contents(options)
{
}


void
Config::readFile(const string& filename)
{
    std::ifstream in(filename.c_str());
    if (!in)
        throw FileNotFound(filename);
    in >> (*this);
}

std::istream&
operator>>(std::istream& is, Config& cf)
{
    // might need to read ahead to see where value ends
    string nextLine;

    while (is || !nextLine.empty()) {
        // Read an entire line at a time
        string line;
        if (nextLine.empty())
            line = cf.readLine(is);
        else
            line.swap(nextLine); // we read ahead; use it now

        size_t delimPos = line.find(cf.delimiter);
        if (delimPos != string::npos) {
            // Extract the key from line
            string key = line.substr(0, delimPos);
            rtrim(key);

            // Extract the value from line
            string value;
            line.swap(value);
            value.erase(0, delimPos + cf.delimiter.length());
            ltrim(value);

            // See if value continues on the next line
            // Stop at empty line, next line with a key, or end of stream
            while (is) {
                line = cf.readLine(is);

                // Empty lines end multi-line values
                if (line.empty())
                    break;

                // Lines with delimiters end multi-line values
                delimPos = line.find(cf.delimiter);
                if (delimPos != string::npos) {
                    nextLine.swap(line);
                    break;
                }

                // Append this line to the multi-line value.
                value += "\n";
                value += line;
            }

            // Store key and value
            cf.contents[key] = value;  // overwrites if key is repeated
        }
    }

    return is;
}

std::ostream&
operator<<(std::ostream& os, const Config& cf)
{
    for (auto p = cf.contents.begin(); p != cf.contents.end(); ++p) {
        os << p->first << " "
           << cf.delimiter << " "
           << p->second << std::endl;
    }
    return os;
}

bool
Config::keyExists(const string& key) const
{
    return (contents.find(key) != contents.end());
}

void
Config::set(const string& key, const string& value)
{
    string k = key;
    string v = value;
    trim(k);
    trim(v);
    contents[k] = v;
}

void
Config::remove(const string& key)
{
    auto it = contents.find(key);
    if (it != contents.end())
        contents.erase(it);
}

// private methods

template<>
std::string
Config::fromString<std::string>(const string& key, const string& s)
{
    return s;
}

template<>
bool
Config::fromString<bool>(const string& key, const string& s)
{
    static const std::map<string, bool> values {
        // These must be in sorted ASCII order for binary search to work. All
        // alpha entries should appear once in lowercase and once in uppercase.
        { "0",     false },
        { "1",     true  },
        { "F",     false },
        { "FALSE", false },
        { "N",     false },
        { "NO",    false },
        { "T",     true  },
        { "TRUE",  true  },
        { "Y",     true  },
        { "YES",   true  },
        { "f",     false },
        { "false", false },
        { "n",     false },
        { "no",    false },
        { "t",     true  },
        { "true",  true  },
        { "y",     true  },
        { "yes",   true  },
    };
    auto it = values.find(s);
    if (it != values.end())
        return it->second;
    throw ConversionError(key, s, "bool");
}


std::string
Config::readLine(std::istream& is) const
{
    string line;
    std::getline(is, line);
    size_t commentPos = line.find(comment);
    if (commentPos != string::npos)
        line.erase(commentPos);
    trim(line);
    return line;
}

} // namespace LogCabin::Core
} // namespace LogCabin
