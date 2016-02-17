/* Copyright (c) 2014 Stanford University
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

#include "Core/Config.h"
#include "Core/Debug.h"
#include "Storage/Layout.h"
#include "Storage/LogFactory.h"
#include "Storage/MemoryLog.h"
#include "Storage/SegmentedLog.h"
#include "Storage/SimpleFileLog.h"

namespace LogCabin {
namespace Storage {
namespace LogFactory {

std::unique_ptr<Log>
makeLog(const Core::Config& config,
        const Storage::Layout& storageLayout)
{
    const FilesystemUtil::File& parentDir = storageLayout.logDir;
    std::string module =
        config.read<std::string>("storageModule", "Segmented");
    std::unique_ptr<Log> log;
    if (module == "Memory") {
        log.reset(new MemoryLog());
    } else if (module == "SimpleFile") {
        log.reset(new SimpleFileLog(parentDir));
    } else if (module == "Segmented" ||
               module == "Segmented-Binary") {
        log.reset(new SegmentedLog(parentDir,
                                   SegmentedLog::Encoding::BINARY,
                                   config));
    } else if (module == "Segmented-Text") {
        log.reset(new SegmentedLog(parentDir,
                                   SegmentedLog::Encoding::TEXT,
                                   config));
    } else {
        EXIT("Unknown storage module from config file: %s", module.c_str());
    }
    return log;
}

} // namespace LogCabin::Storage::LogFactory
} // namespace LogCabin::Storage
} // namespace LogCabin
