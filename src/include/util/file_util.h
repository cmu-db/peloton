//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// file_util.h
//
// Identification: src/include/util/file_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string.h>
#include <sys/stat.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace peloton {

/**
 * File Utility Functions
 * These are some handy functions in case we ever need to do some basic file
 * stuff.
 * Notice that these don't check for errors or handle exceptions, so you should
 * not be using these to handle important data that we absolutely cannot lose.
 */
class FileUtil {
 public:
  /**
   * Get the contents of the file at the given path and return it as a string
   * This is not the most efficient implementation.
   * http://stackoverflow.com/a/2912614
   */
  static std::string GetFile(const std::string path) {
    std::string content;
    std::ifstream ifs(path);
    content.assign((std::istreambuf_iterator<char>(ifs)),
                   (std::istreambuf_iterator<char>()));
    return (content);
  }

  /**
   * Gets the absolute path given a relative path
   * @param relative_path: the path **must** be relative to the root "peloton"
   * directory
   * @return absolute path as a string
   */
  static std::string GetRelativeToRootPath(const std::string &relative_path) {
    boost::filesystem::path root_path(
        boost::filesystem::current_path().branch_path().branch_path());
    root_path /= relative_path;
    return root_path.string();
  }

  /**
   * Write the contents of the provided string to a new temp file and return
   * the path to that file.
   * @param the string to write to the file
   * @param an optional prefix to use for the temp filename
   * @param an optional suffix/extension to use for the temp filename
   */
  static std::string WriteTempFile(const std::string contents,
                                   const std::string prefix = "",
                                   const std::string ext = "") {
    std::ostringstream path;
    path << prefix << "%%%%%%%%%";
    if (ext.empty() == false) path << "." << ext;

    boost::filesystem::path tempFile =
        boost::filesystem::temp_directory_path() /
        boost::filesystem::unique_path(path.str());

    std::ofstream out(tempFile.string());
    // out.open(tempFile);
    if (out) out << contents;

    return (tempFile.string());
  }

  /**
   * Returns true if the file at the provided path exists.
   * http://stackoverflow.com/a/12774387
   */
  static bool Exists(const std::string path) {
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
  }


};

}  // namespace peloton
