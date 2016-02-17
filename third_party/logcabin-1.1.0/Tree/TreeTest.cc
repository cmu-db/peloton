/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2014 Diego Ongaro
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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <stdexcept>
#include <sys/stat.h>

#include "Core/StringUtil.h"
#include "Tree/Tree.h"
#include "Storage/FilesystemUtil.h"
#include "Storage/Layout.h"
#include "Storage/SnapshotFile.h"

namespace LogCabin {
namespace Tree {
namespace {

using namespace Internal; // NOLINT

#define EXPECT_OK(c) do { \
    Result result = (c); \
    EXPECT_EQ(Status::OK, result.status) << result.error; \
} while (0)

void
dumpTreeHelper(const Tree& tree,
               std::string path,
               std::vector<std::string>& nodes)
{
    nodes.push_back(path);

    std::vector<std::string> children;
    EXPECT_OK(tree.listDirectory(path, children));
    for (auto it = children.begin();
         it != children.end();
         ++it) {
        if (Core::StringUtil::endsWith(*it, "/")) {
            dumpTreeHelper(tree, path + *it, nodes);
        } else {
            nodes.push_back(path + *it);
        }
    }
}

std::string
dumpTree(const Tree& tree)
{
    std::vector<std::string> nodes;
    dumpTreeHelper(tree, "/", nodes);
    std::string ret;
    for (size_t i = 0; i < nodes.size(); ++i) {
        ret += nodes.at(i);
        if (i < nodes.size() - 1)
            ret += " ";
    }
    return ret;
}

TEST(TreeFileTest, dumpSnapshot)
{
    Storage::Layout layout;
    layout.initTemporary();
    {
        Storage::SnapshotFile::Writer writer(layout);
        File f;
        f.contents = "hello, world!";
        f.dumpSnapshot(writer);
        writer.save();
    }
    {
        Storage::SnapshotFile::Reader reader(layout);
        File f;
        f.loadSnapshot(reader);
        EXPECT_EQ("hello, world!", f.contents);
    }
}

TEST(TreeDirectoryTest, getChildren)
{
    Directory d;
    EXPECT_EQ((std::vector<std::string> {
               }), d.getChildren());
    d.makeFile("d");
    d.makeDirectory("c");
    d.makeFile("b");
    d.makeDirectory("a");
    EXPECT_EQ((std::vector<std::string> {
                "a/", "c/", "b", "d",
               }), d.getChildren());
}

TEST(TreeDirectoryTest, lookupDirectory)
{
    Directory d;
    EXPECT_TRUE(NULL == d.lookupDirectory("foo"));
    d.makeFile("foo");
    EXPECT_TRUE(NULL == d.lookupDirectory("foo"));
    d.makeDirectory("bar");
    Directory* d2 = d.lookupDirectory("bar");
    ASSERT_TRUE(d2 != NULL);
    EXPECT_EQ((std::vector<std::string> {
               }), d2->getChildren());
    EXPECT_EQ(d2, d.lookupDirectory("bar"));
}

TEST(TreeDirectoryTest, lookupDirectory_const)
{
    Directory d;
    const Directory& constd = d;
    EXPECT_TRUE(NULL == constd.lookupDirectory("foo"));
    d.makeFile("foo");
    EXPECT_TRUE(NULL == constd.lookupDirectory("foo"));
    d.makeDirectory("bar");
    const Directory* d2 = constd.lookupDirectory("bar");
    ASSERT_TRUE(d2 != NULL);
    EXPECT_EQ((std::vector<std::string> {
               }), d2->getChildren());
    EXPECT_EQ(d2, constd.lookupDirectory("bar"));
}

TEST(TreeDirectoryTest, makeDirectory)
{
    Directory d;
    d.makeFile("foo");
    EXPECT_TRUE(NULL == d.makeDirectory("foo"));
    Directory* d2 = d.makeDirectory("bar");
    ASSERT_TRUE(d2 != NULL);
    EXPECT_EQ((std::vector<std::string> {
               }), d2->getChildren());
    EXPECT_EQ(d2, d.makeDirectory("bar"));
}

TEST(TreeDirectoryTest, removeDirectory)
{
    Directory d;
    d.removeDirectory("foo");
    d.makeDirectory("bar")->makeDirectory("baz");
    d.removeDirectory("bar");
    EXPECT_EQ((std::vector<std::string> {
               }), d.getChildren());
}

TEST(TreeDirectoryTest, lookupFile)
{
    Directory d;
    EXPECT_TRUE(NULL == d.lookupFile("foo"));
    d.makeDirectory("foo");
    EXPECT_TRUE(NULL == d.lookupFile("foo"));
    d.makeFile("bar");
    File* f = d.lookupFile("bar");
    ASSERT_TRUE(f != NULL);
    EXPECT_EQ("", f->contents);
    EXPECT_EQ(f, d.lookupFile("bar"));
}

TEST(TreeDirectoryTest, lookupFile_const)
{
    Directory d;
    const Directory& constd = d;
    EXPECT_TRUE(NULL == constd.lookupFile("foo"));
    d.makeDirectory("foo");
    EXPECT_TRUE(NULL == constd.lookupFile("foo"));
    d.makeFile("bar");
    const File* f = constd.lookupFile("bar");
    ASSERT_TRUE(f != NULL);
    EXPECT_EQ("", f->contents);
    EXPECT_EQ(f, constd.lookupFile("bar"));
}

TEST(TreeDirectoryTest, makeFile)
{
    Directory d;
    d.makeDirectory("foo");
    EXPECT_TRUE(NULL == d.makeFile("foo"));
    File* f = d.makeFile("bar");
    ASSERT_TRUE(f != NULL);
    EXPECT_EQ("", f->contents);
    EXPECT_EQ(f, d.makeFile("bar"));
}

TEST(TreeDirectoryTest, removeFile)
{
    Directory d;
    d.removeFile("foo");
    d.makeFile("bar");
    d.removeFile("bar");
    EXPECT_EQ((std::vector<std::string> {
               }), d.getChildren());
}

TEST(TreeDirectoryTest, dumpSnapshot)
{
    Tree tree;
    tree.makeDirectory("/a");
    tree.makeDirectory("/a/b");
    tree.makeDirectory("/a/b/c");
    tree.makeDirectory("/a/d");
    tree.makeDirectory("/e");
    tree.makeDirectory("/f");
    tree.makeDirectory("/f/h");
    tree.write("/f/g", "rawr");

    Storage::Layout layout;
    layout.initTemporary();
    {
        Storage::SnapshotFile::Writer writer(layout);
        tree.superRoot.dumpSnapshot(writer);
        writer.save();
    }
    {
        Storage::SnapshotFile::Reader reader(layout);
        Tree t2;
        t2.superRoot.loadSnapshot(reader);
        EXPECT_EQ(dumpTree(tree), dumpTree(t2));
    }
}

TEST(TreePathTest, constructor)
{
    Path p1("");
    EXPECT_EQ(Status::INVALID_ARGUMENT, p1.result.status);

    Path p2("/");
    EXPECT_OK(p2.result);
    EXPECT_EQ("/", p2.symbolic);
    EXPECT_EQ((std::vector<std::string> {
               }), p2.parents);
    EXPECT_EQ("root", p2.target);

    Path p3("/foo");
    EXPECT_OK(p3.result);
    EXPECT_EQ("/foo", p3.symbolic);
    EXPECT_EQ((std::vector<std::string> {
                   "root",
               }), p3.parents);
    EXPECT_EQ("foo", p3.target);

    Path p4("/foo/bar/");
    EXPECT_OK(p4.result);
    EXPECT_EQ("/foo/bar/", p4.symbolic);
    EXPECT_EQ((std::vector<std::string> {
                   "root", "foo",
               }), p4.parents);
    EXPECT_EQ("bar", p4.target);
}

TEST(TreePathTest, parentsThrough)
{
    Path path("/a/b/c");
    auto it = path.parents.begin(); // root
    EXPECT_EQ("/", path.parentsThrough(it));
    ++it; // a
    EXPECT_EQ("/a", path.parentsThrough(it));
    ++it; // b
    EXPECT_EQ("/a/b", path.parentsThrough(it));
    ++it; // c
    EXPECT_EQ("/a/b/c", path.parentsThrough(it));
}

class TreeTreeTest : public ::testing::Test {
    TreeTreeTest()
        : tree()
    {
        EXPECT_EQ("/", dumpTree(tree));
    }

    Tree tree;
};

TEST_F(TreeTreeTest, dumpSnapshot)
{
    Storage::Layout layout;
    layout.initTemporary();
    {
        Storage::SnapshotFile::Writer writer(layout);
        tree.write("/c", "foo");
        tree.dumpSnapshot(writer);
        writer.save();
    }
    tree.removeFile("/c");
    tree.write("/d", "bar");
    {
        Storage::SnapshotFile::Reader reader(layout);
        tree.loadSnapshot(reader);
    }
    std::vector<std::string> children;
    EXPECT_OK(tree.listDirectory("/", children));
    EXPECT_EQ((std::vector<std::string>{ "c" }), children);
}


TEST_F(TreeTreeTest, normalLookup)
{
    std::string contents;
    Result result;
    result = tree.read("/a/b", contents);
    EXPECT_EQ(Status::LOOKUP_ERROR, result.status);
    EXPECT_EQ("Parent /a of /a/b does not exist", result.error);

    tree.write("/c", "foo");
    result = tree.read("/c/d", contents);
    EXPECT_EQ(Status::TYPE_ERROR, result.status);
    EXPECT_EQ("Parent /c of /c/d is a file", result.error);
}

TEST_F(TreeTreeTest, normalLookup_const)
{
    const Tree& constTree = tree;
    std::string contents;
    Result result;
    result = constTree.read("/a/b", contents);
    EXPECT_EQ(Status::LOOKUP_ERROR, result.status);
    EXPECT_EQ("Parent /a of /a/b does not exist", result.error);

    tree.write("/c", "foo");
    result = constTree.read("/c/d", contents);
    EXPECT_EQ(Status::TYPE_ERROR, result.status);
    EXPECT_EQ("Parent /c of /c/d is a file", result.error);
}


TEST_F(TreeTreeTest, mkdirLookup)
{
    std::string contents;
    Result result;
    tree.write("/c", "foo");
    result = tree.makeDirectory("/c/d");
    EXPECT_EQ(Status::TYPE_ERROR, result.status);
    EXPECT_EQ("Parent /c of /c/d is a file", result.error);
}

TEST_F(TreeTreeTest, checkCondition)
{
    tree.write("/a", "b");
    EXPECT_OK(tree.checkCondition("/a", "b"));
    Result result;
    result = tree.checkCondition("/c", "d");
    EXPECT_EQ(Status::CONDITION_NOT_MET, result.status);
    EXPECT_EQ("Could not read value at path '/c': /c does not exist",
              result.error);
    result = tree.checkCondition("/a", "d");
    EXPECT_EQ(Status::CONDITION_NOT_MET, result.status);
    EXPECT_EQ("Path '/a' has value 'b', not 'd' as required",
              result.error);

    EXPECT_OK(tree.checkCondition("/x", ""));
    EXPECT_OK(tree.makeDirectory("/c"));
    result = tree.checkCondition("/c", "");
    EXPECT_EQ(Status::CONDITION_NOT_MET, result.status);
    EXPECT_EQ("Could not read value at path '/c': /c is a directory",
              result.error);
}

TEST_F(TreeTreeTest, makeDirectory)
{
    EXPECT_OK(tree.makeDirectory("/"));
    EXPECT_EQ("/", dumpTree(tree));

    EXPECT_OK(tree.makeDirectory("/a/"));
    EXPECT_OK(tree.makeDirectory("/a/nodir/b"));
    EXPECT_EQ("/ /a/ /a/nodir/ /a/nodir/b/", dumpTree(tree));

    EXPECT_EQ(Status::INVALID_ARGUMENT, tree.makeDirectory("").status);

    EXPECT_OK(tree.write("/c", "foo"));
    EXPECT_EQ(Status::TYPE_ERROR, tree.makeDirectory("/c/b").status);

    Result result;
    result = tree.makeDirectory("/c");
    EXPECT_EQ(Status::TYPE_ERROR, result.status);
    EXPECT_EQ("/c already exists but is a file", result.error);
}

TEST_F(TreeTreeTest, listDirectory)
{
    std::vector<std::string> children;
    EXPECT_EQ(Status::INVALID_ARGUMENT,
              tree.listDirectory("", children).status);
    EXPECT_OK(tree.listDirectory("/", children));
    EXPECT_EQ((std::vector<std::string>{ }), children);

    EXPECT_OK(tree.makeDirectory("/a/"));
    EXPECT_OK(tree.write("/b", "foo"));
    EXPECT_OK(tree.makeDirectory("/c"));
    EXPECT_OK(tree.write("/d", "foo"));
    EXPECT_OK(tree.listDirectory("/", children));
    EXPECT_EQ((std::vector<std::string>{
                    "a/", "c/", "b", "d",
               }), children);

    Result result;
    result = tree.listDirectory("/e", children);
    EXPECT_EQ(Status::LOOKUP_ERROR, result.status);
    EXPECT_EQ("/e does not exist", result.error);
    result = tree.listDirectory("/d", children);
    EXPECT_EQ(Status::TYPE_ERROR, result.status);
    EXPECT_EQ("/d is a file", result.error);
}

TEST_F(TreeTreeTest, removeDirectory)
{
    EXPECT_EQ(Status::INVALID_ARGUMENT, tree.removeDirectory("").status);

    EXPECT_OK(tree.removeDirectory("/a/"));
    EXPECT_OK(tree.removeDirectory("/b"));
    EXPECT_EQ("/", dumpTree(tree));

    EXPECT_OK(tree.makeDirectory("/a/b"));
    EXPECT_OK(tree.write("/a/b/c", "foo"));
    EXPECT_OK(tree.write("/d", "foo"));
    EXPECT_OK(tree.removeDirectory("/a"));

    Result result;
    result = tree.removeDirectory("/d");
    EXPECT_EQ(Status::TYPE_ERROR, result.status);
    EXPECT_EQ("/d is a file", result.error);
    EXPECT_EQ("/ /d", dumpTree(tree));

    EXPECT_OK(tree.removeDirectory("/"));
    EXPECT_EQ("/", dumpTree(tree));
}

TEST_F(TreeTreeTest, write)
{
    EXPECT_EQ(Status::INVALID_ARGUMENT, tree.write("", "").status);
    EXPECT_EQ(Status::TYPE_ERROR, tree.write("/", "").status);
    EXPECT_OK(tree.write("/a", "foo"));
    EXPECT_EQ("/ /a", dumpTree(tree));
    std::string contents;
    EXPECT_OK(tree.read("/a", contents));
    EXPECT_EQ("foo", contents);
    EXPECT_OK(tree.write("/a", "bar"));
    EXPECT_OK(tree.read("/a", contents));
    EXPECT_EQ("bar", contents);

    EXPECT_OK(tree.makeDirectory("/b"));
    Result result;
    result = tree.write("/b", "baz");
    EXPECT_EQ(Status::TYPE_ERROR, result.status);
    EXPECT_EQ("/b is a directory", result.error);
}

TEST_F(TreeTreeTest, read)
{
    std::string contents;
    EXPECT_EQ(Status::INVALID_ARGUMENT, tree.read("", contents).status);
    EXPECT_EQ(Status::TYPE_ERROR, tree.read("/", contents).status);

    EXPECT_OK(tree.write("/a", "foo"));
    EXPECT_OK(tree.read("/a", contents));
    EXPECT_EQ("foo", contents);

    EXPECT_OK(tree.makeDirectory("/b"));

    Result result;
    result = tree.read("/b", contents);
    EXPECT_EQ(Status::TYPE_ERROR, result.status);
    EXPECT_EQ("/b is a directory", result.error);

    result = tree.read("/c", contents);
    EXPECT_EQ(Status::LOOKUP_ERROR, result.status);
    EXPECT_EQ("/c does not exist", result.error);
}

TEST_F(TreeTreeTest, removeFile)
{
    EXPECT_EQ(Status::INVALID_ARGUMENT, tree.removeFile("").status);
    EXPECT_EQ(Status::TYPE_ERROR, tree.removeFile("/").status);

    EXPECT_OK(tree.removeFile("/a"));

    EXPECT_OK(tree.write("/b", "foo"));
    EXPECT_OK(tree.removeFile("/b"));
    EXPECT_OK(tree.removeFile("/c/d"));

    EXPECT_OK(tree.makeDirectory("/e"));
    Result result;
    result = tree.removeFile("/e");
    EXPECT_EQ(Status::TYPE_ERROR, result.status);
    EXPECT_EQ("/e is a directory", result.error);
}

} // namespace LogCabin::Tree::<anonymous>
} // namespace LogCabin::Tree
} // namespace LogCabin
