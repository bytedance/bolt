/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"

namespace bytedance::bolt::functions::flinksql::test {
namespace {

class URLFunctionsTest : public FlinkFunctionBaseTest {
 protected:
  std::optional<std::string> parseUrl(
      const std::optional<std::string>& url,
      const std::optional<std::string>& partToExtract) {
    return evaluateOnce<std::string>(
        "flink_parse_url(c0, c1)", url, partToExtract);
  }

  std::optional<std::string> parseUrl(
      const std::optional<std::string>& url,
      const std::optional<std::string>& partToExtract,
      const std::optional<std::string>& key) {
    return evaluateOnce<std::string>(
        "flink_parse_url(c0, c1, c2)", url, partToExtract, key);
  }

  void validate(
      const std::string& url,
      const std::optional<std::string>& expectedHost,
      const std::optional<std::string>& expectedPath,
      const std::optional<std::string>& expectedQuery,
      const std::optional<std::string>& expectedRef,
      const std::optional<std::string>& expectedProtocol,
      const std::optional<std::string>& expectedFile,
      const std::optional<std::string>& expectedAuthority,
      const std::optional<std::string>& expectedUserInfo) {
    EXPECT_EQ(parseUrl(url, "HOST"), expectedHost);
    EXPECT_EQ(parseUrl(url, "PATH"), expectedPath);
    EXPECT_EQ(parseUrl(url, "QUERY"), expectedQuery);
    EXPECT_EQ(parseUrl(url, "REF"), expectedRef);
    EXPECT_EQ(parseUrl(url, "PROTOCOL"), expectedProtocol);
    EXPECT_EQ(parseUrl(url, "FILE"), expectedFile);
    EXPECT_EQ(parseUrl(url, "AUTHORITY"), expectedAuthority);
    EXPECT_EQ(parseUrl(url, "USERINFO"), expectedUserInfo);
  }

  void validateInvalidUrl(const std::string& url) {
    SCOPED_TRACE(url);
    for (const auto* part :
         {"HOST",
          "PATH",
          "QUERY",
          "REF",
          "PROTOCOL",
          "FILE",
          "AUTHORITY",
          "USERINFO"}) {
      EXPECT_EQ(parseUrl(url, part), std::nullopt);
    }
    EXPECT_EQ(parseUrl(url, "QUERY", "key"), std::nullopt);
  }
};

TEST_F(URLFunctionsTest, flinkCompatibility) {
  const auto validateWithQuery =
      [&](const std::string& url,
          const std::optional<std::string>& expectedHost,
          const std::optional<std::string>& expectedPath,
          const std::optional<std::string>& expectedQuery,
          const std::optional<std::string>& expectedRef,
          const std::optional<std::string>& expectedProtocol,
          const std::optional<std::string>& expectedFile,
          const std::optional<std::string>& expectedAuthority,
          const std::optional<std::string>& expectedUserInfo,
          const std::optional<std::string>& expectedQueryValue) {
        validate(
            url,
            expectedHost,
            expectedPath,
            expectedQuery,
            expectedRef,
            expectedProtocol,
            expectedFile,
            expectedAuthority,
            expectedUserInfo);
        EXPECT_EQ(parseUrl(url, "QUERY", "query"), expectedQueryValue);
      };

  validateWithQuery(
      "http://userinfo@flink.apache.org/path?query=1#Ref",
      "flink.apache.org",
      "/path",
      "query=1",
      "Ref",
      "http",
      "/path?query=1",
      "userinfo@flink.apache.org",
      "userinfo",
      "1");

  validateWithQuery(
      "https://use%20r:pas%20s@example.com/dir%20/pa%20th.HTML?query=x%20y&"
      "q2=2#Ref%20two",
      "example.com",
      "/dir%20/pa%20th.HTML",
      "query=x%20y&q2=2",
      "Ref%20two",
      "https",
      "/dir%20/pa%20th.HTML?query=x%20y&q2=2",
      "use%20r:pas%20s@example.com",
      "use%20r:pas%20s",
      "x%20y");

  validateWithQuery(
      "http://user:pass@host",
      "host",
      "",
      std::nullopt,
      std::nullopt,
      "http",
      "",
      "user:pass@host",
      "user:pass",
      std::nullopt);

  validateWithQuery(
      "http://user:pass@host/",
      "host",
      "/",
      std::nullopt,
      std::nullopt,
      "http",
      "/",
      "user:pass@host",
      "user:pass",
      std::nullopt);

  validateWithQuery(
      "http://user:pass@host/?#",
      "host",
      "/",
      "",
      "",
      "http",
      "/?",
      "user:pass@host",
      "user:pass",
      std::nullopt);

  validateWithQuery(
      "http://user:pass@host/file;param?query;p2",
      "host",
      "/file;param",
      "query;p2",
      std::nullopt,
      "http",
      "/file;param?query;p2",
      "user:pass@host",
      "user:pass",
      std::nullopt);

  validateWithQuery(
      "invalid://user:pass@host/file;param?query;p2",
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt);
}

TEST_F(URLFunctionsTest, requiresProtocol) {
  for (const auto* url :
       {"",
        "   ",
        "//image-cdn.tuchong.com/weili/video/ms/1814206798056260889.webp",
        "path/to/file",
        "/absolute/path"}) {
    validateInvalidUrl(url);
  }
}

TEST_F(URLFunctionsTest, invalidProtocol) {
  for (const auto* url :
       {"http//host/path",
        "://host/path",
        "1http://host/path",
        "ht*tp://host/path",
        "unknown://host/path"}) {
    validateInvalidUrl(url);
  }
}

TEST_F(URLFunctionsTest, validPort) {
  validate(
      "http://host:/path",
      "host",
      "/path",
      std::nullopt,
      std::nullopt,
      "http",
      "/path",
      "host:",
      std::nullopt);

  validate(
      "http://[::1]:/path",
      "[::1]",
      "/path",
      std::nullopt,
      std::nullopt,
      "http",
      "/path",
      "[::1]:",
      std::nullopt);

  validate(
      "http://host:65536/path",
      "host",
      "/path",
      std::nullopt,
      std::nullopt,
      "http",
      "/path",
      "host:65536",
      std::nullopt);
}

TEST_F(URLFunctionsTest, invalidPort) {
  for (const auto* url :
       {"http://host:abc/path",
        "http://host:-2/path",
        "http://host:+/path",
        "http://host:2147483648/path"}) {
    validateInvalidUrl(url);
  }
}

TEST_F(URLFunctionsTest, invalidAuthority) {
  for (const auto* url :
       {"http://[::1/path",
        "http://::1/path",
        "http://[::1]extra/path",
        "http://[]/path",
        "http://[garbage]/path",
        "http://[1.2.3.4]/path",
        "http://[::1%]/path"}) {
    validateInvalidUrl(url);
  }
}

TEST_F(URLFunctionsTest, invalidProtocolSpecificForm) {
  for (const auto* url :
       {"jar:file:/tmp/a.jar",
        "jar:file:/tmp/a.jar!entry",
        "jar:invalid:/tmp/a.jar!/entry",
        "jar:/tmp/a.jar!/entry",
        "mailto:"}) {
    validateInvalidUrl(url);
  }
}

TEST_F(URLFunctionsTest, queryParameter) {
  const std::string url =
      "http://host/path?key=first&key=second&bare&empty=&=unnamed&"
      "literal.a=value&a+b=plus";
  EXPECT_EQ(parseUrl(url, "QUERY", "key"), "first");
  EXPECT_EQ(parseUrl(url, "QUERY", "bare"), std::nullopt);
  EXPECT_EQ(parseUrl(url, "QUERY", "empty"), "");
  EXPECT_EQ(parseUrl(url, "QUERY", ""), "unnamed");
  EXPECT_EQ(parseUrl(url, "QUERY", "literal.a"), "value");
  EXPECT_EQ(parseUrl(url, "QUERY", "a+b"), "plus");
  EXPECT_EQ(parseUrl(url, "QUERY", "missing"), std::nullopt);
  EXPECT_EQ(parseUrl(url, "HOST", "key"), std::nullopt);
  EXPECT_EQ(parseUrl("http://host/path?", "QUERY", "key"), std::nullopt);
}

TEST_F(URLFunctionsTest, nullAndInvalidArguments) {
  const std::string url = "http://host/path?key=value";
  EXPECT_EQ(parseUrl(std::nullopt, "HOST"), std::nullopt);
  EXPECT_EQ(parseUrl(url, std::nullopt), std::nullopt);
  EXPECT_EQ(parseUrl(url, "QUERY", std::nullopt), std::nullopt);
  EXPECT_EQ(parseUrl(url, "host"), std::nullopt);
  EXPECT_EQ(parseUrl(url, "PORT"), std::nullopt);
  EXPECT_EQ(parseUrl("http://host:invalid/path", "HOST"), std::nullopt);
}

TEST_F(URLFunctionsTest, jdk8UrlSemantics) {
  validate(
      " \tHTTP://Example.COM/a b?x=%zz#Ref \n",
      "Example.COM",
      "/a b",
      "x=%zz",
      "Ref",
      "http",
      "/a b?x=%zz",
      "Example.COM",
      std::nullopt);

  validate(
      "file:///tmp/a?x=1#r",
      "",
      "/tmp/a",
      "x=1",
      "r",
      "file",
      "/tmp/a?x=1",
      "",
      std::nullopt);

  validate(
      "ftp://user:pass@host:21/path?x=1#r",
      "host",
      "/path",
      "x=1",
      "r",
      "ftp",
      "/path?x=1",
      "user:pass@host:21",
      "user:pass");

  validate(
      "jar:file:/tmp/a.jar!/entry?x=1#r",
      "",
      "file:/tmp/a.jar!/entry",
      "x=1",
      "r",
      "jar",
      "file:/tmp/a.jar!/entry?x=1",
      std::nullopt,
      std::nullopt);

  validate(
      "mailto:a@b.com?subject=x#discarded",
      "",
      "a@b.com",
      "subject=x",
      std::nullopt,
      "mailto",
      "a@b.com?subject=x",
      std::nullopt,
      std::nullopt);

  validate(
      "netdoc://host/path?x=1#r",
      "host",
      "/path",
      "x=1",
      "r",
      "netdoc",
      "/path?x=1",
      "host",
      std::nullopt);

  EXPECT_EQ(parseUrl("jar:file:/tmp/a.jar", "PATH"), std::nullopt);
  EXPECT_EQ(parseUrl("http://[::1]:8080/path", "HOST"), "[::1]");
  EXPECT_EQ(parseUrl("http://[::1]:8080/path", "AUTHORITY"), "[::1]:8080");
  EXPECT_EQ(parseUrl("http://[::1%eth0]/path", "HOST"), "[::1%eth0]");
}

TEST_F(URLFunctionsTest, fourSlashPath) {
  validate(
      "file:////server/share?x=1#r",
      "",
      "////server/share",
      "x=1",
      "r",
      "file",
      "////server/share?x=1",
      std::nullopt,
      std::nullopt);
}

TEST_F(URLFunctionsTest, multipleAtAuthority) {
  validate(
      "http://user@@host:80/path",
      "",
      "/path",
      std::nullopt,
      std::nullopt,
      "http",
      "/path",
      "user@@host:80",
      std::nullopt);
}

} // namespace
} // namespace bytedance::bolt::functions::flinksql::test
