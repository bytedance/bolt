/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/benchmarks/clickbench/ClickBenchQueryBuilder.h"

#include <algorithm>
#include <cctype>
#include <filesystem>

#include <fmt/format.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/ReaderFactory.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"

namespace bytedance::bolt::exec::test {
namespace {

std::string lowercase(std::string value) {
  std::transform(
      value.begin(), value.end(), value.begin(), [](unsigned char character) {
        return std::tolower(character);
      });
  return value;
}

std::vector<std::string> allButPrimaryKey() {
  return ClickBenchQueryBuilder::columnNames();
}

std::vector<std::string> q30Aggregates() {
  std::vector<std::string> aggregates;
  aggregates.reserve(90);
  for (int32_t i = 0; i < 90; ++i) {
    aggregates.emplace_back(fmt::format("sum(width_{}) as sum_{}", i, i));
  }
  return aggregates;
}

std::vector<std::string> q30Projections() {
  std::vector<std::string> projections;
  projections.reserve(90);
  projections.emplace_back("resolutionwidth as width_0");
  for (int32_t i = 1; i < 90; ++i) {
    projections.emplace_back(fmt::format(
        "resolutionwidth + cast({} as smallint) as width_{}", i, i));
  }
  return projections;
}

std::string eventDateBetween(int32_t start, int32_t end) {
  return fmt::format(
      "eventdate >= cast({} as integer) and eventdate <= cast({} as integer)",
      start,
      end);
}

std::string andEventDateBetween(
    std::string prefix,
    int32_t start,
    int32_t end,
    std::string suffix = "") {
  auto result = fmt::format("{} and {}", prefix, eventDateBetween(start, end));
  if (!suffix.empty()) {
    result = fmt::format("{} and {}", result, suffix);
  }
  return result;
}

} // namespace

ClickBenchQueryBuilder::ClickBenchQueryBuilder(dwio::common::FileFormat format)
    : format_(format),
      pool_(memory::memoryManager()->addLeafPool("ClickBenchQueryBuilder")) {
  BOLT_USER_CHECK(
      format_ == dwio::common::FileFormat::PARQUET,
      "ClickBench currently supports Parquet input only");
}

const std::vector<std::string>& ClickBenchQueryBuilder::queryNames() {
  static const std::vector<std::string> kNames = [] {
    std::vector<std::string> names;
    names.reserve(43);
    for (int32_t i = 1; i <= 43; ++i) {
      names.emplace_back(fmt::format("q{}", i));
    }
    return names;
  }();
  return kNames;
}

const std::vector<std::string>& ClickBenchQueryBuilder::columnNames() {
  // Canonical ClickBench hits schema, in physical Parquet column order.
  // Keep names lowercase because Bolt's DuckDB expression parser normalizes
  // unquoted identifiers to lowercase.
  static const std::vector<std::string> kNames = {
      "watchid",
      "javaenable",
      "title",
      "goodevent",
      "eventtime",
      "eventdate",
      "counterid",
      "clientip",
      "regionid",
      "userid",
      "counterclass",
      "os",
      "useragent",
      "url",
      "referer",
      "isrefresh",
      "referercategoryid",
      "refererregionid",
      "urlcategoryid",
      "urlregionid",
      "resolutionwidth",
      "resolutionheight",
      "resolutiondepth",
      "flashmajor",
      "flashminor",
      "flashminor2",
      "netmajor",
      "netminor",
      "useragentmajor",
      "useragentminor",
      "cookieenable",
      "javascriptenable",
      "ismobile",
      "mobilephone",
      "mobilephonemodel",
      "params",
      "ipnetworkid",
      "traficsourceid",
      "searchengineid",
      "searchphrase",
      "advengineid",
      "isartifical",
      "windowclientwidth",
      "windowclientheight",
      "clienttimezone",
      "clienteventtime",
      "silverlightversion1",
      "silverlightversion2",
      "silverlightversion3",
      "silverlightversion4",
      "pagecharset",
      "codeversion",
      "islink",
      "isdownload",
      "isnotbounce",
      "funiqid",
      "originalurl",
      "hid",
      "isoldcounter",
      "isevent",
      "isparameter",
      "dontcounthits",
      "withhash",
      "hitcolor",
      "localeventtime",
      "age",
      "sex",
      "income",
      "interests",
      "robotness",
      "remoteip",
      "windowname",
      "openername",
      "historylength",
      "browserlanguage",
      "browsercountry",
      "socialnetwork",
      "socialaction",
      "httperror",
      "sendtiming",
      "dnstiming",
      "connecttiming",
      "responsestarttiming",
      "responseendtiming",
      "fetchtiming",
      "socialsourcenetworkid",
      "socialsourcepage",
      "paramprice",
      "paramorderid",
      "paramcurrency",
      "paramcurrencyid",
      "openstatservicename",
      "openstatcampaignid",
      "openstatadid",
      "openstatsourceid",
      "utmsource",
      "utmmedium",
      "utmcampaign",
      "utmcontent",
      "utmterm",
      "fromtag",
      "hasgclid",
      "refererhash",
      "urlhash",
      "clid"};
  return kNames;
}

void ClickBenchQueryBuilder::initialize(const std::string& dataPath) {
  namespace fs = std::filesystem;
  const fs::path inputPath(dataPath);
  std::vector<std::string> files;
  if (fs::is_regular_file(inputPath)) {
    files.push_back(fs::absolute(inputPath).string());
  } else {
    const auto singleFile = inputPath / "hits.parquet";
    if (fs::is_regular_file(singleFile)) {
      files.push_back(fs::absolute(singleFile).string());
    }

    const auto tablePath =
        fs::is_directory(inputPath / "hits") ? inputPath / "hits" : inputPath;
    std::error_code error;
    if (files.empty()) {
      for (const auto& entry : fs::directory_iterator(
               tablePath,
               fs::directory_options::skip_permission_denied,
               error)) {
        if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
          files.push_back(fs::absolute(entry.path()).string());
        }
      }
    }
    BOLT_USER_CHECK(
        !error,
        "Failed to scan ClickBench data path {}: {}",
        tablePath.string(),
        error.message());
  }
  std::sort(files.begin(), files.end());
  BOLT_USER_CHECK(
      !files.empty(),
      "No Parquet files found in ClickBench data path: {}",
      dataPath);

  dwio::common::ReaderOptions readerOptions{pool_.get()};
  readerOptions.setFileFormat(format_);
  auto uniqueReadFile = filesystems::getFileSystem(files.front(), nullptr)
                            ->openFileForRead(files.front());
  std::shared_ptr<ReadFile> readFile(uniqueReadFile.release());
  auto input = std::make_unique<dwio::common::BufferedInput>(
      readFile, readerOptions.getMemoryPool());
  auto reader = dwio::common::getReaderFactory(format_)->createReader(
      std::move(input), readerOptions);
  initialize(reader->rowType(), std::move(files));
}

void ClickBenchQueryBuilder::initialize(
    RowTypePtr fileType,
    std::vector<std::string> dataFiles) {
  BOLT_USER_CHECK_NOT_NULL(fileType);
  BOLT_USER_CHECK(!dataFiles.empty(), "ClickBench data file list is empty");

  std::unordered_map<std::string, column_index_t> physicalColumns;
  for (column_index_t i = 0; i < fileType->size(); ++i) {
    physicalColumns.emplace(lowercase(fileType->nameOf(i)), i);
  }

  std::vector<TypePtr> canonicalTypes;
  canonicalTypes.reserve(columnNames().size());
  fileColumnNames_.clear();
  for (const auto& name : columnNames()) {
    const auto it = physicalColumns.find(name);
    BOLT_USER_CHECK(
        it != physicalColumns.end(),
        "ClickBench data is missing required column '{}'",
        name);
    if (name == "eventdate" && fileType->childAt(it->second)->isSmallint()) {
      canonicalTypes.push_back(INTEGER());
    } else {
      canonicalTypes.push_back(fileType->childAt(it->second));
    }
    fileColumnNames_.emplace(name, fileType->nameOf(it->second));
  }

  fileType_ = std::move(fileType);
  auto canonicalNames = columnNames();
  canonicalType_ = ROW(std::move(canonicalNames), std::move(canonicalTypes));
  dataFiles_ = std::move(dataFiles);
}

RowTypePtr ClickBenchQueryBuilder::selectedType(
    const std::vector<std::string>& columns) const {
  BOLT_USER_CHECK_NOT_NULL(
      canonicalType_, "ClickBenchQueryBuilder must be initialized first");
  std::vector<TypePtr> types;
  types.reserve(columns.size());
  for (const auto& column : columns) {
    const auto type = canonicalType_->findChild(column);
    BOLT_USER_CHECK_NOT_NULL(type, "Unknown ClickBench column: {}", column);
    types.push_back(type);
  }
  auto selectedNames = columns;
  return ROW(std::move(selectedNames), std::move(types));
}

TpchPlan ClickBenchQueryBuilder::makePlan(
    int32_t queryId,
    const std::vector<std::string>& scanColumns,
    const std::string& filter,
    const std::vector<std::string>& projections,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postProjections,
    const std::string& postFilter,
    const std::vector<std::string>& orderBy,
    int64_t offset,
    int64_t limit,
    bool hasDistinctAggregation,
    const std::vector<std::string>& outputProjections) const {
  core::PlanNodeId scanId;
  PlanBuilder builder(pool_.get());
  builder.tableScan("hits", selectedType(scanColumns), fileColumnNames_)
      .captureScanNodeId(scanId)
      .optionalFilter(filter)
      .optionalProject(projections);

  if (!groupingKeys.empty() || !aggregates.empty()) {
    if (hasDistinctAggregation) {
      builder.localPartition(groupingKeys);
      builder.singleAggregation(groupingKeys, aggregates);
    } else {
      builder.partialAggregation(groupingKeys, aggregates)
          .localPartition(groupingKeys)
          .finalAggregation();
    }
  }
  builder.optionalProject(postProjections).optionalFilter(postFilter);
  if (!orderBy.empty()) {
    builder.orderBy(orderBy, false);
  }
  if (limit > 0) {
    builder.limit(offset, limit, false);
  }
  builder.optionalProject(outputProjections);

  return {
      .plan = builder.planNode(),
      .dataFiles = {{scanId, dataFiles_}},
      .dataFileFormat = format_,
      .planName = fmt::format("q{}", queryId)};
}

TpchPlan ClickBenchQueryBuilder::getQueryPlan(int32_t queryId) const {
  const std::vector<std::string> none;
  switch (queryId) {
    case 1:
      return makePlan(
          1, {"watchid"}, "", none, {}, {"count(1) as count"}, none, "", none);
    case 2:
      return makePlan(
          2,
          {"advengineid"},
          "advengineid != 0",
          none,
          {},
          {"count(1) as count"},
          none,
          "",
          none);
    case 3:
      return makePlan(
          3,
          {"advengineid", "resolutionwidth"},
          "",
          none,
          {},
          {"sum(advengineid) as adv_sum",
           "count(1) as count",
           "avg(resolutionwidth) as width_avg"},
          none,
          "",
          none);
    case 4:
      return makePlan(
          4,
          {"userid"},
          "",
          none,
          {},
          {"avg(userid) as user_avg"},
          none,
          "",
          none);
    case 5:
      return makePlan(
          5,
          {"userid"},
          "",
          none,
          {},
          {"count(distinct userid) as count"},
          none,
          "",
          none,
          0,
          0,
          true);
    case 6:
      return makePlan(
          6,
          {"searchphrase"},
          "",
          none,
          {},
          {"count(distinct searchphrase) as count"},
          none,
          "",
          none,
          0,
          0,
          true);
    case 7:
      return makePlan(
          7,
          {"eventdate"},
          "",
          none,
          {},
          {"min(eventdate) as min_date", "max(eventdate) as max_date"},
          {"date_add('day', cast(min_date as bigint), cast('1970-01-01' as "
           "date)) as min_date",
           "date_add('day', cast(max_date as bigint), cast('1970-01-01' as "
           "date)) as max_date"},
          "",
          none);
    case 8:
      return makePlan(
          8,
          {"advengineid"},
          "advengineid != 0",
          none,
          {"advengineid"},
          {"count(1) as count"},
          none,
          "",
          {"count DESC"});
    case 9:
      return makePlan(
          9,
          {"regionid", "userid"},
          "",
          none,
          {"regionid"},
          {"count(distinct userid) as u"},
          none,
          "",
          {"u DESC"},
          0,
          10,
          true);
    case 10:
      return makePlan(
          10,
          {"regionid", "advengineid", "resolutionwidth", "userid"},
          "",
          none,
          {"regionid"},
          {"sum(advengineid) as adv_sum",
           "count(1) as c",
           "avg(resolutionwidth) as width_avg",
           "count(distinct userid) as users"},
          none,
          "",
          {"c DESC"},
          0,
          10,
          true);
    case 11:
      return makePlan(
          11,
          {"mobilephonemodel", "userid"},
          "mobilephonemodel != ''",
          none,
          {"mobilephonemodel"},
          {"count(distinct userid) as u"},
          none,
          "",
          {"u DESC"},
          0,
          10,
          true);
    case 12:
      return makePlan(
          12,
          {"mobilephone", "mobilephonemodel", "userid"},
          "mobilephonemodel != ''",
          none,
          {"mobilephone", "mobilephonemodel"},
          {"count(distinct userid) as u"},
          none,
          "",
          {"u DESC"},
          0,
          10,
          true);
    case 13:
      return makePlan(
          13,
          {"searchphrase"},
          "searchphrase != ''",
          none,
          {"searchphrase"},
          {"count(1) as c"},
          none,
          "",
          {"c DESC"},
          0,
          10);
    case 14:
      return makePlan(
          14,
          {"searchphrase", "userid"},
          "searchphrase != ''",
          none,
          {"searchphrase"},
          {"count(distinct userid) as u"},
          none,
          "",
          {"u DESC"},
          0,
          10,
          true);
    case 15:
      return makePlan(
          15,
          {"searchengineid", "searchphrase"},
          "searchphrase != ''",
          none,
          {"searchengineid", "searchphrase"},
          {"count(1) as c"},
          none,
          "",
          {"c DESC"},
          0,
          10);
    case 16:
      return makePlan(
          16,
          {"userid"},
          "",
          none,
          {"userid"},
          {"count(1) as c"},
          none,
          "",
          {"c DESC"},
          0,
          10);
    case 17:
      return makePlan(
          17,
          {"userid", "searchphrase"},
          "",
          none,
          {"userid", "searchphrase"},
          {"count(1) as c"},
          none,
          "",
          {"c DESC"},
          0,
          10);
    case 18:
      return makePlan(
          18,
          {"userid", "searchphrase"},
          "",
          none,
          {"userid", "searchphrase"},
          {"count(1) as c"},
          none,
          "",
          none,
          0,
          10);
    case 19:
      return makePlan(
          19,
          {"userid", "eventtime", "searchphrase"},
          "",
          {"userid",
           "minute(from_unixtime(cast(eventtime as double))) as event_minute",
           "searchphrase"},
          {"userid", "event_minute", "searchphrase"},
          {"count(1) as c"},
          none,
          "",
          {"c DESC"},
          0,
          10);
    case 20:
      return makePlan(
          20,
          {"userid"},
          "userid = 435090932899640449",
          none,
          {},
          {},
          none,
          "",
          none);
    case 21:
      return makePlan(
          21,
          {"url"},
          "url like '%google%'",
          none,
          {},
          {"count(1) as count"},
          none,
          "",
          none);
    case 22:
      return makePlan(
          22,
          {"searchphrase", "url"},
          "url like '%google%' and searchphrase != ''",
          none,
          {"searchphrase"},
          {"min(url) as min_url", "count(1) as c"},
          none,
          "",
          {"c DESC"},
          0,
          10);
    case 23:
      return makePlan(
          23,
          {"searchphrase", "url", "title", "userid"},
          "title like '%Google%' and not(url like '%.google.%') and searchphrase != ''",
          none,
          {"searchphrase"},
          {"min(url) as min_url",
           "min(title) as min_title",
           "count(1) as c",
           "count(distinct userid) as users"},
          none,
          "",
          {"c DESC"},
          0,
          10,
          true);
    case 24:
      return makePlan(
          24,
          allButPrimaryKey(),
          "url like '%google%'",
          none,
          {},
          {},
          none,
          "",
          {"eventtime"},
          0,
          10);
    case 25:
      return makePlan(
          25,
          {"searchphrase", "eventtime"},
          "searchphrase != ''",
          none,
          {},
          {},
          none,
          "",
          {"eventtime"},
          0,
          10,
          false,
          {"searchphrase"});
    case 26:
      return makePlan(
          26,
          {"searchphrase"},
          "searchphrase != ''",
          none,
          {},
          {},
          none,
          "",
          {"searchphrase"},
          0,
          10);
    case 27:
      return makePlan(
          27,
          {"searchphrase", "eventtime"},
          "searchphrase != ''",
          none,
          {},
          {},
          none,
          "",
          {"eventtime", "searchphrase"},
          0,
          10,
          false,
          {"searchphrase"});
    case 28:
      return makePlan(
          28,
          {"counterid", "url"},
          "url != ''",
          {"counterid", "length(url) as url_length"},
          {"counterid"},
          {"avg(url_length) as l", "count(1) as c"},
          none,
          "c > 100000",
          {"l DESC"},
          0,
          25);
    case 29:
      return makePlan(
          29,
          {"referer"},
          "referer != ''",
          {"regexp_replace(referer, '^https?://(?:www\\.)?([^/]+)/.*$', '$1') as k",
           "length(referer) as referer_length",
           "referer"},
          {"k"},
          {"avg(referer_length) as l",
           "count(1) as c",
           "min(referer) as min_referer"},
          none,
          "c > 100000",
          {"l DESC"},
          0,
          25);
    case 30:
      return makePlan(
          30,
          {"resolutionwidth"},
          "",
          q30Projections(),
          {},
          q30Aggregates(),
          none,
          "",
          none);
    case 31:
      return makePlan(
          31,
          {"searchengineid",
           "clientip",
           "searchphrase",
           "isrefresh",
           "resolutionwidth"},
          "searchphrase != ''",
          none,
          {"searchengineid", "clientip"},
          {"count(1) as c",
           "sum(isrefresh) as refreshes",
           "avg(resolutionwidth) as width_avg"},
          none,
          "",
          {"c DESC"},
          0,
          10);
    case 32:
      return makePlan(
          32,
          {"watchid",
           "clientip",
           "searchphrase",
           "isrefresh",
           "resolutionwidth"},
          "searchphrase != ''",
          none,
          {"watchid", "clientip"},
          {"count(1) as c",
           "sum(isrefresh) as refreshes",
           "avg(resolutionwidth) as width_avg"},
          none,
          "",
          {"c DESC"},
          0,
          10);
    case 33:
      return makePlan(
          33,
          {"watchid", "clientip", "isrefresh", "resolutionwidth"},
          "",
          none,
          {"watchid", "clientip"},
          {"count(1) as c",
           "sum(isrefresh) as refreshes",
           "avg(resolutionwidth) as width_avg"},
          none,
          "",
          {"c DESC"},
          0,
          10);
    case 34:
      return makePlan(
          34,
          {"url"},
          "",
          none,
          {"url"},
          {"count(1) as c"},
          none,
          "",
          {"c DESC"},
          0,
          10);
    case 35:
      return makePlan(
          35,
          {"url"},
          "",
          none,
          {"url"},
          {"count(1) as c"},
          {"1 as one", "url", "c"},
          "",
          {"c DESC"},
          0,
          10);
    case 36:
      return makePlan(
          36,
          {"clientip"},
          "",
          {"clientip",
           "clientip - 1 as clientip_1",
           "clientip - 2 as clientip_2",
           "clientip - 3 as clientip_3"},
          {"clientip", "clientip_1", "clientip_2", "clientip_3"},
          {"count(1) as c"},
          none,
          "",
          {"c DESC"},
          0,
          10);
    case 37:
      return makePlan(
          37,
          {"url", "counterid", "eventdate", "dontcounthits", "isrefresh"},
          andEventDateBetween(
              "counterid = 62",
              15887,
              15917,
              "dontcounthits = 0 and isrefresh = 0 and url != ''"),
          none,
          {"url"},
          {"count(1) as pageviews"},
          none,
          "",
          {"pageviews DESC"},
          0,
          10);
    case 38:
      return makePlan(
          38,
          {"title", "counterid", "eventdate", "dontcounthits", "isrefresh"},
          andEventDateBetween(
              "counterid = 62",
              15887,
              15917,
              "dontcounthits = 0 and isrefresh = 0 and title != ''"),
          none,
          {"title"},
          {"count(1) as pageviews"},
          none,
          "",
          {"pageviews DESC"},
          0,
          10);
    case 39:
      return makePlan(
          39,
          {"url",
           "counterid",
           "eventdate",
           "isrefresh",
           "islink",
           "isdownload"},
          andEventDateBetween(
              "counterid = 62",
              15887,
              15917,
              "isrefresh = 0 and islink != 0 and isdownload = 0"),
          none,
          {"url"},
          {"count(1) as pageviews"},
          none,
          "",
          {"pageviews DESC"},
          1000,
          10);
    case 40:
      return makePlan(
          40,
          {"traficsourceid",
           "searchengineid",
           "advengineid",
           "referer",
           "url",
           "counterid",
           "eventdate",
           "isrefresh"},
          andEventDateBetween("counterid = 62", 15887, 15917, "isrefresh = 0"),
          {"traficsourceid",
           "searchengineid",
           "advengineid",
           "if(searchengineid = 0 and advengineid = 0, referer, '') as src",
           "url as dst"},
          {"traficsourceid", "searchengineid", "advengineid", "src", "dst"},
          {"count(1) as pageviews"},
          none,
          "",
          {"pageviews DESC"},
          1000,
          10);
    case 41:
      return makePlan(
          41,
          {"urlhash",
           "eventdate",
           "counterid",
           "isrefresh",
           "traficsourceid",
           "refererhash"},
          andEventDateBetween(
              "counterid = 62",
              15887,
              15917,
              "isrefresh = 0 and traficsourceid in (-1, 6) and refererhash = "
              "3594120000172545465"),
          none,
          {"urlhash", "eventdate"},
          {"count(1) as pageviews"},
          none,
          "",
          {"pageviews DESC"},
          100,
          10);
    case 42:
      return makePlan(
          42,
          {"windowclientwidth",
           "windowclientheight",
           "counterid",
           "eventdate",
           "isrefresh",
           "dontcounthits",
           "urlhash"},
          andEventDateBetween(
              "counterid = 62",
              15887,
              15917,
              "isrefresh = 0 and dontcounthits = 0 and urlhash = "
              "2868770270353813622"),
          none,
          {"windowclientwidth", "windowclientheight"},
          {"count(1) as pageviews"},
          none,
          "",
          {"pageviews DESC"},
          10000,
          10);
    case 43:
      return makePlan(
          43,
          {"eventtime", "counterid", "eventdate", "isrefresh", "dontcounthits"},
          andEventDateBetween(
              "counterid = 62",
              15900,
              15901,
              "isrefresh = 0 and dontcounthits = 0"),
          {"date_trunc('minute', from_unixtime(cast(eventtime as double))) as event_minute"},
          {"event_minute"},
          {"count(1) as pageviews"},
          none,
          "",
          {"event_minute"},
          1000,
          10);
    default:
      BOLT_USER_FAIL("ClickBench query must be between 1 and 43: {}", queryId);
  }
}

} // namespace bytedance::bolt::exec::test
