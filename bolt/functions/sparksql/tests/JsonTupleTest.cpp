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

#include <fstream>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/dwio/common/tests/utils/DataFiles.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "bolt/vector/VectorPrinter.h"
using namespace bytedance::bolt::test;
namespace bytedance::bolt::functions::sparksql::test {
namespace {
class JsonTupleTest : public SparkFunctionBaseTest {
 protected:
  ArrayVectorPtr evaluateJsonTuple(
      const std::vector<std::optional<std::string>>& jsonStrs,
      const std::vector<std::string>& paths,
      bool legacy) {
    auto nullAt = [&jsonStrs](vector_size_t row) {
      return !jsonStrs[row].has_value();
    };
    auto valueAt = [&jsonStrs](vector_size_t row) {
      return jsonStrs[row] ? StringView(*jsonStrs[row]) : StringView();
    };

    auto result = [&] {
      auto inputString =
          makeFlatVector<StringView>(jsonStrs.size(), valueAt, nullAt);
      auto pathsArray = makeArrayVector<std::string>({paths});

      auto rowVector = makeRowVector({inputString, pathsArray});

      std::string expressionString =
          fmt::format("json_tuple_with_codegen(c0, c1, {})", legacy);

      return evaluate<ArrayVector>(expressionString, rowVector);
    }();

    return result;
  }
  void testJsonTuple(
      const std::vector<std::optional<std::string>>& jsonStrs,
      const std::vector<std::string>& paths,
      bool legacy,
      const std::string& output) {
    auto result = evaluateJsonTuple(jsonStrs, paths, legacy);
    auto expectedResult = makeArrayVectorFromJson<std::string>({output});

    // Checking the results
    assertEqualVectors(expectedResult, result);
  }
};

TEST_F(JsonTupleTest, BasicCases) {
  // Basic test cases
  // a json segment from
  // https://github.com/simdjson/simdjson/blob/master/jsonexamples/twitter.json
  std::string json = R"json(
    {
      "id": 903487807,
      "id_str": "903487807",
      "name": "RT&ファボ魔のむっつんさっm",
      "screen_name": "yuttari1998",
      "location": "関西    ↓詳しいプロ↓",
      "description": "無言フォローはあまり好みません ゲームと動画が好きですシモ野郎ですがよろしく…最近はMGSとブレイブルー、音ゲーをプレイしてます",
      "url": "http://t.co/Yg9e1Fl8wd",
      "entities": {
        "url": {
          "urls": [
            {
              "url": "http://t.co/Yg9e1Fl8wd",
              "expanded_url": "http://twpf.jp/yuttari1998",
              "display_url": "twpf.jp/yuttari1998",
              "indices": [
                0,
                22
              ]
            }
          ]
        },
        "description": {
          "urls": []
        }
      },
      "protected": false,
      "followers_count": 95,
      "friends_count": 158,
      "listed_count": 1,
      "created_at": "Thu Oct 25 08:27:13 +0000 2012",
      "favourites_count": 3652,
      "utc_offset": null,
      "time_zone": null,
      "geo_enabled": false,
      "verified": false,
      "statuses_count": 10276,
      "lang": "ja",
      "contributors_enabled": false,
      "is_translator": false,
      "is_translation_enabled": false,
      "profile_background_color": "C0DEED",
      "profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png",
      "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png",
      "profile_background_tile": false,
      "profile_image_url": "http://pbs.twimg.com/profile_images/500268849275494400/AoXHZ7Ij_normal.jpeg",
      "profile_image_url_https": "https://pbs.twimg.com/profile_images/500268849275494400/AoXHZ7Ij_normal.jpeg",
      "profile_banner_url": "https://pbs.twimg.com/profile_banners/903487807/1409062272",
      "profile_link_color": "0084B4",
      "profile_sidebar_border_color": "C0DEED",
      "profile_sidebar_fill_color": "DDEEF6",
      "profile_text_color": "333333",
      "profile_use_background_image": true,
      "default_profile": true,
      "default_profile_image": false,
      "following": false,
      "follow_request_sent": false,
      "notifications": false
    }
  )json";

  std::vector<std::string> paths{
      "id_str",
      "id",
      "location",
      "description",
      "entities.url.urls[0].indices[1]",
      "followers_count",
      "friends_count",
      "profile_background_image_url",
      "profile_use_background_image",
      "notifications",
      "entities"};
  std::string expectedOutput =
      "[\"903487807\",\"903487807\",\"関西    ↓詳しいプロ↓\",\"無言フォローはあまり好みません ゲームと動画が好きですシモ野郎ですがよろしく…最近はMGSとブレイブルー、音ゲーをプレイしてます\",null,\"95\",\"158\",\"http://abs.twimg.com/images/themes/theme1/bg.png\",\"true\",\"false\",\"{\\\"url\\\":{\\\"urls\\\":[{\\\"url\\\":\\\"http://t.co/Yg9e1Fl8wd\\\",\\\"expanded_url\\\":\\\"http://twpf.jp/yuttari1998\\\",\\\"display_url\\\":\\\"twpf.jp/yuttari1998\\\",\\\"indices\\\":[0,22]}]},\\\"description\\\":{\\\"urls\\\":[]}}\"]";
  testJsonTuple({json}, paths, true, expectedOutput);
  testJsonTuple({json}, paths, false, expectedOutput);
}

TEST_F(JsonTupleTest, numericFormatting) {
  const std::string json = R"({
    "small":0.0001,
    "large":1e10,
    "negative":-1e10,
    "lowerBoundary":0.001,
    "belowUpperBoundary":1e6,
    "upperBoundary":1e7,
    "integer":32,
    "decimal":1.5,
    "object":{"small":0.0001,"large":1e10},
    "array":[0.0001,1e10,32]
  })";
  const std::vector<std::string> paths{
      "small",
      "large",
      "negative",
      "lowerBoundary",
      "belowUpperBoundary",
      "upperBoundary",
      "integer",
      "decimal",
      "object",
      "array"};
  const std::string expectedOutput =
      R"(["1.0E-4","1.0E10","-1.0E10","0.001","1000000.0","1.0E7","32","1.5","{\"small\":1.0E-4,\"large\":1.0E10}","[1.0E-4,1.0E10,32]"])";

  for (const auto* useSonicJson : {"true", "false"}) {
    SCOPED_TRACE(std::string("useSonicJson: ") + useSonicJson);
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, useSonicJson}});
    for (const bool legacy : {true, false}) {
      SCOPED_TRACE(fmt::format("legacy: {}", legacy));
      testJsonTuple({json}, paths, legacy, expectedOutput);
    }
  }
}

TEST_F(JsonTupleTest, invalidValue) {
  std::string json = "{\"a\":1,\"b\":2c}";
  std::vector<std::string> paths{"a", "b"};
  std::string expectedOutput = "[\"1\",null]";
  testJsonTuple({json}, paths, true, expectedOutput);
  expectedOutput = "[null,null]";
  testJsonTuple({json}, paths, false, expectedOutput);
}
TEST_F(JsonTupleTest, malformed) {
  std::string json = "{\"a\":1,\"b\":2, c}";
  std::vector<std::string> paths{"a", "b", "c", "d"};

  std::string expectedOutput = "[\"1\",\"2\",null,null]";
  testJsonTuple({json}, paths, true, expectedOutput);
  expectedOutput = "[null,null,null,null]";
  testJsonTuple({json}, paths, false, expectedOutput);
}

TEST_F(JsonTupleTest, escapeQuote) {
  std::string json = R"({"id":0,"sku_id":"\""})";
  std::vector<std::string> paths{"sku_id"};

  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kUseSonicJson, "true"}});
  std::string expectedOutput = R"(["\""])";
  testJsonTuple({json}, paths, true, expectedOutput);
}
TEST_F(JsonTupleTest, sparkCornerCase) {
  std::string json = R"({"1.a": "b"})";
  std::vector<std::string> paths{"1.a"};
  std::string expectedOutput = R"(["b"])";
  testJsonTuple({json}, paths, true, expectedOutput);
}

TEST_F(JsonTupleTest, invalidArrayInJson) {
  std::string json =
      R"({"ad_tag_type":0,"series_list":[{"series_id":4711,"series_name":"瑞虎8 PLUS"},"sub_brand_id":216,"sub_brand_name":"奇瑞汽车"})";
  std::vector<std::string> paths{"ad_tag_type", "sub_brand_id", "series_list"};

  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kUseSonicJson, "true"}});
  std::string expectedOutput = R"([0, null, null])";
  testJsonTuple({json}, paths, true, expectedOutput);
  // Turn off sonic
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kUseSonicJson, "false"}});
  testJsonTuple({json}, paths, true, "[0, null, null]");
}
} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
