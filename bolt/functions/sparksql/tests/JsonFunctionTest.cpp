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

#include <gtest/gtest.h>

#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
namespace bytedance::bolt::functions::sparksql::test {
using namespace bytedance::bolt::test;

namespace {

class JsonFucntionTest : public SparkFunctionBaseTest {
 protected:
  void testJsonExtract(
      const std::vector<std::optional<std::string>>& input,
      const std::string& json_path,
      const std::vector<std::optional<std::string>>& output);

  std::optional<std::string> testGetJsonObjectOnce(
      const std::optional<std::string>& input,
      const std::optional<std::string>& json_path);
};

void JsonFucntionTest::testJsonExtract(
    const std::vector<std::optional<std::string>>& input,
    const std::string& json_path,
    const std::vector<std::optional<std::string>>& output) {}

std::optional<std::string> JsonFucntionTest::testGetJsonObjectOnce(
    const std::optional<std::string>& input,
    const std::optional<std::string>& json_path) {
  return evaluateOnce<std::string>("get_json_object(c0, c1)", input, json_path);
}

TEST_F(JsonFucntionTest, array_test) {
  // test dom
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "true"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json = R"(
        {"charset": "utf-8","order_info": {"amount": "16900","checkout_id": "1","sub_order_info": [{"sale_channel": "default.poi_anchor.default","merchant_area_id": "","actual_amount": "16900","merchant_nickname": "北京袋鼠吉厨餐饮管理有限公司","poi_num": "275","enter_source": "poi","fee_type": "0","product_sub_type": "0","sale_start_time": "1711012860","first_category_name": "美食","over_sea_flag": "0","price": "16900","product_id": "1764125373947920","third_category_name": "烤肉","merchant_city_id": "110100","code_valid_start_time": "0","merchant_prov_id": "110000","code_valid_end_time": "0","create_time": "","merchant_name": "北京袋鼠吉厨餐饮管理有限公司","sku_id": "1764125373947920","user_limit": "6","product_name": "【聚会必点】16荤16素韩式肥牛烤肉3-5人套餐（含电烤炉+可乐）","order_item_id": [],"shop_id": "7211441316230596667","commodity_ec_discount_amount": "0","user_id": "2101903216082967","second_cid": "1005000","author_id": "59837929947","room_anchor_id": "<nil>","order_limit": "6","order_id": "","merchant_userid": "200_2184047828143099","room_id": "<nil>","third_cid": "1005001","order_scene": "","merchant_address": "北京市大兴区庆祥南路29号院5号楼3层309","first_cid": "1000000","poi_enter_page": "others_homepage","quantity": "1","detail_enter_page": "poi_page","item_id": "7338666391777627401","service_provider_name": "","miniapp_name": "","trade_limit": "","commodity_amount": "16900","detail_enter_method": "card","product_type": "14","service_provider_id": "","sale_end_time": "1742016119","order_source": "others_homepage","miniapp_id": "","second_category_name": "烤肉"},{"commodity_amount": "0","product_type": "14","commodity_ec_discount_amount": "0","actual_amount": "0","sku_id": "0","fee_type": "1"}]}}
      )";

      EXPECT_EQ(
          testGetJsonObjectOnce(json, "$.order_info.sub_order_info[*].shop_id"),
          "\"7211441316230596667\"");

      json = R"(
      {"charset": "utf-8","order_info": {"amount": "16900","checkout_id": "1","sub_order_info": [{"sale_channel": "default.poi_anchor.default","merchant_area_id": "","actual_amount": "16900","merchant_nickname": "北京袋鼠吉厨餐饮管理有限公司","poi_num": "275","enter_source": "poi","fee_type": "0","product_sub_type": "0","sale_start_time": "1711012860","first_category_name": "美食","over_sea_flag": "0","price": "16900","product_id": "1764125373947920","third_category_name": "烤肉","merchant_city_id": "110100","code_valid_start_time": "0","merchant_prov_id": "110000","code_valid_end_time": "0","create_time": "","merchant_name": "北京袋鼠吉厨餐饮管理有限公司","sku_id": "1764125373947920","user_limit": "6","product_name": "【聚会必点】16荤16素韩式肥牛烤肉3-5人套餐（含电烤炉+可乐）","order_item_id": [],"shop_id": "7211441316230596667","commodity_ec_discount_amount": "0","user_id": "2101903216082967","second_cid": "1005000","author_id": "59837929947","room_anchor_id": "<nil>","order_limit": "6","order_id": "","merchant_userid": "200_2184047828143099","room_id": "<nil>","third_cid": "1005001","order_scene": "","merchant_address": "北京市大兴区庆祥南路29号院5号楼3层309","first_cid": "1000000","poi_enter_page": "others_homepage","quantity": "1","detail_enter_page": "poi_page","item_id": "7338666391777627401","service_provider_name": "","miniapp_name": "","trade_limit": "","commodity_amount": "16900","detail_enter_method": "card","product_type": "14","service_provider_id": "","sale_end_time": "1742016119","order_source": "others_homepage","miniapp_id": "","second_category_name": "烤肉"},{"commodity_amount": "0","product_type": "14","commodity_ec_discount_amount": "0","actual_amount": "0","sku_id": "0","fee_type": "1"},{"shop_id": "1111111","commodity_ec_discount_amount": "0"}]}}
      )";
      EXPECT_EQ(
          testGetJsonObjectOnce(
              R"(
      {"charset": "utf-8","order_info": {"amount": "16900","checkout_id": "1","sub_order_info": [{"sale_channel": "default.poi_anchor.default","merchant_area_id": "","actual_amount": "16900","merchant_nickname": "北京袋鼠吉厨餐饮管理有限公司","poi_num": "275","enter_source": "poi","fee_type": "0","product_sub_type": "0","sale_start_time": "1711012860","first_category_name": "美食","over_sea_flag": "0","price": "16900","product_id": "1764125373947920","third_category_name": "烤肉","merchant_city_id": "110100","code_valid_start_time": "0","merchant_prov_id": "110000","code_valid_end_time": "0","create_time": "","merchant_name": "北京袋鼠吉厨餐饮管理有限公司","sku_id": "1764125373947920","user_limit": "6","product_name": "【聚会必点】16荤16素韩式肥牛烤肉3-5人套餐（含电烤炉+可乐）","order_item_id": [],"shop_id": "7211441316230596667","commodity_ec_discount_amount": "0","user_id": "2101903216082967","second_cid": "1005000","author_id": "59837929947","room_anchor_id": "<nil>","order_limit": "6","order_id": "","merchant_userid": "200_2184047828143099","room_id": "<nil>","third_cid": "1005001","order_scene": "","merchant_address": "北京市大兴区庆祥南路29号院5号楼3层309","first_cid": "1000000","poi_enter_page": "others_homepage","quantity": "1","detail_enter_page": "poi_page","item_id": "7338666391777627401","service_provider_name": "","miniapp_name": "","trade_limit": "","commodity_amount": "16900","detail_enter_method": "card","product_type": "14","service_provider_id": "","sale_end_time": "1742016119","order_source": "others_homepage","miniapp_id": "","second_category_name": "烤肉"},{"commodity_amount": "0","product_type": "14","commodity_ec_discount_amount": "0","actual_amount": "0","sku_id": "0","fee_type": "1"},{"shop_id": "1111111","commodity_ec_discount_amount": "0"}]}}
      )",
              "$.order_info.sub_order_info[*].shop_id"),
          "[\"7211441316230596667\",\"1111111\"]");
      EXPECT_EQ(
          testGetJsonObjectOnce(
              R"(
      {"charset": "utf-8","order_info": {"amount": "16900","checkout_id": "1","sub_order_info": [{"sale_channel": "default.poi_anchor.default","merchant_area_id": "","actual_amount": "16900","merchant_nickname": "北京袋鼠吉厨餐饮管理有限公司","poi_num": "275","enter_source": "poi","fee_type": "0","product_sub_type": "0","sale_start_time": "1711012860","first_category_name": "美食","over_sea_flag": "0","price": "16900","product_id": "1764125373947920","third_category_name": "烤肉","merchant_city_id": "110100","code_valid_start_time": "0","merchant_prov_id": "110000","code_valid_end_time": "0","create_time": "","merchant_name": "北京袋鼠吉厨餐饮管理有限公司","sku_id": "1764125373947920","user_limit": "6","product_name": "【聚会必点】16荤16素韩式肥牛烤肉3-5人套餐（含电烤炉+可乐）","order_item_id": [],"commodity_ec_discount_amount": "0","user_id": "2101903216082967","second_cid": "1005000","author_id": "59837929947","room_anchor_id": "<nil>","order_limit": "6","order_id": "","merchant_userid": "200_2184047828143099","room_id": "<nil>","third_cid": "1005001","order_scene": "","merchant_address": "北京市大兴区庆祥南路29号院5号楼3层309","first_cid": "1000000","poi_enter_page": "others_homepage","quantity": "1","detail_enter_page": "poi_page","item_id": "7338666391777627401","service_provider_name": "","miniapp_name": "","trade_limit": "","commodity_amount": "16900","detail_enter_method": "card","product_type": "14","service_provider_id": "","sale_end_time": "1742016119","order_source": "others_homepage","miniapp_id": "","second_category_name": "烤肉"},{"commodity_amount": "0","product_type": "14","commodity_ec_discount_amount": "0","actual_amount": "0","shop_id": "7211441316230596667","sku_id": "0","fee_type": "1"},{"shop_id": "1111111","commodity_ec_discount_amount": "0"}]}}
      )",
              "$.order_info.sub_order_info[*].shop_id"),
          "[\"7211441316230596667\",\"1111111\"]");
      EXPECT_EQ(
          testGetJsonObjectOnce(
              R"(
      {"charset": "utf-8","order_info": {"amount": "16900","checkout_id": "1","sub_order_info": [{"sale_channel": "default.poi_anchor.default","merchant_area_id": "","actual_amount": "16900","merchant_nickname": "北京袋鼠吉厨餐饮管理有限公司","poi_num": "275","enter_source": "poi","fee_type": "0","product_sub_type": "0","sale_start_time": "1711012860","first_category_name": "美食","over_sea_flag": "0","price": "16900","product_id": "1764125373947920","third_category_name": "烤肉","merchant_city_id": "110100","code_valid_start_time": "0","merchant_prov_id": "110000","code_valid_end_time": "0","create_time": "","merchant_name": "北京袋鼠吉厨餐饮管理有限公司","sku_id": "1764125373947920","user_limit": "6","product_name": "【聚会必点】16荤16素韩式肥牛烤肉3-5人套餐（含电烤炉+可乐）","order_item_id": [],"shop_id": "7211441316230596667","commodity_ec_discount_amount": "0","user_id": "2101903216082967","second_cid": "1005000","author_id": "59837929947","room_anchor_id": "<nil>","order_limit": "6","order_id": "","merchant_userid": "200_2184047828143099","room_id": "<nil>","third_cid": "1005001","order_scene": "","merchant_address": "北京市大兴区庆祥南路29号院5号楼3层309","first_cid": "1000000","poi_enter_page": "others_homepage","quantity": "1","detail_enter_page": "poi_page","item_id": "7338666391777627401","service_provider_name": "","miniapp_name": "","trade_limit": "","commodity_amount": "16900","detail_enter_method": "card","product_type": "14","service_provider_id": "","sale_end_time": "1742016119","order_source": "others_homepage","miniapp_id": "","second_category_name": "烤肉"},{"commodity_amount": "0","product_type": "14","commodity_ec_discount_amount": "0","actual_amount": "0","shop_id": "1111111","sku_id": "0","fee_type": "1"},{"commodity_ec_discount_amount": "0"}]}}
      )",
              "$.order_info.sub_order_info[*].shop_id"),
          "[\"7211441316230596667\",\"1111111\"]");
    }
  }

  // test ondemand
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "false"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json = R"(
        {"charset": "utf-8","order_info": {"amount": "16900","checkout_id": "1","sub_order_info": [{"sale_channel": "default.poi_anchor.default","merchant_area_id": "","actual_amount": "16900","merchant_nickname": "北京袋鼠吉厨餐饮管理有限公司","poi_num": "275","enter_source": "poi","fee_type": "0","product_sub_type": "0","sale_start_time": "1711012860","first_category_name": "美食","over_sea_flag": "0","price": "16900","product_id": "1764125373947920","third_category_name": "烤肉","merchant_city_id": "110100","code_valid_start_time": "0","merchant_prov_id": "110000","code_valid_end_time": "0","create_time": "","merchant_name": "北京袋鼠吉厨餐饮管理有限公司","sku_id": "1764125373947920","user_limit": "6","product_name": "【聚会必点】16荤16素韩式肥牛烤肉3-5人套餐（含电烤炉+可乐）","order_item_id": [],"shop_id": "7211441316230596667","commodity_ec_discount_amount": "0","user_id": "2101903216082967","second_cid": "1005000","author_id": "59837929947","room_anchor_id": "<nil>","order_limit": "6","order_id": "","merchant_userid": "200_2184047828143099","room_id": "<nil>","third_cid": "1005001","order_scene": "","merchant_address": "北京市大兴区庆祥南路29号院5号楼3层309","first_cid": "1000000","poi_enter_page": "others_homepage","quantity": "1","detail_enter_page": "poi_page","item_id": "7338666391777627401","service_provider_name": "","miniapp_name": "","trade_limit": "","commodity_amount": "16900","detail_enter_method": "card","product_type": "14","service_provider_id": "","sale_end_time": "1742016119","order_source": "others_homepage","miniapp_id": "","second_category_name": "烤肉"},{"commodity_amount": "0","product_type": "14","commodity_ec_discount_amount": "0","actual_amount": "0","sku_id": "0","fee_type": "1"}]}}
      )";

      EXPECT_EQ(
          testGetJsonObjectOnce(json, "$.order_info.sub_order_info[*].shop_id"),
          "\"7211441316230596667\"");

      json = R"(
      {"charset": "utf-8","order_info": {"amount": "16900","checkout_id": "1","sub_order_info": [{"sale_channel": "default.poi_anchor.default","merchant_area_id": "","actual_amount": "16900","merchant_nickname": "北京袋鼠吉厨餐饮管理有限公司","poi_num": "275","enter_source": "poi","fee_type": "0","product_sub_type": "0","sale_start_time": "1711012860","first_category_name": "美食","over_sea_flag": "0","price": "16900","product_id": "1764125373947920","third_category_name": "烤肉","merchant_city_id": "110100","code_valid_start_time": "0","merchant_prov_id": "110000","code_valid_end_time": "0","create_time": "","merchant_name": "北京袋鼠吉厨餐饮管理有限公司","sku_id": "1764125373947920","user_limit": "6","product_name": "【聚会必点】16荤16素韩式肥牛烤肉3-5人套餐（含电烤炉+可乐）","order_item_id": [],"shop_id": "7211441316230596667","commodity_ec_discount_amount": "0","user_id": "2101903216082967","second_cid": "1005000","author_id": "59837929947","room_anchor_id": "<nil>","order_limit": "6","order_id": "","merchant_userid": "200_2184047828143099","room_id": "<nil>","third_cid": "1005001","order_scene": "","merchant_address": "北京市大兴区庆祥南路29号院5号楼3层309","first_cid": "1000000","poi_enter_page": "others_homepage","quantity": "1","detail_enter_page": "poi_page","item_id": "7338666391777627401","service_provider_name": "","miniapp_name": "","trade_limit": "","commodity_amount": "16900","detail_enter_method": "card","product_type": "14","service_provider_id": "","sale_end_time": "1742016119","order_source": "others_homepage","miniapp_id": "","second_category_name": "烤肉"},{"commodity_amount": "0","product_type": "14","commodity_ec_discount_amount": "0","actual_amount": "0","sku_id": "0","fee_type": "1"},{"shop_id": "1111111","commodity_ec_discount_amount": "0"}]}}
      )";
      EXPECT_EQ(
          testGetJsonObjectOnce(
              R"(
      {"charset": "utf-8","order_info": {"amount": "16900","checkout_id": "1","sub_order_info": [{"sale_channel": "default.poi_anchor.default","merchant_area_id": "","actual_amount": "16900","merchant_nickname": "北京袋鼠吉厨餐饮管理有限公司","poi_num": "275","enter_source": "poi","fee_type": "0","product_sub_type": "0","sale_start_time": "1711012860","first_category_name": "美食","over_sea_flag": "0","price": "16900","product_id": "1764125373947920","third_category_name": "烤肉","merchant_city_id": "110100","code_valid_start_time": "0","merchant_prov_id": "110000","code_valid_end_time": "0","create_time": "","merchant_name": "北京袋鼠吉厨餐饮管理有限公司","sku_id": "1764125373947920","user_limit": "6","product_name": "【聚会必点】16荤16素韩式肥牛烤肉3-5人套餐（含电烤炉+可乐）","order_item_id": [],"shop_id": "7211441316230596667","commodity_ec_discount_amount": "0","user_id": "2101903216082967","second_cid": "1005000","author_id": "59837929947","room_anchor_id": "<nil>","order_limit": "6","order_id": "","merchant_userid": "200_2184047828143099","room_id": "<nil>","third_cid": "1005001","order_scene": "","merchant_address": "北京市大兴区庆祥南路29号院5号楼3层309","first_cid": "1000000","poi_enter_page": "others_homepage","quantity": "1","detail_enter_page": "poi_page","item_id": "7338666391777627401","service_provider_name": "","miniapp_name": "","trade_limit": "","commodity_amount": "16900","detail_enter_method": "card","product_type": "14","service_provider_id": "","sale_end_time": "1742016119","order_source": "others_homepage","miniapp_id": "","second_category_name": "烤肉"},{"commodity_amount": "0","product_type": "14","commodity_ec_discount_amount": "0","actual_amount": "0","sku_id": "0","fee_type": "1"},{"shop_id": "1111111","commodity_ec_discount_amount": "0"}]}}
      )",
              "$.order_info.sub_order_info[*].shop_id"),
          "[\"7211441316230596667\",\"1111111\"]");
      EXPECT_EQ(
          testGetJsonObjectOnce(
              R"(
      {"charset": "utf-8","order_info": {"amount": "16900","checkout_id": "1","sub_order_info": [{"sale_channel": "default.poi_anchor.default","merchant_area_id": "","actual_amount": "16900","merchant_nickname": "北京袋鼠吉厨餐饮管理有限公司","poi_num": "275","enter_source": "poi","fee_type": "0","product_sub_type": "0","sale_start_time": "1711012860","first_category_name": "美食","over_sea_flag": "0","price": "16900","product_id": "1764125373947920","third_category_name": "烤肉","merchant_city_id": "110100","code_valid_start_time": "0","merchant_prov_id": "110000","code_valid_end_time": "0","create_time": "","merchant_name": "北京袋鼠吉厨餐饮管理有限公司","sku_id": "1764125373947920","user_limit": "6","product_name": "【聚会必点】16荤16素韩式肥牛烤肉3-5人套餐（含电烤炉+可乐）","order_item_id": [],"commodity_ec_discount_amount": "0","user_id": "2101903216082967","second_cid": "1005000","author_id": "59837929947","room_anchor_id": "<nil>","order_limit": "6","order_id": "","merchant_userid": "200_2184047828143099","room_id": "<nil>","third_cid": "1005001","order_scene": "","merchant_address": "北京市大兴区庆祥南路29号院5号楼3层309","first_cid": "1000000","poi_enter_page": "others_homepage","quantity": "1","detail_enter_page": "poi_page","item_id": "7338666391777627401","service_provider_name": "","miniapp_name": "","trade_limit": "","commodity_amount": "16900","detail_enter_method": "card","product_type": "14","service_provider_id": "","sale_end_time": "1742016119","order_source": "others_homepage","miniapp_id": "","second_category_name": "烤肉"},{"commodity_amount": "0","product_type": "14","commodity_ec_discount_amount": "0","actual_amount": "0","shop_id": "7211441316230596667","sku_id": "0","fee_type": "1"},{"shop_id": "1111111","commodity_ec_discount_amount": "0"}]}}
      )",
              "$.order_info.sub_order_info[*].shop_id"),
          "[\"7211441316230596667\",\"1111111\"]");
      EXPECT_EQ(
          testGetJsonObjectOnce(
              R"(
      {"charset": "utf-8","order_info": {"amount": "16900","checkout_id": "1","sub_order_info": [{"sale_channel": "default.poi_anchor.default","merchant_area_id": "","actual_amount": "16900","merchant_nickname": "北京袋鼠吉厨餐饮管理有限公司","poi_num": "275","enter_source": "poi","fee_type": "0","product_sub_type": "0","sale_start_time": "1711012860","first_category_name": "美食","over_sea_flag": "0","price": "16900","product_id": "1764125373947920","third_category_name": "烤肉","merchant_city_id": "110100","code_valid_start_time": "0","merchant_prov_id": "110000","code_valid_end_time": "0","create_time": "","merchant_name": "北京袋鼠吉厨餐饮管理有限公司","sku_id": "1764125373947920","user_limit": "6","product_name": "【聚会必点】16荤16素韩式肥牛烤肉3-5人套餐（含电烤炉+可乐）","order_item_id": [],"shop_id": "7211441316230596667","commodity_ec_discount_amount": "0","user_id": "2101903216082967","second_cid": "1005000","author_id": "59837929947","room_anchor_id": "<nil>","order_limit": "6","order_id": "","merchant_userid": "200_2184047828143099","room_id": "<nil>","third_cid": "1005001","order_scene": "","merchant_address": "北京市大兴区庆祥南路29号院5号楼3层309","first_cid": "1000000","poi_enter_page": "others_homepage","quantity": "1","detail_enter_page": "poi_page","item_id": "7338666391777627401","service_provider_name": "","miniapp_name": "","trade_limit": "","commodity_amount": "16900","detail_enter_method": "card","product_type": "14","service_provider_id": "","sale_end_time": "1742016119","order_source": "others_homepage","miniapp_id": "","second_category_name": "烤肉"},{"commodity_amount": "0","product_type": "14","commodity_ec_discount_amount": "0","actual_amount": "0","shop_id": "1111111","sku_id": "0","fee_type": "1"},{"commodity_ec_discount_amount": "0"}]}}
      )",
              "$.order_info.sub_order_info[*].shop_id"),
          "[\"7211441316230596667\",\"1111111\"]");
    }
  }
}

TEST_F(JsonFucntionTest, CornerCases) {
  // test dom
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "true"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      // json, query, expected result
      using TestCase = std::tuple<std::string, std::string, std::string>;
      std::vector<TestCase> cases{
          {R"({"ab\"cd\"ef": 2})", R"($["ab\"cd\"ef"])", "2"},
      };

      for (auto& c : cases) {
        EXPECT_EQ(
            testGetJsonObjectOnce(std::get<0>(c), std::get<1>(c)).value(),
            std::get<2>(c));
      }
    }
    {
      auto json = R"(   {"name": {"name" : "bytedance"}}  )";
      EXPECT_EQ(
          testGetJsonObjectOnce(json, "$.name.name.name.name.name"),
          std::nullopt);
    }
  }

  // test ondemand
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "false"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      // json, query, expected result
      using TestCase = std::tuple<std::string, std::string, std::string>;
      std::vector<TestCase> cases{
          {R"({"ab\"cd\"ef": 2})", R"($["ab\"cd\"ef"])", "2"},
      };

      for (auto& c : cases) {
        EXPECT_EQ(
            testGetJsonObjectOnce(std::get<0>(c), std::get<1>(c)).value(),
            std::get<2>(c));
      }
    }
    {
      auto json = R"(   {"name": {"name" : "bytedance"}}  )";
      EXPECT_EQ(
          testGetJsonObjectOnce(json, "$.name.name.name.name.name"),
          std::nullopt);
    }
  }
}

TEST_F(JsonFucntionTest, number) {
  // test dom
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "true"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json = R"( {"key": 0.0001, "value": 32 }  )";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.key"), "1.0E-4");
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.value"), "32");
    }
  }

  // test ondemand
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "false"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json = R"( {"key": 0.0001, "value": 32 }  )";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.key"), "1.0E-4");
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.value"), "32");
    }
  }
}

TEST_F(JsonFucntionTest, arrayWildcard) {
  auto json = R"(
  {
    "a": {
      "b": [
        [
          [
            {
              "c": 1
            },
            {
              "c": 2
            }
          ]
        ]
      ]
    }
  }
  )";
  auto expected = "[1,2]";
  auto jsonPath = "$.a.b[0][0][*].c";
  auto result = testGetJsonObjectOnce(json, jsonPath);
  LOG(INFO) << result.value_or("null");
  EXPECT_EQ(testGetJsonObjectOnce(json, jsonPath), expected);
}

TEST_F(JsonFucntionTest, nullBehavior) {
  // test dom
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "true"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json = R"( {"name": "张三丰", "height": null} )";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.height"), std::nullopt);
    }
    {
      auto json = R"( {"name": "张三丰", "height": [null]} )";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.height[*]"), "null");
    }
    {
      auto json = R"( {"name": "张三丰", "height": [null, null]} )";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.height[*]"), "[null,null]");
    }
  }

  // test ondemand
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "false"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json = R"( {"name": "张三丰", "height": null} )";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.height"), std::nullopt);
    }
    {
      auto json = R"( {"name": "张三丰", "height": [null]} )";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.height[*]"), "null");
    }
    {
      auto json = R"( {"name": "张三丰", "height": [null, null]} )";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.height[*]"), "[null,null]");
    }
  }
}

TEST_F(JsonFucntionTest, invalidJson) {
  {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kThrowExceptionWhenEncounterBadJson, "false"},
         {core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "false"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, "false"}});
    auto json =
        R"( {"姓名":"xiaoxiao", "性别": false, "身高": Nan, "是否过线": true} )";
    EXPECT_EQ(testGetJsonObjectOnce(json, "$.姓名"), "xiaoxiao");
    EXPECT_EQ(testGetJsonObjectOnce(json, "$.性别"), "false");
    EXPECT_EQ(testGetJsonObjectOnce(json, "$.身高"), std::nullopt);
    EXPECT_EQ(testGetJsonObjectOnce(json, "$.是否过线"), "true");
  }
  {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kThrowExceptionWhenEncounterBadJson, "true"},
         {core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "false"}});
    auto json =
        R"( {"姓名":"xiaoxiao", "性别": false, "身高": Nan, "是否过线": true} )";
    EXPECT_EQ(testGetJsonObjectOnce(json, "$.身高"), std::nullopt);
  }
  {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kThrowExceptionWhenEncounterBadJson, "true"},
         {core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "true"}});
    auto json =
        R"( {"姓名":"xiaoxiao", "性别": false, "身高": Nan, "是否过线": true} )";
    EXPECT_THROW(testGetJsonObjectOnce(json, "$.姓名"), BoltUserError);
    EXPECT_THROW(testGetJsonObjectOnce(json, "$.性别"), BoltUserError);
    EXPECT_THROW(testGetJsonObjectOnce(json, "$.身高"), BoltUserError);
    EXPECT_THROW(testGetJsonObjectOnce(json, "$.是否过线"), BoltUserError);
  }
  {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kThrowExceptionWhenEncounterBadJson, "false"},
         {core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "true"}});
    auto json =
        R"( {"姓名":"xiaoxiao", "性别": false, "身高": Nan, "是否过线": true} )";
    EXPECT_EQ(testGetJsonObjectOnce(json, "$.姓名"), std::nullopt);
    EXPECT_EQ(testGetJsonObjectOnce(json, "$.性别"), std::nullopt);
    EXPECT_EQ(testGetJsonObjectOnce(json, "$.身高"), std::nullopt);
    EXPECT_EQ(testGetJsonObjectOnce(json, "$.是否过线"), std::nullopt);
  }
}

TEST_F(JsonFucntionTest, invalidJsonPath) {
  // test dom
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "true"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json =
          R"( {"姓名":"xiaoxiao", "性别": false, "身高": 1.82, "是否过线": true} )";
      EXPECT_EQ(testGetJsonObjectOnce(json, "dfsfsd"), std::nullopt);
      EXPECT_EQ(testGetJsonObjectOnce(json, "d$fggh.xx$"), std::nullopt);
      EXPECT_EQ(testGetJsonObjectOnce(json, "gdsfgs"), std::nullopt);
      EXPECT_EQ(testGetJsonObjectOnce(json, ".$性别"), std::nullopt);
    }
  }

  // test ondemand
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "false"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json =
          R"( {"姓名":"xiaoxiao", "性别": false, "身高": 1.82, "是否过线": true} )";
      EXPECT_EQ(testGetJsonObjectOnce(json, "dfsfsd"), std::nullopt);
      EXPECT_EQ(testGetJsonObjectOnce(json, "d$fggh.xx$"), std::nullopt);
      EXPECT_EQ(testGetJsonObjectOnce(json, "gdsfgs"), std::nullopt);
      EXPECT_EQ(testGetJsonObjectOnce(json, ".$性别"), std::nullopt);
    }
  }
}

TEST_F(JsonFucntionTest, emptyInput) {
  // test dom
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "true"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json = "";
      EXPECT_EQ(testGetJsonObjectOnce(json, "性别"), std::nullopt);
    }
    {
      auto json = "[]";
      EXPECT_EQ(
          testGetJsonObjectOnce(json, "$[*].inspect_status"), std::nullopt);
    }
  }

  // test ondemand
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "false"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json = "";
      EXPECT_EQ(testGetJsonObjectOnce(json, "性别"), std::nullopt);
    }
    {
      auto json = "[]";
      EXPECT_EQ(
          testGetJsonObjectOnce(json, "$[*].inspect_status"), std::nullopt);
    }
  }
}

TEST_F(JsonFucntionTest, emptyStringField) {
  // test dom
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "true"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json =
          R"( {"user_new_counter":true,"app_total_report":1,"reason_total_report":1,"use_new_report_service":true,"owner_total_report":2,"is_transfer":true,"device_id":"66271100845","reason_app_total_report":1,"already_send_notice":true,"app_id":1128,"source":"homepage_hot","origin_aweme_uri":"","comment_report_date":"","report_desc":"","owner_app_total_report":1,"origin_owner":"","safe_domain_id":6932394787169373454,"report_is_teen":0,"origin_author_uri":"","current_play_position":"6090","report_count":1,"notice_time":1614127756,"report_classy":"V_report_check","classy_count":1,"safe_domain_model":2,"total_report":1,"area":"CN","related_username":"","related_ttid":""} )";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.report_desc"), "");
    }
    {
      auto json =
          R"( {"user_new_counter":null,"app_total_report":1,"reason_total_report":1,"use_new_report_service":true,"owner_total_report":2,"is_transfer":true,"device_id":"66271100845","reason_app_total_report":1,"already_send_notice":true,"app_id":1128,"source":"homepage_hot","origin_aweme_uri":"","comment_report_date":"","report_desc":"","owner_app_total_report":1,"origin_owner":"","safe_domain_id":6932394787169373454,"report_is_teen":0,"origin_author_uri":"","current_play_position":"6090","report_count":1,"notice_time":1614127756,"report_classy":"V_report_check","classy_count":1,"safe_domain_model":2,"total_report":1,"area":"CN","related_username":"","related_ttid":""} )";
      EXPECT_EQ(
          testGetJsonObjectOnce(json, "$.user_new_counter"), std::nullopt);
    }
  }

  // test ondemand
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "false"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json =
          R"( {"user_new_counter":true,"app_total_report":1,"reason_total_report":1,"use_new_report_service":true,"owner_total_report":2,"is_transfer":true,"device_id":"66271100845","reason_app_total_report":1,"already_send_notice":true,"app_id":1128,"source":"homepage_hot","origin_aweme_uri":"","comment_report_date":"","report_desc":"","owner_app_total_report":1,"origin_owner":"","safe_domain_id":6932394787169373454,"report_is_teen":0,"origin_author_uri":"","current_play_position":"6090","report_count":1,"notice_time":1614127756,"report_classy":"V_report_check","classy_count":1,"safe_domain_model":2,"total_report":1,"area":"CN","related_username":"","related_ttid":""} )";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.report_desc"), "");
    }
    {
      auto json =
          R"( {"user_new_counter":null,"app_total_report":1,"reason_total_report":1,"use_new_report_service":true,"owner_total_report":2,"is_transfer":true,"device_id":"66271100845","reason_app_total_report":1,"already_send_notice":true,"app_id":1128,"source":"homepage_hot","origin_aweme_uri":"","comment_report_date":"","report_desc":"","owner_app_total_report":1,"origin_owner":"","safe_domain_id":6932394787169373454,"report_is_teen":0,"origin_author_uri":"","current_play_position":"6090","report_count":1,"notice_time":1614127756,"report_classy":"V_report_check","classy_count":1,"safe_domain_model":2,"total_report":1,"area":"CN","related_username":"","related_ttid":""} )";
      EXPECT_EQ(
          testGetJsonObjectOnce(json, "$.user_new_counter"), std::nullopt);
    }
  }
}

TEST_F(JsonFucntionTest, scalarValue) {
  // test dom
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "true"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json = "null";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$"), "null");
    }
    {
      auto json = "null";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.name"), std::nullopt);
    }
    {
      auto json = "123";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$"), "123");
    }
    {
      auto json = "\"abc\"";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$"), "abc");
    }
    {
      auto json = "true";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$"), "true");
    }
    {
      auto json = "false";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$"), "false");
    }
  }

  // test ondemand
  for (auto value : {"true", "false"}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"},
         {core::QueryConfig::kUseDOMParserInGetJsonObject, "false"},
         {core::QueryConfig::kGetJsonObjectEscapeEmoji, value}});
    {
      auto json = "null";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$"), "null");
    }
    {
      auto json = "null";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$.name"), std::nullopt);
    }
    {
      auto json = "123";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$"), "123");
    }
    {
      auto json = "\"abc\"";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$"), "abc");
    }
    {
      auto json = "true";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$"), "true");
    }
    {
      auto json = "false";
      EXPECT_EQ(testGetJsonObjectOnce(json, "$"), "false");
    }
  }
}

TEST_F(JsonFucntionTest, jsonTupleParityEscape) {
  {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"}});
    auto json = R"(  { "a":"\""} )";
    EXPECT_EQ(testGetJsonObjectOnce(json, "$.a"), "\"");

    json = R"(  "\"" )";
    EXPECT_EQ(testGetJsonObjectOnce(json, "$"), "\"");

    json = R"( {"id":0,"sku_id":"\""})";
    EXPECT_EQ(testGetJsonObjectOnce(json, "$.sku_id"), "\"");
    json = R"( {"id":0,"sku_id":"\\\""})";
    EXPECT_EQ(testGetJsonObjectOnce(json, "$.sku_id"), "\\\"");
  }
}
TEST_F(JsonFucntionTest, jsonInfiniteLoop) {
  {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kUseSonicJson, "true"}});
    auto json = R"(  [8 )";
    EXPECT_EQ(
        testGetJsonObjectOnce(json, "$.motor_content_boost"), std::nullopt);
  }
}

} // namespace

} // namespace bytedance::bolt::functions::sparksql::test
