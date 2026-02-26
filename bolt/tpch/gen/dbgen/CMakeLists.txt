cmake_minimum_required(VERSION 3.0.0)
cmake_policy(SET CMP0063 NEW)

project(dbgen CXX C)

add_definitions(-DDBNAME=dss -DMAC -DORACLE -DTPCH)

bolt_add_library(
  bolt_dbgen
  bm_utils.cpp
  build.cpp
  dbgen.cpp
  dbgen_gunk.cpp
  permute.cpp
  rnd.cpp
  rng64.cpp
  speed_seed.cpp
  text.cpp
)

target_include_directories(bolt_dbgen PRIVATE include)

# Suppress warnings when compiling dbgen.
target_compile_options(bolt_dbgen PRIVATE -w)
