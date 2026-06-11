bolt_add_library(
  bolt_exec_bm
  BmRowContainer.cpp
  BmRowBlockLoader.cpp
  BmRowContainerCompare.cpp
  BmRowContainerExtract.cpp
  BmRowContainerLoad.cpp
  BmRowCopier.cpp
  BmRowLayout.cpp
  BmSegmentCollection.cpp
  BmSegmentCollectionRead.cpp
  BmSegmentCollectionRelease.cpp
  BmSegmentCollectionWrite.cpp
  BmMergeReadSession.cpp
  BmStringViewRebaser.cpp
  BmRowContainerReorder.cpp
  BmRowContainerRow.cpp
  BmRowContainerSegment.cpp
  BmWindowReadSession.cpp
)

target_compile_features(bolt_exec_bm PRIVATE cxx_std_20)

target_link_libraries(
  bolt_exec_bm
  bolt_exec
  bolt_memory_bm
  bolt_vector
  bolt_common_base
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
