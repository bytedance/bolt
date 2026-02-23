# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

bolt_add_gtest(
    bolt_paimon_connector_test
    PaimonConnectorTest.cpp
)

target_link_libraries(bolt_paimon_connector_test
    bolt_connector_paimon
    bolt_exec_tests_utils
)
