# Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
# SPDX-License-Identifier: MIT

# A generic test target.
add_library(nats.test.base INTERFACE)
target_include_directories(nats.test.base INTERFACE ../extern)
target_link_libraries(nats.test.base INTERFACE nats.base nats boost_cobalt)
target_compile_definitions(nats.test.base INTERFACE NATS_TEST)

add_executable(nats.test)
target_sources(nats.test PUBLIC
  Common.cpp Protocol.cpp Disruptor.cpp Nuid.cpp)
target_link_libraries(nats.test PRIVATE nats.test.base)
set_target_properties(nats.test PROPERTIES OUTPUT_NAME "nats")

# add_subdirectory(examples)

# Add tests to ctest
add_test(NAME nats.test COMMAND nats.test)

