# Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
# SPDX-License-Identifier: MIT

# A generic test target.
add_library(nats.test.base)
target_sources(nats.test.base PRIVATE Main.cpp)
target_include_directories(nats.test.base PUBLIC ../extern)
target_link_libraries(nats.test.base PUBLIC nats.base)
target_compile_definitions(nats.test.base PUBLIC NATS_TEST)

add_executable(nats.test)
target_sources(nats.test PUBLIC Disruptor.cpp Nuid.cpp)
target_link_libraries(nats.test PRIVATE nats.test.base)
set_target_properties(nats.test PROPERTIES OUTPUT_NAME "nats")

# add_subdirectory(examples)

# Add tests to ctest
add_test(NAME nats.test COMMAND nats.test)

