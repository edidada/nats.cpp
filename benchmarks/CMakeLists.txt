# Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
# SPDX-License-Identifier: MIT

# A generic benchmark target.
add_library(nats.benchmark.base INTERFACE)
# target_sources(nats.benchmark.base PRIVATE main.cpp)
target_compile_options(nats.benchmark.base INTERFACE -O2)
target_include_directories(nats.benchmark.base INTERFACE ../extern)
target_link_libraries(nats.benchmark.base INTERFACE nats.base)

# # disruptor
# add_executable(nats.benchmark.disruptor)
# target_sources(nats.benchmark.disruptor PRIVATE Disruptor.cpp)
# target_link_libraries(nats.benchmark.disruptor PRIVATE nats.benchmark.base)
# set_target_properties(nats.benchmark.disruptor PROPERTIES OUTPUT_NAME "disruptor")
#
# # nuid
# add_executable(nats.benchmark.nuid)
# target_sources(nats.benchmark.nuid PRIVATE Nuid.cpp)
# target_link_libraries(nats.benchmark.nuid PRIVATE nats.benchmark.base)
# set_target_properties(nats.benchmark.nuid PROPERTIES OUTPUT_NAME "nuid")

