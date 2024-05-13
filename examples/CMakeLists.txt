# Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
# SPDX-License-Identifier: MIT

add_library(nats.example.base INTERFACE)
target_link_libraries(nats.example.base INTERFACE nats.base)

# playground
add_executable(nats.example.playground)
target_sources(nats.example.playground PRIVATE Playground.cpp)
target_link_libraries(nats.example.playground PUBLIC nats.example.base)
set_target_properties(nats.example.playground PROPERTIES OUTPUT_NAME "playground")


