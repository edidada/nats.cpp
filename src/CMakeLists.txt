# Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
# SPDX-License-Identifier: MIT

add_library(nats.base INTERFACE)
target_compile_options(nats.base INTERFACE 
  -std=c++2c
  -march=native -Wpedantic -Wall -Wextra -Wsign-conversion -Wconversion)
target_include_directories(nats.base
  SYSTEM INTERFACE ../extern ~/Apps/boost_1_85_0)
target_include_directories(nats.base INTERFACE ../include)
target_link_directories(nats.base INTERFACE ~/Apps/boost_1_85_0/stage/lib)
target_link_libraries(nats.base INTERFACE stdc++exp)

add_library(nats)
target_sources(nats PUBLIC Protocol.cpp Client.cpp)
target_link_libraries(nats PUBLIC nats.base)

if(NATS_USER_CONFIG_HEADER)
  if(EXISTS ${NATS_USER_CONFIG_HEADER})
    target_compile_definitions(nats.base INTERFACE NATS_USER_CONFIG_HEADER="${NATS_USER_CONFIG_HEADER}")
  else()
    message(WARNING "User config header for nats.cpp specified as '${NATS_USER_CONFIG_HEADER}', but not found")
  endif()
endif()

include(GNUInstallDirs)

# add_executable(nats)
# target_sources(nats PUBLIC Main.cpp)
# target_link_libraries(nats PUBLIC nats.base)

