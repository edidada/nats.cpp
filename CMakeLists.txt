# Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
# SPDX-License-Identifier: MIT

cmake_minimum_required (VERSION 3.28)

# set(CMAKE_C_COMPILER clang)
# set(CMAKE_CXX_COMPILER clang++)

project(nats.cpp VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_FLAGS -std=c++2c)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_LINKER_TYPE GOLD)

set(NATS_USER_CONFIG_HEADER "" CACHE FILEPATH "The user config header for nats.cpp.")

add_subdirectory(src)
add_subdirectory(apps)

if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  option(NATS_BUILD_BENCHMARKS "whether or not benchmarks should be built" ON)
  option(NATS_BUILD_EXAMPLES   "whether or not examples should be built" ON)
  option(NATS_BUILD_TESTS      "whether or not tests should be built" ON)
  option(NATS_BUILD_DOCS       "whether or not docs should be built" ON)

  if(NATS_BUILD_PACKAGE)
    set(package_files include/ src/ cmake/ CMakeLists.txt LICENSE)
    add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/nats.cpp-src.zip
      COMMAND ${CMAKE_COMMAND} -E tar c ${CMAKE_CURRENT_BINARY_DIR}/nats.cpp-src.zip --format=zip -- ${package_files}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      DEPENDS ${package_files})
    add_custom_target(nats_package DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/nats.cpp-src.zip)
  endif()

  if(NATS_BUILD_EXAMPLES)
    add_subdirectory(examples)
  endif()
  if(NATS_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
  endif()
  if(NATS_BUILD_TESTS)
    add_subdirectory(tests)
  endif()
  if(NATS_BUILD_DOCS)
    # add_subdirectory(docs EXCLUDE_FROM_ALL)
  endif()
endif()

