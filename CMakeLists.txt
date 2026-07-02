cmake_minimum_required(VERSION 3.16)
project(recordstore CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O2")
add_compile_options(-Wall -Wextra)

find_package(Threads REQUIRED)

# Library: the decoupled writer + reader halves. src/ is private (internal format).
add_library(recordstore src/record_writer.cpp src/record_reader.cpp)
target_include_directories(recordstore PUBLIC include PRIVATE src)
target_link_libraries(recordstore PUBLIC Threads::Threads)

# Driver executable.
add_executable(main app/main.cpp)
target_link_libraries(main PRIVATE recordstore)

# Benchmark.
add_executable(bench bench/bench.cpp)
target_link_libraries(bench PRIVATE recordstore)

enable_testing()

# GoogleTest: prefer a system install; fall back to fetching a pinned release.
find_package(GTest QUIET)
if(NOT GTest_FOUND)
  include(FetchContent)
  FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

include(GoogleTest)
foreach(t test_crc32c test_writer_reader test_recovery test_fuzz test_concurrency)
  add_executable(${t} test/${t}.cpp)
  target_include_directories(${t} PRIVATE include)
  target_link_libraries(${t} PRIVATE recordstore GTest::gtest GTest::gtest_main)
  gtest_discover_tests(${t})
endforeach()
