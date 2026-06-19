cmake_minimum_required(VERSION 3.16)
project(blobstore CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O2")
add_compile_options(-Wall -Wextra)

find_package(Threads REQUIRED)

add_library(blobstore src/blob_store.cpp)
target_include_directories(blobstore PUBLIC include)
target_link_libraries(blobstore PUBLIC Threads::Threads)

add_executable(bench bench/bench.cpp)
target_link_libraries(bench PRIVATE blobstore)

enable_testing()
foreach(t test_crc32c test_cache test_store test_recovery)
  add_executable(${t} test/${t}.cpp)
  target_include_directories(${t} PRIVATE include test)
  target_link_libraries(${t} PRIVATE blobstore)
  add_test(NAME ${t} COMMAND ${t})
endforeach()
