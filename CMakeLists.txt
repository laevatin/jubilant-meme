cmake_minimum_required(VERSION 3.16)
project(recordstore CXX)

set(CMAKE_CXX_STANDARD 17)
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

enable_testing()
foreach(t test_crc32c test_writer_reader)
  add_executable(${t} test/${t}.cpp)
  target_include_directories(${t} PRIVATE include test)
  target_link_libraries(${t} PRIVATE recordstore)
  add_test(NAME ${t} COMMAND ${t})
endforeach()
