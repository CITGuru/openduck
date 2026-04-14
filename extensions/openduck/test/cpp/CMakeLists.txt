cmake_minimum_required(VERSION 3.12)
project(openduck_arrow_bridge_test)

find_package(Arrow CONFIG REQUIRED)
find_package(Threads REQUIRED)
find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
  find_package(Protobuf REQUIRED)
endif()
find_package(gRPC CONFIG REQUIRED)

set(DUCKDB_SRC "${CMAKE_CURRENT_SOURCE_DIR}/../../duckdb")
set(DUCKDB_BUILD "${CMAKE_CURRENT_SOURCE_DIR}/../../build/debug")

add_executable(test_arrow_bridge
  test_arrow_bridge.cpp
  ../../src/arrow_bridge.cpp
)

target_compile_features(test_arrow_bridge PRIVATE cxx_std_17)
target_compile_options(test_arrow_bridge PRIVATE
  -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all
)
target_link_options(test_arrow_bridge PRIVATE
  -fsanitize=address -fsanitize=undefined
)

target_include_directories(test_arrow_bridge PRIVATE
  ../../src/include
  ${DUCKDB_SRC}/src/include
  ${DUCKDB_SRC}/third_party/catch
)

target_link_libraries(test_arrow_bridge
  Arrow::arrow_static
  "${DUCKDB_BUILD}/extension/openduck/libopenduck_extension.a"
  "${DUCKDB_BUILD}/src/libduckdb_static.a"
  "${DUCKDB_BUILD}/extension/libduckdb_generated_extension_loader.a"
  "${DUCKDB_BUILD}/extension/core_functions/libcore_functions_extension.a"
  "${DUCKDB_BUILD}/extension/parquet/libparquet_extension.a"
  "${DUCKDB_BUILD}/third_party/fmt/libduckdb_fmt.a"
  "${DUCKDB_BUILD}/third_party/fsst/libduckdb_fsst.a"
  "${DUCKDB_BUILD}/third_party/hyperloglog/libduckdb_hyperloglog.a"
  "${DUCKDB_BUILD}/third_party/fastpforlib/libduckdb_fastpforlib.a"
  "${DUCKDB_BUILD}/third_party/miniz/libduckdb_miniz.a"
  "${DUCKDB_BUILD}/third_party/re2/libduckdb_re2.a"
  "${DUCKDB_BUILD}/third_party/utf8proc/libduckdb_utf8proc.a"
  "${DUCKDB_BUILD}/third_party/mbedtls/libduckdb_mbedtls.a"
  "${DUCKDB_BUILD}/third_party/libpg_query/libduckdb_pg_query.a"
  "${DUCKDB_BUILD}/third_party/yyjson/libduckdb_yyjson.a"
  "${DUCKDB_BUILD}/third_party/skiplist/libduckdb_skiplistlib.a"
  "${DUCKDB_BUILD}/third_party/zstd/libduckdb_zstd.a"
  gRPC::grpc++
  protobuf::libprotobuf
  Threads::Threads
  ${CMAKE_DL_LIBS}
)
