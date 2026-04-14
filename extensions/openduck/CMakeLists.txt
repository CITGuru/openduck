cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME openduck)

find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
  find_package(Protobuf REQUIRED)
endif()
find_package(gRPC CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)

# ── Proto generation ─────────────────────────────────────────────────────────
set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../proto")
set(PROTO_FILE "${PROTO_DIR}/openduck/v1/execution.proto")
set(PROTO_OUT "${CMAKE_CURRENT_BINARY_DIR}/gen")
file(MAKE_DIRECTORY "${PROTO_OUT}/openduck/v1")

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin HINTS ${gRPC_DIR}/../../../bin)
if(NOT GRPC_CPP_PLUGIN)
  set(GRPC_CPP_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

add_custom_command(
  OUTPUT "${PROTO_OUT}/openduck/v1/execution.pb.cc"
         "${PROTO_OUT}/openduck/v1/execution.pb.h"
         "${PROTO_OUT}/openduck/v1/execution.grpc.pb.cc"
         "${PROTO_OUT}/openduck/v1/execution.grpc.pb.h"
  COMMAND protobuf::protoc
  ARGS --proto_path="${PROTO_DIR}"
       --cpp_out="${PROTO_OUT}"
       --grpc_out="${PROTO_OUT}"
       --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
       "${PROTO_FILE}"
  DEPENDS "${PROTO_FILE}"
  COMMENT "Generating C++ gRPC stubs from execution.proto"
)

add_custom_target(openduck_proto_gen
  DEPENDS "${PROTO_OUT}/openduck/v1/execution.pb.cc"
          "${PROTO_OUT}/openduck/v1/execution.grpc.pb.cc"
)

# ── Extension sources ────────────────────────────────────────────────────────
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
  src/openduck_extension.cpp
  src/openduck_catalog.cpp
  src/openduck_schema.cpp
  src/openduck_table_entry.cpp
  src/openduck_filesystem.cpp
  src/grpc_client.cpp
  src/arrow_bridge.cpp
  "${PROTO_OUT}/openduck/v1/execution.pb.cc"
  "${PROTO_OUT}/openduck/v1/execution.grpc.pb.cc"
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# ── Optional Rust bridge (PgStorageBackend via C ABI) ─────────────────────
# Build: cargo build -p diff-bridge --release
# Then pass: cmake -DOPENDUCK_BRIDGE_LIB=/path/to/libdiff_bridge.a ...
option(OPENDUCK_BRIDGE_LIB "Path to libdiff_bridge.a (Rust static library)" "")

foreach(_tgt ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
  add_dependencies(${_tgt} openduck_proto_gen)
  target_include_directories(${_tgt} PRIVATE "${PROTO_OUT}")
  target_link_libraries(${_tgt} gRPC::grpc++ protobuf::libprotobuf Arrow::arrow_static)

  if(OPENDUCK_BRIDGE_LIB AND NOT "${OPENDUCK_BRIDGE_LIB}" STREQUAL "")
    target_compile_definitions(${_tgt} PRIVATE OPENDUCK_HAS_BRIDGE=1)
    target_link_libraries(${_tgt} "${OPENDUCK_BRIDGE_LIB}")
    if(APPLE)
      target_link_libraries(${_tgt} "-framework Security" "-framework CoreFoundation" "-framework SystemConfiguration")
    endif()
    target_link_libraries(${_tgt} pthread dl m)
    message(STATUS "OpenDuck bridge enabled: ${OPENDUCK_BRIDGE_LIB}")
  endif()
endforeach()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
