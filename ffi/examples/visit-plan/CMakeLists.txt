cmake_minimum_required(VERSION 3.12)
project(visit_plan)

add_executable(visit_plan visit_plan.c)
target_compile_definitions(visit_plan PUBLIC DEFINE_DEFAULT_ENGINE_BASE DEFINE_TEST_FFI)
target_include_directories(visit_plan PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/ffi-headers")
target_link_directories(visit_plan PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../../target/debug")
target_link_libraries(visit_plan PUBLIC delta_kernel_ffi)
target_compile_options(visit_plan PUBLIC)

if(MSVC)
  target_compile_options(visit_plan PRIVATE /W4 /WX)
else()
  # no-strict-prototypes because arrow headers have fn defs without prototypes
  target_compile_options(visit_plan PRIVATE -Wall -Wextra -Wpedantic -Werror -Wno-strict-prototypes -g -fsanitize=address)
  target_link_options(visit_plan PRIVATE -g -fsanitize=address)
endif()

