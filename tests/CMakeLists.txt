cmake_minimum_required(VERSION 3.14)

# -------------------------------------------------------------------------
# Fetch Unity test framework
# https://github.com/ThrowTheSwitch/Unity
# -------------------------------------------------------------------------
include(FetchContent)

FetchContent_Declare(
    unity
    GIT_REPOSITORY https://github.com/ThrowTheSwitch/Unity.git
    GIT_TAG        v2.6.0
    GIT_SHALLOW    TRUE
)

# Unity's CMakeLists exposes a `unity` target when fetched this way.
# We set UNITY_EXTENSION_FIXTURE OFF to keep it minimal.
set(UNITY_EXTENSION_FIXTURE OFF CACHE BOOL "" FORCE)
set(UNITY_EXTENSION_MEMORY  OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(unity)

# Enable double-precision float assertions in Unity
target_compile_definitions(unity PUBLIC UNITY_INCLUDE_DOUBLE)

# -------------------------------------------------------------------------
# Determine which gorilla library target to link tests against.
# Prefer static for tests (no rpath / install headaches).
# -------------------------------------------------------------------------
if(TARGET gorilla_static)
    set(GORILLA_TEST_LIB gorilla_static)
elseif(TARGET gorilla_shared)
    set(GORILLA_TEST_LIB gorilla_shared)
else()
    message(FATAL_ERROR "No gorilla library target found for test linkage")
endif()

# -------------------------------------------------------------------------
# Helper macro: add_gorilla_test(name source...)
#   Compiles a test executable, links Unity + gorilla, registers with CTest.
# -------------------------------------------------------------------------
macro(add_gorilla_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE unity ${GORILLA_TEST_LIB})
    target_include_directories(${name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${unity_SOURCE_DIR}/src
    )
    add_test(NAME ${name} COMMAND ${name})
    # Colour output is nice in terminals
    set_tests_properties(${name} PROPERTIES
        ENVIRONMENT "UNITY_OUTPUT_COLOR=1"
    )
endmacro()

# -------------------------------------------------------------------------
# Test suites
# -------------------------------------------------------------------------
add_gorilla_test(test_bitstream   test_bitstream.c)
add_gorilla_test(test_timestamps  test_timestamps.c)
add_gorilla_test(test_values      test_values.c)
add_gorilla_test(test_edge_cases  test_edge_cases.c)
add_gorilla_test(test_streaming   test_streaming.c)
