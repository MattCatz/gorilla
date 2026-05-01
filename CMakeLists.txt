cmake_minimum_required(VERSION 3.14)
project(gorilla
    VERSION 1.0.0
    DESCRIPTION "Gorilla time-series compression (VLDB 2015)"
    LANGUAGES C
)

# -------------------------------------------------------------------------
# Options
# -------------------------------------------------------------------------
option(GORILLA_BUILD_SHARED  "Build a shared library (.so/.dylib/.dll)" OFF)
option(GORILLA_BUILD_STATIC  "Build a static library (.a)"              ON)
option(GORILLA_BUILD_TESTS   "Build the test suite"                     ON)
option(GORILLA_INSTALL       "Generate install targets"                 ON)

# Require at least one library type
if(NOT GORILLA_BUILD_SHARED AND NOT GORILLA_BUILD_STATIC)
    message(FATAL_ERROR "At least one of GORILLA_BUILD_SHARED or GORILLA_BUILD_STATIC must be ON")
endif()

# -------------------------------------------------------------------------
# C standard
# -------------------------------------------------------------------------
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

# -------------------------------------------------------------------------
# Compiler warnings
# -------------------------------------------------------------------------
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    add_compile_options(-Wall -Wextra -Wpedantic -Wno-unused-parameter)
endif()

# -------------------------------------------------------------------------
# Source files
# -------------------------------------------------------------------------
set(GORILLA_SOURCES src/gorilla.c)
set(GORILLA_HEADERS include/gorilla.h)

# -------------------------------------------------------------------------
# Static library
# -------------------------------------------------------------------------
if(GORILLA_BUILD_STATIC)
    add_library(gorilla_static STATIC ${GORILLA_SOURCES})
    target_include_directories(gorilla_static
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
    )
    set_target_properties(gorilla_static PROPERTIES
        OUTPUT_NAME gorilla
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        POSITION_INDEPENDENT_CODE ON   # allows linking into shared libs later
    )
    # Alias so consumers can use gorilla::gorilla regardless of build type
    add_library(gorilla::gorilla_static ALIAS gorilla_static)
endif()

# -------------------------------------------------------------------------
# Shared library
# -------------------------------------------------------------------------
if(GORILLA_BUILD_SHARED)
    add_library(gorilla_shared SHARED ${GORILLA_SOURCES})
    target_include_directories(gorilla_shared
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
    )
    set_target_properties(gorilla_shared PROPERTIES
        OUTPUT_NAME gorilla
        SOVERSION ${PROJECT_VERSION_MAJOR}
        VERSION   ${PROJECT_VERSION}
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"  # Windows DLL
    )
    add_library(gorilla::gorilla_shared ALIAS gorilla_shared)
endif()

# -------------------------------------------------------------------------
# Convenience alias: gorilla::gorilla → prefer shared if both built
# -------------------------------------------------------------------------
if(GORILLA_BUILD_SHARED)
    add_library(gorilla::gorilla ALIAS gorilla_shared)
elseif(GORILLA_BUILD_STATIC)
    add_library(gorilla::gorilla ALIAS gorilla_static)
endif()

# -------------------------------------------------------------------------
# Tests
# -------------------------------------------------------------------------
if(GORILLA_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# -------------------------------------------------------------------------
# Install
# -------------------------------------------------------------------------
if(GORILLA_INSTALL)
    include(GNUInstallDirs)

    if(GORILLA_BUILD_STATIC)
        install(TARGETS gorilla_static
            EXPORT gorillaTargets
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        )
    endif()

    if(GORILLA_BUILD_SHARED)
        install(TARGETS gorilla_shared
            EXPORT gorillaTargets
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        )
    endif()

    install(FILES ${GORILLA_HEADERS}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

    # CMake package config so downstream can do find_package(gorilla)
    include(CMakePackageConfigHelpers)

    install(EXPORT gorillaTargets
        FILE        gorillaTargets.cmake
        NAMESPACE   gorilla::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/gorilla
    )

    configure_package_config_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/gorillaConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/gorillaConfig.cmake
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/gorilla
    )

    write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/gorillaConfigVersion.cmake
        VERSION       ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
    )

    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/gorillaConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/gorillaConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/gorilla
    )
endif()
