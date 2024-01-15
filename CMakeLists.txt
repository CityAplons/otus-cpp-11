cmake_minimum_required(VERSION 3.10)

set(PROJECT_REVISION 1 CACHE INTERNAL "Project revision (CI)")
project(mapreduce
    VERSION 0.0.${PROJECT_REVISION}
    DESCRIPTION "General CMake template for C++ projects by N.Mikhailovskiy"
    HOMEPAGE_URL "https://github.com/CityAplons/cmake-multiplatform-template"
    LANGUAGES CXX
)

set_property(GLOBAL PROPERTY G_CXX_STANDARD 20)
set_property(GLOBAL PROPERTY G_CXX_STANDARD_REQUIRED ON)

# Boost
set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_RELEASE_LIBS ON)
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost COMPONENTS filesystem program_options thread) 

# Include utility functions
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(GNUInstallDirs)
include(compiler)
include(test)

# Transparent versioning
include_directories(cmake/version)
configure_file(cmake/version/version.h.in version.h)
configure_file(cmake/version/VERSION.in VERSION)
add_library(version cmake/version/project.cpp)
target_include_directories(version
    PRIVATE "${CMAKE_BINARY_DIR}"
)

# Describe build targets
add_executable(mapreduce src/main.cpp src/mapreduce.cpp src/runner.cpp)
target_link_libraries(mapreduce PRIVATE
    version
    Boost::filesystem
    Boost::program_options
    Boost::thread
)

set_compile_options(mapreduce)
set_compile_options(version)

install(TARGETS mapreduce
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(FILES test.txt
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# Describe unit tests
enable_testing()
add_unit_test(test/test_version.cpp version)

# Packaging
option(MAKE_PACKAGE "Add 'package' and 'package_source' build targets for packages" ON)
if (MAKE_PACKAGE)
	add_subdirectory(cmake/packaging)
endif()