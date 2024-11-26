cmake_minimum_required(VERSION 3.10)
project(OdessStorage)

set(CMAKE_CXX_STANDARD 14)

aux_source_directory(Utility Utility)
aux_source_directory(RollHash RollHash)
aux_source_directory(EncodingMethod EncodingMethod)
aux_source_directory(MetadataManager MetadataManager)
aux_source_directory(Pipeline Pipeline)
aux_source_directory(FeatureMethod FeatureMethod)

add_subdirectory(gflags-2.2.2)
#add_subdirectory(xdelta)

link_directories(/root/jemalloc-5.3.0/lib/)
link_directories(/root/zstd-1.5.5/lib/)
link_directories(/root/isa-l_crypto-2.24.0/.libs)
link_directories(/root/openssl-OpenSSL_1_1_1t)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fsanitize=address")

link_libraries(gflags::gflags libisal_crypto.a pthread libcrypto.a libzstd.a)

add_executable(Odess main.cpp ${Utility} ${RollHash} ${EncodingMethod} ${MetadataManager} ${Pipeline} ${FeatureMethod} Analysis/FDistributionSolver.h)

add_executable(distriTest Test/distriTest.cpp Analysis/SFDistributionSolver.h)
