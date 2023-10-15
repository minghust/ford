set(STORAGE_TEST_SRC storage_test.cpp)

add_executable(storage_test ${STORAGE_TEST_SRC})
target_link_libraries(storage_test pthread ford ${DYNAMIC_LIB} ${BRPC_LIB})