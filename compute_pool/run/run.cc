// Author: Ming Zhang
// Copyright (c) 2022

#include "worker/handler.h"

// Entrance to run threads that spawn coroutines as coordinators to run distributed transactions
int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "./run <benchmark_name> <system_name> <thread_num>(optional) <coroutine_num>(optional). E.g., ./run tatp ford 16 8" << std::endl;
    return 0;
  }

  Handler* handler = new Handler();
  handler->ConfigureComputeNode(argc, argv);
  handler->GenThreads(std::string(argv[1]));
  handler->OutputResult(std::string(argv[1]), std::string(argv[2]));
}
