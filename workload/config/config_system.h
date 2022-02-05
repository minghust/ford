// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include <unistd.h>

#include <string>

void ModifyComputeNodeConfig(int argc, char* argv[]) {
  std::string config_file = "../../../config/compute_node_config.json";
  std::string system_name = std::string(argv[1]);
  if (argc == 3) {
    std::string s1 = "sed -i '5c \"thread_num_per_machine\": " + std::string(argv[2]) + ",' " + config_file;
    std::string s2 = "sed -i '6c \"coroutine_num\": " + std::string(argv[3]) + ",' " + config_file;
    system(s1.c_str());
    system(s2.c_str());
  }

  return;
}