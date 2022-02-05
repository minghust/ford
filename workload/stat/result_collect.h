// Author: Ming Zhang
// Copyright (c) 2021

#pragma once

#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "common/common.h"

#define TEST_DURATION 1

void CollectResult(std::string workload_name, std::string system_name);