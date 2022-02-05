// Author: Ming Zhang
// Copyright (c) 2021

#include "stat/result_collect.h"

std::atomic<uint64_t> tx_id_generator;
std::atomic<uint64_t> connected_t_num;
std::mutex mux;

std::vector<t_id_t> tid_vec;
std::vector<double> attemp_tp_vec;
std::vector<double> tp_vec;
std::vector<double> medianlat_vec;
std::vector<double> taillat_vec;
std::vector<double> lock_durations;

void CollectResult(std::string workload_name, std::string system_name) {
  std::ofstream of, of_detail;
  std::string res_file = "../../../bench_results/" + workload_name + "/result.txt";
  std::string detail_res_file = "../../../bench_results/" + workload_name + "/detail_result.txt";

  of.open(res_file.c_str(), std::ios::app);
  of_detail.open(detail_res_file.c_str(), std::ios::app);

  of_detail << system_name << std::endl;
  of_detail << "tid attemp_tp tp 50lat 99lat" << std::endl;

  double total_attemp_tp = 0;
  double total_tp = 0;
  double total_median = 0;
  double total_tail = 0;

  for (int i = 0; i < tid_vec.size(); i++) {
    of_detail << tid_vec[i] << " " << attemp_tp_vec[i] << " " << tp_vec[i] << " " << medianlat_vec[i] << " " << taillat_vec[i] << std::endl;
    total_attemp_tp += attemp_tp_vec[i];
    total_tp += tp_vec[i];
    total_median += medianlat_vec[i];
    total_tail += taillat_vec[i];
  }

  size_t thread_num = tid_vec.size();

  double avg_median = total_median / thread_num;
  double avg_tail = total_tail / thread_num;

  std::sort(medianlat_vec.begin(), medianlat_vec.end());
  std::sort(taillat_vec.begin(), taillat_vec.end());

  of_detail << total_attemp_tp << " " << total_tp << " " << medianlat_vec[0] << " " << medianlat_vec[thread_num - 1]
            << " " << avg_median << " " << taillat_vec[0] << " " << taillat_vec[thread_num - 1] << " " << avg_tail << std::endl;

  of << system_name << " " << total_attemp_tp / 1000 << " " << total_tp / 1000 << " " << avg_median << " " << avg_tail << std::endl;

  of_detail << std::endl;

  of.close();
  of_detail.close();

  // Open it when testing the duration
#if TEST_DURATION
  if (workload_name == "MICRO") {
    // print avg lock duration
    std::string file = "../../../bench_results/" + workload_name + "/avg_lock_duration.txt";
    of.open(file.c_str(), std::ios::app);

    double total_lock_dur = 0;
    for (int i = 0; i < lock_durations.size(); i++) {
      total_lock_dur += lock_durations[i];
    }

    of << system_name << " " << total_lock_dur / lock_durations.size() << std::endl;
    std::cerr << system_name << " avg_lock_dur: " << total_lock_dur / lock_durations.size() << std::endl;
  }
#endif
}