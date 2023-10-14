/**
 * grading_clock_replacer_test.cpp, which is a WRONG filename on gradescope
 * so I rename it to grading_lru_replacer_test.cpp
 *
 * TEST in project #1 on gradescope (40.0/160.0):
 * test_ClockReplacer_SampleTest (__main__.TestProject1) (5.0/5.0)
 * test_ClockReplacer_Victim (__main__.TestProject1) (5.0/5.0)
 * test_ClockReplacer_Pin (__main__.TestProject1) (5.0/5.0)
 * test_ClockReplacer_Size (__main__.TestProject1) (5.0/5.0)
 * test_ClockReplacer_ConcurrencyTest (__main__.TestProject1) (10.0/10.0)
 * test_ClockReplacer_IntegratedTest (__main__.TestProject1) (10.0/10.0)
 */

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "replacer/lru_replacer.h"
#include "gtest/gtest.h"

TEST(LRUReplacerTest, SampleTest) {
  LRUReplacer lru_replacer(10);
  //0pined 1pined 2pined 3pined 4pined 5pined 6pined 7pined 8pined 9pined 
  //
  
  lru_replacer.unpin(1);
  lru_replacer.unpin(2);
  lru_replacer.unpin(3);
  lru_replacer.unpin(5);
  lru_replacer.unpin(6);
  lru_replacer.unpin(8);
  lru_replacer.unpin(9);
  // 0pined 1解锁 2解锁 3解锁 4pined 5解锁 6解锁 7pined 8解锁 9解锁
  // 1 2 3 5 6 8 9  
  EXPECT_EQ(7, lru_replacer.Size());

  int value;
  lru_replacer.victim(&value);
  EXPECT_EQ(1, value);
  lru_replacer.victim(&value);
  EXPECT_EQ(2, value);
  lru_replacer.victim(&value);
  EXPECT_EQ(3, value);
  lru_replacer.victim(&value);
  EXPECT_EQ(5, value);
  lru_replacer.victim(&value);
  EXPECT_EQ(6, value);
  // 0pined 1替换 2替换 3替换 4pined 5替换 6替换 7pined 8pined 9解锁
  // 9  

  lru_replacer.pin(5);
  lru_replacer.pin(6);
  lru_replacer.pin(7);
  lru_replacer.pin(8);
  EXPECT_EQ(1, lru_replacer.Size());

  lru_replacer.unpin(4);
  lru_replacer.unpin(0);
  // 0pined 1替换 2替换 3替换 4解锁 5替换 6替换 7pined 8pined 9解锁
  // 9 4 10
  EXPECT_EQ(3, lru_replacer.Size());

  lru_replacer.victim(&value);
  EXPECT_EQ(9, value);
  lru_replacer.victim(&value);
  EXPECT_EQ(4, value);
  lru_replacer.victim(&value);
  EXPECT_EQ(0, value);
  EXPECT_EQ(false, lru_replacer.victim(&value));
}

TEST(LRUReplacerTest, victim) {
  auto lru_replacer = new LRUReplacer(710); // should bigger than 710
  //0pined 1pined 2pined ... 709pined
  // 

  // Empty and try removing
  int result;
  EXPECT_EQ(0, lru_replacer->victim(&result)) << "Check your return value behavior for LRUReplacer::victim after init LRUReplacer,it should equal 0";


  // unpin one and remove
  lru_replacer->unpin(7);
  //0pined 1pined ... 6pined 7解锁 8pined ... 709pined
  //7
  EXPECT_EQ(1, lru_replacer->victim(&result)) << "Check your return value behavior for LRUReplacer::victim return value,afer unpin one pined frame,it should add 1";
  //0pined 1pined ... 6pined 7替换 8pined ... 709pined
  //
  EXPECT_EQ(7, result);

  // unpin, remove and verify
  lru_replacer->unpin(1);
  lru_replacer->unpin(3);
  lru_replacer->unpin(4);
  //0pined 1解锁 2pined 3解锁 4解锁 5pined 6pined 7替换 8pined ... 709pined
  //1 3 4
  EXPECT_EQ(1, lru_replacer->victim(&result))<< "Check your return value behavior for LRUReplacer::victim";
  //0pined 1替换 2pined 3解锁 4解锁 5pined 6pined 7替换 8pined ... 709pined
  //3 4
  EXPECT_EQ(1, result);
  lru_replacer->unpin(1);
  lru_replacer->unpin(3);
  lru_replacer->unpin(4);
  //0pined 1解锁 2pined 3解锁 4解锁 5pined 6pined 7替换 8pined ... 709pined
  //3 4 1
  lru_replacer->unpin(9);
  lru_replacer->unpin(8);
  //0pined 1解锁 2pined 3解锁 4解锁 5pined 6pined 7替换 8解锁 9解锁 10pined... 709pined
  //3 4 1 9 8
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(3, result);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(4, result);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(1, result);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(9, result);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(8, result);
  EXPECT_EQ(0, lru_replacer->victim(&result)) << "Check your return value behavior for LRUReplacer::victim after pined some frame and other frames have been ouputted by victim";
  //0pined 1替换 2pined 3替换 4替换 5pined 6pined 7替换 8替换 9替换 10pined ...
  //
  lru_replacer->unpin(13);
  lru_replacer->unpin(14);
  lru_replacer->unpin(10);
  lru_replacer->unpin(11);
  lru_replacer->unpin(12);
  // ... 10解锁 11解锁 12解锁 13解锁 14解锁 ...
  //13 14 10 11 12
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(13, result);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(14, result);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(10, result);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(11, result);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(12, result);
  EXPECT_EQ(0, lru_replacer->victim(&result)) << "Check your return value behavior for LRUReplacer::victim =="+std::to_string(result);
  lru_replacer->unpin(71);
  lru_replacer->unpin(71);
  // ... 71解锁 ...
  //71
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(71, result);
  EXPECT_EQ(0, lru_replacer->victim(&result));
  EXPECT_EQ(0, lru_replacer->victim(&result));
  EXPECT_EQ(0, lru_replacer->victim(&result));

  for (int i = 0; i < 710; i++) {
    if(i%2==0){
      lru_replacer->unpin(i);
    }
  }
  for (int i = 10; i < 710; i++) {
    if(i%2==0){
      EXPECT_EQ(1, lru_replacer->victim(&result));
      EXPECT_EQ(i - 10, result);
    }
  }
  EXPECT_EQ(5, lru_replacer->Size());

  delete lru_replacer;
}

TEST(LRUReplacerTest, pin) {
  auto lru_replacer = new LRUReplacer(710);//should >= 710

  // Empty and try removing
  int result;
  lru_replacer->pin(0);
  lru_replacer->pin(1);

  // unpin one and remove
  lru_replacer->unpin(71);
  //list:71
  lru_replacer->pin(71);
  //list:
  lru_replacer->pin(71);
  EXPECT_EQ(false, lru_replacer->victim(&result));
  lru_replacer->pin(57);
  EXPECT_EQ(false, lru_replacer->victim(&result));

  // unpin, remove and verify
  lru_replacer->unpin(401);
  lru_replacer->unpin(401);
  lru_replacer->pin(401);
  EXPECT_EQ(false, lru_replacer->victim(&result));
  lru_replacer->unpin(33);
  lru_replacer->unpin(34);
  lru_replacer->unpin(31);
  lru_replacer->unpin(33);
  lru_replacer->unpin(34);
  lru_replacer->unpin(310);
  lru_replacer->pin(33);
  //list:34 31 310
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(34, result);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(31, result);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(310, result);
  //list:
  EXPECT_EQ(0, lru_replacer->victim(&result));

  lru_replacer->unpin(45);
  lru_replacer->unpin(46);
  lru_replacer->unpin(47);
  lru_replacer->unpin(48);
  lru_replacer->unpin(46);
  lru_replacer->pin(47);
  lru_replacer->pin(45);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(46, result);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(48, result);
  EXPECT_EQ(false, lru_replacer->victim(&result));
  lru_replacer->unpin(610);
  lru_replacer->unpin(610);
  lru_replacer->unpin(611);
  lru_replacer->unpin(611);
  lru_replacer->unpin(612);
  lru_replacer->unpin(612);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(610, result);
  lru_replacer->pin(611);
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(612, result);
  EXPECT_EQ(0, lru_replacer->victim(&result));

  for (int i = 0; i <= 700; i++) {
    lru_replacer->unpin(i);
    //list 0 ... 700
  }
  int j = 0;
  for (int i = 100; i < 700; i += 2) {
    lru_replacer->pin(i);
    // list 0 1 2 .. 99 101 103 .. 699 
    EXPECT_EQ(true, lru_replacer->victim(&result));
    if (j <= 99) {
      EXPECT_EQ(j, result);
      j++;
    } else {
      EXPECT_EQ(j + 1, result);
      j += 2;
    }
  }
  lru_replacer->pin(result);

  delete lru_replacer;
}

TEST(LRUReplacerTest, Size) {
  auto lru_replacer = new LRUReplacer(710);

  EXPECT_EQ(0, lru_replacer->Size());
  lru_replacer->unpin(1);
  EXPECT_EQ(1, lru_replacer->Size());
  lru_replacer->unpin(2);
  EXPECT_EQ(2, lru_replacer->Size());
  lru_replacer->unpin(3);
  EXPECT_EQ(3, lru_replacer->Size());
  lru_replacer->unpin(3);
  EXPECT_EQ(3, lru_replacer->Size());
  lru_replacer->unpin(5);
  EXPECT_EQ(4, lru_replacer->Size());
  lru_replacer->unpin(6);
  EXPECT_EQ(5, lru_replacer->Size());
  lru_replacer->unpin(1);
  EXPECT_EQ(5, lru_replacer->Size());
  lru_replacer->unpin(71);
  EXPECT_EQ(6, lru_replacer->Size());
  lru_replacer->unpin(671);
  EXPECT_EQ(7, lru_replacer->Size());

  // pop element from replacer
  int result;
  for (int i = 7; i >= 1; i--) {
    lru_replacer->victim(&result);
    EXPECT_EQ(i - 1, lru_replacer->Size());
  }
  EXPECT_EQ(0, lru_replacer->Size());

  for (int i = 0; i < 710; i++) {
    lru_replacer->unpin(i);
    EXPECT_EQ(i + 1, lru_replacer->Size());
  }
  for (int i = 0; i < 710; i += 2) {
    lru_replacer->pin(i);
    EXPECT_EQ(709 - (i / 2), lru_replacer->Size());
  }

  delete lru_replacer;
}

TEST(LRUReplacerTest, ConcurrencyTest) {
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    int value_size = 1000;
    std::shared_ptr<LRUReplacer> lru_replacer{new LRUReplacer(value_size)};
    std::vector<std::thread> threads;
    int result;
    std::vector<int> value(value_size);
    for (int i = 0; i < value_size; i++) {
      value[i] = i;
    }
    auto rng = std::default_random_engine{};
    std::shuffle(value.begin(), value.end(), rng);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([tid, &lru_replacer, &value]() {  // NOLINT
        int share = 1000 / 5;
        for (int i = 0; i < share; i++) {
          lru_replacer->unpin(value[tid * share + i]);
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }
    std::vector<int> out_values;
    for (int i = 0; i < value_size; i++) {
      EXPECT_EQ(1, lru_replacer->victim(&result));
      out_values.push_back(result);
    }
    std::sort(value.begin(), value.end());
    std::sort(out_values.begin(), out_values.end());
    EXPECT_EQ(value, out_values);
    EXPECT_EQ(0, lru_replacer->victim(&result));
  }
}

TEST(LRUReplacerTest, IntegratedTest) {
  int result;
  int value_size = 710;
  auto lru_replacer = new LRUReplacer(value_size);
  std::vector<int> value(value_size);
  for (int i = 0; i < value_size; i++) {
    value[i] = i;
  }
  auto rng = std::default_random_engine{};
  std::shuffle(value.begin(), value.end(), rng);

  for (int i = 0; i < value_size; i++) {
    lru_replacer->unpin(value[i]);
  }
  EXPECT_EQ(value_size, lru_replacer->Size());

  // pin and unpin 277
  lru_replacer->pin(277);
  lru_replacer->unpin(277);
  // pin and unpin 0
  EXPECT_EQ(1, lru_replacer->victim(&result));
  EXPECT_EQ(value[0], result);
  lru_replacer->unpin(value[0]);

  for (int i = 0; i < value_size / 2; i++) {
    if (value[i] != value[0] && value[i] != 277) {
      lru_replacer->pin(value[i]);
      lru_replacer->unpin(value[i]);
    }
  }

  std::vector<int> lru_array;
  for (int i = value_size / 2; i < value_size; ++i) {
    if (value[i] != value[0] && value[i] != 277) {
      lru_array.push_back(value[i]);
    }
  }
  lru_array.push_back(277);
  lru_array.push_back(value[0]);
  for (int i = 0; i < value_size / 2; ++i) {
    if (value[i] != value[0] && value[i] != 277) {
      lru_array.push_back(value[i]);
    }
  }
  EXPECT_EQ(value_size, lru_replacer->Size());

  for (int e : lru_array) {
    EXPECT_EQ(true, lru_replacer->victim(&result));
    EXPECT_EQ(e, result);
  }
  EXPECT_EQ(value_size - lru_array.size(), lru_replacer->Size());

  delete lru_replacer;
}
