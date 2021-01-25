#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <iterator>
#include <sstream>
#include <thread>
#include <unordered_set>

#include "benchmarks/tpch/Queries.hpp"
#include "offset/Import.hpp"
#include "offset/Database.hpp"
#include "profile.hpp"
#include "tbb/tbb.h"

static void escape(void* p) { asm volatile("" : : "g"(p) : "memory"); }

size_t nrTuples(offset::Database& db, std::vector<std::string> tables) {
   size_t sum = 0;
   for (auto& table : tables) sum += db[table].nrTuples;
   return sum;
}

/// Clears Linux page cache.
/// This function only works on Linux.
void clearOsCaches() {
   if (system("sync; echo 3 > /proc/sys/vm/drop_caches")) {
      throw std::runtime_error("Could not flush system caches: " +
                               std::string(std::strerror(errno)));
   }
}

int main(int argc, char* argv[]) {
   if (argc <= 2) {
      std::cerr
          << "Usage: ./" << argv[0]
          << "<number of repetitions> <path to tpch dir> [nrThreads = all] \n "
             " EnvVars: [vectorSize = 1024] [SIMDhash = 0] [SIMDjoin = 0] "
             "[SIMDsel = 0]";
      exit(1);
   }

   PerfEvents e;
   offset::Database tpch;
   // load tpch data
   offset::tpch::import(argv[2], tpch);

   exit(0);

   // run queries
   auto repetitions = atoi(argv[1]);
   size_t nrThreads = std::thread::hardware_concurrency();
   // std::cout << "Nthread " << nrThreads << std::endl;
   size_t vectorSize = 1024;
   bool clearCaches = false;
   if (argc > 3) nrThreads = atoi(argv[3]);

   std::unordered_set<std::string> q = {
       "1h", "1v", "3h", "3hi", "3hi2", "3ha", "3hia", "3v",
       "5h", "5v", "6h", "6v",  "9h",   "9v",  "18h",  "18v"};

   if (auto v = std::getenv("vectorSize")) vectorSize = atoi(v);
   if (auto v = std::getenv("SIMDhash")) conf.useSimdHash = atoi(v);
   if (auto v = std::getenv("SIMDjoin")) conf.useSimdJoin = atoi(v);
   if (auto v = std::getenv("SIMDsel")) conf.useSimdSel = atoi(v);
   if (auto v = std::getenv("SIMDproj")) conf.useSimdProj = atoi(v);
   if (auto v = std::getenv("clearCaches")) clearCaches = atoi(v);
   if (auto v = std::getenv("q")) {
      using namespace std;
      istringstream iss((string(v)));
      q.clear();
      copy(istream_iterator<string>(iss), istream_iterator<string>(),
           insert_iterator<decltype(q)>(q, q.begin()));
   }

   tbb::task_scheduler_init scheduler(nrThreads);
#if 0
   {
      auto result = q3_hyper(tpch, nrThreads);
      escape(&result);
   }
   {
      auto result = q3_hyper_index(tpch, nrThreads);
      escape(&result);
   }
#endif
#if 0
   if (q.count("1h"))
      e.timeAndProfile("q1 hyper     ", nrTuples(tpch, {"lineitem"}),
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q1_hyper(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);

   if (q.count("1v"))
      e.timeAndProfile("q1 vectorwise", nrTuples(tpch, {"lineitem"}),
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result =
                              q1_vectorwise(tpch, nrThreads, vectorSize);
                          escape(&result);
                       },
                       repetitions);
#endif
#if 0

   if (q.count("5h"))
      e.timeAndProfile("q5 hyper", 1000000,
                       /*   nrTuples(tpch, {"supplier", "region", "nation",
                                          "customer", "orders", "lineitem"}),*/
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q5_hyper(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);
   /* if (q.count("5h"))
       e.timeAndProfile("q5 hyper index (no sel)   ",
                        nrTuples(tpch, {"supplier", "region", "nation",
                                        "customer", "orders", "lineitem"}),
                        [&]() {
                           if (clearCaches) clearOsCaches();
                           auto result = q5_no_sel_hyper_i(tpch, nrThreads);
                           escape(&result);
                        },
                        repetitions);*/
   if (q.count("5h"))
      e.timeAndProfile("q5 hyper index", 1000000 /*
                         nrTuples(tpch, {"supplier", "region", "nation",
                                         "customer", "orders", "lineitem"}),*/
                       ,
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q5_hyper_i1(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);

   if (q.count("5h"))
      e.timeAndProfile("q5 asia", 1000000,
                       /*   nrTuples(tpch, {"supplier", "region", "nation",
                                          "customer", "orders", "lineitem"}),*/
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q5_hyper_asia(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);

   if (q.count("5h"))
      e.timeAndProfile("5h asia index", 1000000,
                       /*   nrTuples(tpch, {"supplier", "region", "nation",
                                          "customer", "orders", "lineitem"}),*/
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q5_hyper_asia_i(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);

   if (q.count("5h"))
      e.timeAndProfile("q5 all", 1000000,
                       /*   nrTuples(tpch, {"supplier", "region", "nation",
                                          "customer", "orders", "lineitem"}),*/
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q5_hyper_all(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);

   if (q.count("5h"))
      e.timeAndProfile("5h all index", 1000000,
                       /*   nrTuples(tpch, {"supplier", "region", "nation",
                                          "customer", "orders", "lineitem"}),*/
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q5_hyper_all_i(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);

      /*if (q.count("3h"))
         e.timeAndProfile("q3 hyper     ",
                          nrTuples(tpch, {"customer", "orders", "lineitem"}),
                          [&]() {
                             if (clearCaches) clearOsCaches();
                             auto result = q3_hyper(tpch, nrThreads);
                             escape(&result);
                          },
                          repetitions);
      if (q.count("3hi"))
         e.timeAndProfile("q3 hyper index",
                          nrTuples(tpch, {"customer", "orders", "lineitem"}),
                          [&]() {
                             if (clearCaches) clearOsCaches();
                             auto result = q3_hyper_index(tpch, nrThreads);
                             escape(&result);
                          },
                          repetitions);*/
#endif
#if 1

   if (q.count("3h"))
      e.timeAndProfile("q3 hyper",
                       nrTuples(tpch, {"customer", "orders", "lineitem"}),
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q3_hyper(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);

   if (q.count("3hi"))
      e.timeAndProfile("q3 hyper index",
                       nrTuples(tpch, {"customer", "orders", "lineitem"}),
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q3_hyper_index(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);

   if (q.count("3ha"))
      e.timeAndProfile("q3 hyper all   ",
                       nrTuples(tpch, {"customer", "orders", "lineitem"}),
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q3_hyper_a(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);

   if (q.count("3hia"))
      e.timeAndProfile("q3 hyper all index",
                       nrTuples(tpch, {"customer", "orders", "lineitem"}),
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q3_hyper_ia(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);

   if (q.count("3hi2"))
      e.timeAndProfile("q3 hyper index 2",
                       nrTuples(tpch, {"customer", "orders", "lineitem"}),
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q3_hyper_index2(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);

   if (q.count("3v"))
      e.timeAndProfile(
          "q3 vectorwise", nrTuples(tpch, {"customer", "orders", "lineitem"}),
          [&]() {
             if (clearCaches) clearOsCaches();
             auto result = q3_vectorwise(tpch, nrThreads, vectorSize);
             escape(&result);
          },
          repetitions);
   if (q.count("5h"))
      e.timeAndProfile("q5 hyper     ",
                       nrTuples(tpch, {"supplier", "region", "nation",
                                       "customer", "orders", "lineitem"}),
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q5_hyper(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);
   if (q.count("5v"))
      e.timeAndProfile("q5 vectorwise",
                       nrTuples(tpch, {"supplier", "region", "nation",
                                       "customer", "orders", "lineitem"}),
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result =
                              q5_vectorwise(tpch, nrThreads, vectorSize);
                          escape(&result);
                       },
                       repetitions);
   if (q.count("6h"))
      e.timeAndProfile("q6 hyper     ", tpch["lineitem"].nrTuples,
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q6_hyper(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);
   if (q.count("6v"))
      e.timeAndProfile("q6 vectorwise", tpch["lineitem"].nrTuples,
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result =
                              q6_vectorwise(tpch, nrThreads, vectorSize);
                          escape(&result);
                       },
                       repetitions);
   if (q.count("9h"))
      e.timeAndProfile("q9 hyper     ",
                       nrTuples(tpch, {"nation", "supplier", "part", "partsupp",
                                       "lineitem", "orders"}),
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result = q9_hyper(tpch, nrThreads);
                          escape(&result);
                       },
                       repetitions);
   if (q.count("9v"))
      e.timeAndProfile("q9 vectorwise",
                       nrTuples(tpch, {"nation", "supplier", "part", "partsupp",
                                       "lineitem", "orders"}),
                       [&]() {
                          if (clearCaches) clearOsCaches();
                          auto result =
                              q9_vectorwise(tpch, nrThreads, vectorSize);
                          escape(&result);
                       },
                       repetitions);
   if (q.count("18h"))
      e.timeAndProfile(
          "q18 hyper     ",
          nrTuples(tpch, {"customer", "lineitem", "orders", "lineitem"}),
          [&]() {
             if (clearCaches) clearOsCaches();
             auto result = q18_hyper(tpch, nrThreads);
             escape(&result);
          },
          repetitions);
   if (q.count("18v"))
      e.timeAndProfile(
          "q18 vectorwise",
          nrTuples(tpch, {"customer", "lineitem", "orders", "lineitem"}),
          [&]() {
             if (clearCaches) clearOsCaches();
             auto result = q18_vectorwise(tpch, nrThreads, vectorSize);
             escape(&result);
          },
          repetitions);
#endif
   scheduler.terminate();
   return 0;
}