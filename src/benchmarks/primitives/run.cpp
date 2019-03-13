#include "profile.hpp"
#include "vectorwise/Primitives.hpp"
#include "vectorwise/defs.hpp"
#include <algorithm>
#include <functional>
#include <iostream>
#include <iterator>
#include <random>
#include <vector>
#include "benchmarks/tpch/Queries.hpp"
#include "common/runtime/Hash.hpp"
#include "common/runtime/Types.hpp"
#include "hyper/GroupBy.hpp"
#include "hyper/ParallelHelper.hpp"
#include "tbb/tbb.h"
#include "vectorwise/Operations.hpp"
#include "vectorwise/Operators.hpp"
#include "vectorwise/Primitives.hpp"
#include "vectorwise/QueryBuilder.hpp"
#include "vectorwise/VectorAllocator.hpp"
#include <iostream>

using namespace runtime;
using namespace std;
using vectorwise::pos_t;
using namespace vectorwise::primitives;

template <typename T>
void putRandom(vector<T>& v, size_t min = 0, size_t max = 99) {
   mt19937 mersenne_engine(1337);
   uniform_int_distribution<T> dist(min, max);
   auto gen = std::bind(dist, mersenne_engine);
   generate(begin(v), end(v), gen);
}

template <typename T>
void putRandom(vector<pos_t>& sel, vector<T>& v, size_t min = 0,
               size_t max = 99) {
   mt19937 mersenne_engine(1337);
   uniform_int_distribution<T> dist(min, max);
   auto gen = std::bind(dist, mersenne_engine);
   for (auto pos : sel) v[pos] = gen();
}

#define EACH_T(M, args...) M(args, int8_t)

#define EACH_SEL(M, args...)                                                   \
   M(args, 0)                                                                  \
   M(args, 1)                                                                  \
   M(args, 5)                                                                  \
   M(args, 10) M(args, 30) M(args, 50) M(args, 65) M(args, 80) M(args, 100)

#define EACH_BF(M, args...) M(args, ) M(args, _bf)

#define BENCH_LESS(BF, TYPE, SEL)                                              \
   {                                                                           \
      vector<TYPE> input(n);                                                   \
      putRandom(input);                                                        \
      TYPE val = SEL;                                                          \
      vector<pos_t> output;                                                    \
      output.assign(n, 0);                                                     \
                                                                               \
      e.timeAndProfile("sel_lt" + string(#BF) + "\t" + string(#SEL) + "\t" +   \
                           string(#TYPE) + "\t",                               \
                       n,                                                      \
                       [&]() {                                                 \
                          sel_less_##TYPE##_col_##TYPE##_val##BF(              \
                              n, output.data(), input.data(), &val);           \
                       },                                                      \
                       repetitions);                                           \
   }

#define BENCH_LESS_SEL(BF, TYPE, SEL)                                          \
   {                                                                           \
      vector<pos_t> sel(n);                                                    \
      putRandom(sel, 0, vecSize);                                              \
      vector<TYPE> input(vecSize);                                             \
      putRandom(sel, input);                                                   \
      TYPE val = SEL;                                                          \
      vector<pos_t> output;                                                    \
      output.assign(n, 0);                                                     \
                                                                               \
      e.timeAndProfile("selsel_lt" + string(#BF) + "\t" + string(#SEL) +       \
                           "\t" + string(#TYPE) + "\t",                        \
                       n,                                                      \
                       [&]() {                                                 \
                          selsel_less_##TYPE##_col_##TYPE##_val##BF(           \
                              n, sel.data(), output.data(), input.data(),      \
                              &val);                                           \
                       },                                                      \
                       repetitions);                                           \
   }

NOVECTORIZE void test_ht() {
 using hash = runtime::CRC32Hash;

   Hashmapx<types::Integer, types::Integer, hash> ht2;
   runtime::Stack<decltype(ht2)::Entry> entries2;
   auto found2 = 0;
   const auto one = types::Integer(1);
   const auto two = types::Integer(2);
   const auto three = types::Integer(3	);
   entries2.emplace_back(ht2.hash(one), one, one);
cout << ht2.hash(one) <<endl;
   entries2.emplace_back(ht2.hash(one), one, two);
   entries2.emplace_back(ht2.hash(one), one, three);

   entries2.emplace_back(ht2.hash(two), two, one);
   entries2.emplace_back(ht2.hash(two), two, two);
   entries2.emplace_back(ht2.hash(three), three, three);
   ht2.setSize(6);
   ht2.insertAll(entries2);
   auto h = ht2.hash(one);
   auto entry = reinterpret_cast<decltype(ht2)::Entry*>(ht2.find_chain_tagged(h));

   for (; entry != ht2.end();
        entry = reinterpret_cast<decltype(ht2)::Entry*>(entry->h.next))
      if (entry->h.hash == h && entry->k == one) 
          cout << entry->v;
/*
auto x=ht2.findOneEntry(one,ht2.hash(one));
for(;x!=ht2.end();x=x->h.next)
cout << x->k <<" " <<x->v <<endl;
*/
}


int main() {
   /*size_t vecSize = 1024 * 8;
   size_t n = 1024 * 8;
   size_t repetitions = 100000;
   PerfEvents e;
   // run selection primitives with different selectivities
   EACH_BF(EACH_T, EACH_SEL, BENCH_LESS);
   n = 1024 * 4;
   EACH_BF(EACH_T, EACH_SEL, BENCH_LESS_SEL);*/
test_ht();
   return 0;
}
