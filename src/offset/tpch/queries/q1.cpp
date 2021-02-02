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
#include <algorithm>
#include <deque>
#include <execution>
#include <iostream>
#include <math.h>

#include "offset/Database.hpp"
#include "offset/Types.hpp"
#include "offset/Utils.hpp"

using namespace std;
using vectorwise::primitives::Char_1;
using vectorwise::primitives::hash_t;

//  select
//    l_returnflag,
//    l_linestatus,
//    sum(l_quantity) as sum_qty,
//    sum(l_extendedprice) as sum_base_price,
//    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
//    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
//    avg(l_quantity) as avg_qty,
//    avg(l_extendedprice) as avg_price,
//    avg(l_discount) as avg_disc,
//    count(*) as count_order
//  from
//    lineitem
//  where
//    l_shipdate <= date '1998-12-01' - interval '90' day
//  group by
//    l_returnflag,
//    l_linestatus

std::unique_ptr<runtime::Query> q1_offset(offset::Database& db,
                                          size_t nrThreads) {
   using namespace types;
   using namespace std;
   types::Date c1 = types::Date::castString("1998-09-02");
   types::Numeric<12, 2> one = types::Numeric<12, 2>::castString("1.00");
   auto& li = db["lineitem"];
   auto l_returnflag = li["l_returnflag"].data<types::Char<1>>();
   auto l_linestatus = li["l_linestatus"].data<types::Char<1>>();
   auto l_extendedprice = li["l_extendedprice"].data<types::Numeric<12, 2>>();
   auto l_discount = li["l_discount"].data<types::Numeric<12, 2>>();
   auto l_tax = li["l_tax"].data<types::Numeric<12, 2>>();
   auto l_quantity = li["l_quantity"].data<types::Numeric<12, 2>>();
   auto l_shipdate = li["l_shipdate"].data<types::Date>();

   auto l_shipdate_ri = li.getRowIndex("l_shipdate");
   auto l_returnflag_ri = li.getRowIndex("l_returnflag");
   auto l_linestatus_ri = li.getRowIndex("l_linestatus");

   unsigned l_shipdate_size = *(&l_shipdate + 1) - l_shipdate;
   unsigned l_returnflag_size = *(&l_returnflag + 1) - l_returnflag;
   unsigned l_linestatus_size = *(&l_linestatus + 1) - l_linestatus;

   auto resources = initQuery(nrThreads);

   using hash = runtime::CRC32Hash;
   typedef tbb::concurrent_hash_map<std::tuple<types::Char<1>,types::Char<1>>,bool> HashTable;

   tbb::blocked_range3d<unsigned, unsigned, unsigned> range(
       0, l_shipdate_size, 4, 0, l_returnflag_size, 4, 0, l_linestatus_size, 4);

   tbb::parallel_for(
       range,
       [&](const tbb::blocked_range3d<size_t, size_t, size_t>& r) {
          size_t is = r.pages().begin();
          size_t ie = r.pages().end();
          size_t js = r.rows().begin();
          size_t je = r.rows().end();
          size_t ks = r.cols().end();
          size_t ke = r.cols().end();
          // start l_shipdate's interval lower limit at the upper limit of the
          // previous unique value
          auto il = is != 0 ? reinterpret_cast<offset::types::Date*>(
                                  l_shipdate + (is - 1))
                                  ->offset
                            : (unsigned)0;
          // start l_returnflag's interval lower limit at the upper limit of the
          // previous unique value
          auto jl = js != 0 ? reinterpret_cast<offset::types::Char<1>*>(
                                  l_returnflag + (js - 1))
                                  ->offset
                            : (unsigned)0;
          // start l_linestatus' interval lower limit at the upper limit of the
          // previous unique value
          auto kl = ks != 0 ? reinterpret_cast<offset::types::Char<1>*>(
                                  l_linestatus + (ks - 1))
                                  ->offset
                            : (unsigned)0;
          for (size_t i = is; i < ie; ++i) {
             if (l_shipdate[i] < c1) {
                auto iu = reinterpret_cast<offset::types::Date*>(l_shipdate + i)
                              ->offset;
                for (size_t j = js; j < je; ++j) {
                   auto ju = reinterpret_cast<offset::types::Char<1>*>(
                                 l_returnflag + j)
                                 ->offset;
                   std::vector<unsigned> intersection;
                   std::set_intersection(
                       std::execution::unseq, &(l_shipdate_ri[il]),
                       &(l_shipdate_ri[iu]), &(l_returnflag_ri[jl]),
                       &(l_returnflag_ri[ju]),
                       std::back_inserter(intersection));
                   if (!intersection.empty()) {
                      for (size_t k = ks; k < ke; ++k) {
                         auto ku = reinterpret_cast<offset::types::Char<1>*>(
                                       l_linestatus + k)
                                       ->offset;
                         auto match = offset::utils::avx2_find_one_match(
                             &(*intersection.begin()), &(*intersection.end()),
                             &(l_linestatus_ri[kl]), &(l_linestatus_ri[ku]));
                         if (!std::isnan(match)) {
                            
                         }
                      }
                   }
                }
             }
          }
       },
       tbb::simple_partitioner());
   auto& result = resources.query->result;
   auto retAttr = result->addAttribute("l_returnflag", sizeof(Char<1>));
   auto statusAttr = result->addAttribute("l_linestatus", sizeof(Char<1>));

   leaveQuery(nrThreads);
   return move(resources.query);
}