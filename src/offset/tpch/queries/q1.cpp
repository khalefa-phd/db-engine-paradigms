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
#include <iostream>

#include "offset/Database.hpp"
#include "offset/Types.hpp"

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

template <class T>
inline T& find(T* col, size_t colSize, offset::RowIndex colRowIndexes,
               unsigned indexToFind) {
   unsigned start = 0, end;
   T& uniqueValue = null;
   for (unsigned i = 0; i < colSize; ++i) {
      uniqueValue = col[i];
      end = uniqueValue.offset;
      if (colRowIndexes[end] <= indexToFind) {
         auto searchStart = &colRowIndexes[start];
         auto searchEnd = &colRowIndexes[end];
         auto found = std::binary_search(searchStart, searchEnd, indexToFind);
         if (found) { break; }
      }
      start = end + 1;
   }
   return uniqueValue;
}

NOVECTORIZE std::unique_ptr<runtime::Query> q1_offset(offset::Database& db,
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

   auto resources = initQuery(nrThreads);

   using hash = runtime::CRC32Hash;

   auto groupOp = make_GroupBy<tuple<Char<1>, Char<1>>,
                               tuple<Numeric<12, 2>, Numeric<12, 2>,
                                     Numeric<12, 4>, Numeric<12, 6>, int64_t>,
                               hash>(
       [](auto& acc, auto&& value) {
          get<0>(acc) += get<0>(value);
          get<1>(acc) += get<1>(value);
          get<2>(acc) += get<2>(value);
          get<3>(acc) += get<3>(value);
          get<4>(acc) += get<4>(value);
       },
       make_tuple(Numeric<12, 2>(), Numeric<12, 2>(), Numeric<12, 4>(),
                  Numeric<12, 6>(), int64_t(0)),
       nrThreads);

   tbb::parallel_for(
       tbb::blocked_range<size_t>(0, li.nrTuples, morselSize),
       [&](const tbb::blocked_range<size_t>& r) {
          auto locals = groupOp.preAggLocals();
          for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
             if (l_shipdate[i] <= c1) {
                auto& group = locals.getGroup(
                    make_tuple(l_returnflag[i], l_linestatus[i]));

                get<0>(group) += l_quantity[i];
                get<1>(group) += l_extendedprice[i];
                auto disc_price = l_extendedprice[i] * (one - l_discount[i]);
                get<2>(group) += disc_price;
                auto charge = disc_price * (one + l_tax[i]);
                get<3>(group) += charge;
                get<4>(group) += 1;
             }
          }
       });

   auto& result = resources.query->result;
   auto retAttr = result->addAttribute("l_returnflag", sizeof(Char<1>));
   auto statusAttr = result->addAttribute("l_linestatus", sizeof(Char<1>));
   auto qtyAttr = result->addAttribute("sum_qty", sizeof(Numeric<12, 2>));
   auto base_priceAttr =
       result->addAttribute("sum_base_price", sizeof(Numeric<12, 2>));
   auto disc_priceAttr =
       result->addAttribute("sum_disc_price", sizeof(Numeric<12, 2>));
   auto chargeAttr = result->addAttribute("sum_charge", sizeof(Numeric<12, 2>));
   auto count_orderAttr = result->addAttribute("count_order", sizeof(int64_t));

   groupOp.forallGroups(
       [&](runtime::Stack<decltype(groupOp)::group_t>& /*auto&*/ entries) {
          auto n = entries.size();
          auto block = result->createBlock(n);
          auto ret = reinterpret_cast<Char<1>*>(block.data(retAttr));
          auto status = reinterpret_cast<Char<1>*>(block.data(statusAttr));
          auto qty = reinterpret_cast<Numeric<12, 2>*>(block.data(qtyAttr));
          auto base_price =
              reinterpret_cast<Numeric<12, 2>*>(block.data(base_priceAttr));
          auto disc_price =
              reinterpret_cast<Numeric<12, 4>*>(block.data(disc_priceAttr));
          auto charge =
              reinterpret_cast<Numeric<12, 6>*>(block.data(chargeAttr));
          auto count_order =
              reinterpret_cast<int64_t*>(block.data(count_orderAttr));
          for (auto block : entries)
             for (auto& entry : block) {
                *ret++ = get<0>(entry.k);
                *status++ = get<1>(entry.k);
                *qty++ = get<0>(entry.v);
                *base_price++ = get<1>(entry.v);
                *disc_price++ = get<2>(entry.v);
                *charge++ = get<3>(entry.v);
                *count_order++ = get<4>(entry.v);
             }
          block.addedElements(n);
       });

   leaveQuery(nrThreads);
   return move(resources.query);
}