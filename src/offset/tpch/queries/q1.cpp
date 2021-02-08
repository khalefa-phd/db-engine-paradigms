#include "benchmarks/tpch/Queries.hpp"
#include "common/runtime/Hash.hpp"
#include "common/runtime/Types.hpp"
#include "hyper/GroupBy.hpp"
#include "hyper/ParallelHelper.hpp"
#include "tbb/enumerable_thread_specific.h"
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
#include "offset/tpch/Queries.hpp"

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

using GroupKey = std::tuple<types::Char<1>, types::Char<1>>;
// Tuple has begin and end position of the intersection
// between the row indexes of the elements in the group's key
using GroupRowIndexes = std::tuple<unsigned*, unsigned*>;
using Group = std::tuple<GroupKey, GroupRowIndexes>;
using vector_t = std::vector<Group>;
using Groups = tbb::enumerable_thread_specific<vector_t>;

template <typename OffsetType>
inline size_t get_lower_limit(size_t position, auto uniqueValues) {
   return reinterpret_cast<OffsetType*>(uniqueValues + (position - 1))
       ->offset;
}

template <typename OffsetType>
inline size_t get_upper_limit(size_t position, auto uniqueValues) {
   return reinterpret_cast<OffsetType*>(uniqueValues + position)->offset;
}

template <typename Type, typename ResultType, typename OffsetType>
inline ResultType sum(Type* col, size_t colSize, offset::RowIndex colRowIndex,
                      GroupRowIndexes groupRowIndexes) {
   auto range = tbb::blocked_range<size_t>(0, colSize, 64);
   return tbb::parallel_reduce(
       range, ResultType(),
       [&](const tbb::blocked_range<size_t>& r, ResultType init) -> ResultType {
          size_t is = r.begin();
          size_t ie = r.end();
          auto il = is != 0 ? get_lower_limit<OffsetType>(is, col) : size_t(0);
          for (size_t i = is; i < ie; ++i) {
             auto iu = get_upper_limit<OffsetType>(i, col);
             auto ocurrences = offset::utils::avx2_count_matches(
                 get<0>(groupRowIndexes), get<1>(groupRowIndexes),
                 &(colRowIndex[il]), &(colRowIndex[iu]));
             init += Type(ocurrences) * col[i];
             il = iu;
          }
          return init;
       },
       [](ResultType a, ResultType b) -> ResultType { return a + b; });
}

namespace rtypes = types;

namespace offset {
namespace tpch {
namespace queries {

std::unique_ptr<runtime::Query> q1_offset(offset::Database& db,
                                          size_t nrThreads) {
   using namespace types;
   using namespace std;
   rtypes::Date c1 = rtypes::Date::castString("1998-09-02");
   rtypes::Numeric<12, 2> one = rtypes::Numeric<12, 2>::castString("1.00");
   auto& li = db["lineitem"];
   auto l_returnflag = li["l_returnflag"].data<rtypes::Char<1>>();
   auto l_linestatus = li["l_linestatus"].data<rtypes::Char<1>>();
   auto l_extendedprice = li["l_extendedprice"].data<rtypes::Numeric<12, 2>>();
   auto l_discount = li["l_discount"].data<rtypes::Numeric<12, 2>>();
   auto l_tax = li["l_tax"].data<rtypes::Numeric<12, 2>>();
   auto l_quantity = li["l_quantity"].data<rtypes::Numeric<12, 2>>();
   auto l_shipdate = li["l_shipdate"].data<rtypes::Date>();

   auto& l_shipdate_ri = li.getRowIndex("l_shipdate");
   auto& l_returnflag_ri = li.getRowIndex("l_returnflag");
   auto& l_linestatus_ri = li.getRowIndex("l_linestatus");
   auto& l_quantity_ri = li.getRowIndex("l_quantity");
   auto& l_extendedprice_ri = li.getRowIndex("l_extendedprice");

   unsigned l_shipdate_size = *(&l_shipdate + 1) - l_shipdate;
   unsigned l_returnflag_size = *(&l_returnflag + 1) - l_returnflag;
   unsigned l_linestatus_size = *(&l_linestatus + 1) - l_linestatus;
   unsigned l_extendedprice_size = *(&l_extendedprice + 1) - l_extendedprice;
   unsigned l_quantity_size = *(&l_quantity + 1) - l_quantity;
   unsigned l_tax_size = *(&l_tax + 1) - l_tax;
   unsigned l_discount_size = *(&l_discount + 1) - l_discount;

   auto resources = initQuery(nrThreads);

   Groups groups;

   tbb::blocked_range3d<size_t, size_t, size_t> range(
       0, l_shipdate_size, 4, 0, l_returnflag_size, 4, 0, l_linestatus_size, 4);

   // I can use reduce to emit tuples of groups and the resulting intersection
   // Threads will only produce unique groups
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
          auto il = is != 0 ? get_lower_limit<offset::types::Date>(is, l_shipdate)
                            : size_t(0);
          // start l_returnflag's interval lower limit at the upper limit of the
          // previous unique value
          auto jl = js != 0 ? get_lower_limit<offset::types::Char<1>>(js, l_returnflag)
                            : size_t(0);
          // start l_linestatus' interval lower limit at the upper limit of the
          // previous unique value
          auto kl = ks != 0 ? get_lower_limit<offset::types::Char<1>>(ks, l_linestatus)
                            : size_t(0);
          for (size_t i = is; i < ie; ++i) {
             auto iu = get_upper_limit<offset::types::Date>(i, l_shipdate);
             if (l_shipdate[i] < c1) {
                for (size_t j = js; j < je; ++j) {
                   auto ju = get_upper_limit<offset::types::Char<1>>(j, l_returnflag);
                   std::vector<unsigned> intersection;
                   std::set_intersection(
                       std::execution::unseq, &(l_shipdate_ri[il]),
                       &(l_shipdate_ri[iu]), &(l_returnflag_ri[jl]),
                       &(l_returnflag_ri[ju]),
                       std::back_inserter(intersection));
                   if (!intersection.empty()) {
                      for (size_t k = ks; k < ke; ++k) {
                         auto ku =
                             get_upper_limit<offset::types::Char<1>>(k, l_linestatus);
                         auto groupRowIndexesBegin = &(*intersection.begin());
                         auto groupRowIndexesEnd =
                             offset::utils::avx2_inplace_set_intersection(
                                 &(*intersection.begin()),
                                 &(*intersection.end()), &(l_linestatus_ri[kl]),
                                 &(l_linestatus_ri[ku]));
                         if (groupRowIndexesEnd != groupRowIndexesBegin) {
                            Groups::reference group = groups.local();
                            group.emplace_back(make_tuple(
                                make_tuple(l_returnflag[j], l_linestatus[k]),
                                make_tuple(groupRowIndexesBegin,
                                           groupRowIndexesEnd)));
                         }
                         kl = ku;
                      }
                   }
                   jl = ju;
                }
             }
             il = iu;
          }
       },
       tbb::simple_partitioner());

   auto& result = resources.query->result;
   auto retAttr = result->addAttribute("l_returnflag", sizeof(rtypes::Char<1>));
   auto statusAttr = result->addAttribute("l_linestatus", sizeof(rtypes::Char<1>));
   auto qtyAttr = result->addAttribute("sum_qty", sizeof(rtypes::Numeric<12, 4>));
   auto base_priceAttr =
       result->addAttribute("sum_base_price", sizeof(rtypes::Numeric<12, 4>));
   auto disc_priceAttr =
       result->addAttribute("sum_disc_price", sizeof(rtypes::Numeric<12, 2>));
   auto chargeAttr = result->addAttribute("sum_charge", sizeof(rtypes::Numeric<12, 2>));
   auto count_orderAttr = result->addAttribute("count_order", sizeof(int64_t));

   tbb::parallel_for(groups.range(), [&](Groups::range_type const& r) {
      for (auto& groups : r) {
         auto groupsToProcess = groups.size();

         auto block = result->createBlock(groupsToProcess);
         auto ret = reinterpret_cast<rtypes::Char<1>*>(block.data(retAttr));
         auto status = reinterpret_cast<rtypes::Char<1>*>(block.data(statusAttr));
         auto qty = reinterpret_cast<rtypes::Numeric<12, 4>*>(block.data(qtyAttr));
         auto base_price =
             reinterpret_cast<rtypes::Numeric<12, 4>*>(block.data(base_priceAttr));
         auto disc_price =
             reinterpret_cast<rtypes::Numeric<12, 4>*>(block.data(disc_priceAttr));
         auto charge =
             reinterpret_cast<rtypes::Numeric<12, 6>*>(block.data(chargeAttr));
         auto count_order =
             reinterpret_cast<int64_t*>(block.data(count_orderAttr));
         for (auto& group : groups) {
            auto& key = get<0>(group);
            *ret++ = get<0>(key);
            *status++ = get<1>(key);

            auto& groupRowIndexes = get<1>(group);

            *qty++ = sum<rtypes::Numeric<12, 2>, rtypes::Numeric<12, 4>, offset::types::Numeric<12,2>>(
                l_quantity, l_quantity_size, l_quantity_ri, groupRowIndexes);

            *base_price++ = sum<rtypes::Numeric<12, 2>, rtypes::Numeric<12, 4>, offset::types::Numeric<12,2>>(
                l_extendedprice, l_extendedprice_size, l_extendedprice_ri,
                groupRowIndexes);
         }
      }
   });

   leaveQuery(nrThreads);
   return move(resources.query);
}
} // namespace queries
} // namespace tpch
} // namespace offset