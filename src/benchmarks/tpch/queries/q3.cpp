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
using vectorwise::primitives::Char_10;
using vectorwise::primitives::hash_t;

// select
//  l_orderkey,
//  sum(l_extendedprice * (1 - l_discount)) as revenue,
//  o_orderdate,
//  o_shippriority
// from
//  customer,
//  orders,
//  lineitem
// where
//  c_mktsegment = 'BUILDING'
//  and c_custkey = o_custkey
//  and l_orderkey = o_orderkey
//  and o_orderdate < date '1995-03-15'
//  and l_shipdate > date '1995-03-15'
// group by
//   l_orderkey,
//   o_orderdate,
//   o_shippriority
#define TWO_INDEX
#ifdef ONE_INDEX
NOVECTORIZE std::unique_ptr<runtime::Query>
q3_hyper_index(runtime::Database& db) {
   cout << "hi from q3_hyper index" << endl;
   // --- aggregates
   auto resources = initQuery(1);

   // --- constants
   auto c1 = types::Date::castString("1995-03-15");
   auto c2 = types::Date::castString("1995-03-15");
   string b = "BUILDING";
   auto c3 = types::Char<10>::castString(b.data(), b.size());

   auto& indx = db.getindex("cust_ord");
   auto& indx_val = db.getindex("cust_ord_vals");

   auto& cu = db["customer"];
   auto& ord = db["orders"];
   auto& li = db["lineitem"];
   // cout << "Customer \t" << cu.nrTuples << "Ord \t" << ord.nrTuples
   //      << "Lineitem \t" << li.nrTuples << endl;
   auto c_mktsegment = cu["c_mktsegment"].data<types::Char<10>>();
   auto c_custkey = cu["c_custkey"].data<types::Integer>();
   auto o_custkey = ord["o_custkey"].data<types::Integer>();
   auto o_orderkey = ord["o_orderkey"].data<types::Integer>();
   auto o_orderdate = ord["o_orderdate"].data<types::Date>();
   auto o_shippriority = ord["o_shippriority"].data<types::Integer>();
   auto l_orderkey = li["l_orderkey"].data<types::Integer>();
   auto l_shipdate = li["l_shipdate"].data<types::Date>();
   auto l_extendedprice = li["l_extendedprice"].data<types::Numeric<12, 2>>();
   auto l_discount = li["l_discount"].data<types::Numeric<12, 2>>();

   using hash = runtime::CRC32Hash;

   Hashmapx<types::Integer, std::tuple<types::Date, types::Integer>, hash> ht2;
   runtime::Stack<decltype(ht2)::Entry> entries2;
   auto found2 = 0;
#ifdef VERBOSE
   cout << "query" << endl;
#endif
   for (size_t i = 0, end = cu.nrTuples; i != end; ++i) {
      if (c_mktsegment[i] == c3) {
         // std::cout << "c\t" << i << " \t" << c_custkey[i] << endl;
         size_t b = 0;
         if (i > 0) b = indx[i - 1];
         size_t e = indx[i];
         // cout << b << " " << e << endl;
         for (size_t j = b; j < e; j++) {
            size_t m = indx_val[j];
            if (o_orderdate[m] < c1) {
               //   std::cout << "+\t" << c_custkey[i] << "\t" << o_orderkey[m]
               //           << "\t" << (c_custkey[i] - o_custkey[m]) << endl;
               //   std::cout << "\t\t" << m << "\t" << o_orderkey[m] << endl;
               entries2.emplace_back(
                   ht2.hash(o_orderkey[m]), o_orderkey[m],
                   make_tuple(o_orderdate[m], o_shippriority[m]));
               found2++;
            }
         }
      }
   }
   // std::cout << "count found " << found2 << endl;
   ht2.setSize(found2);
   ht2.insertAll(entries2);

   const auto one = types::Numeric<12, 2>::castString("1.00");
   const auto zero = types::Numeric<12, 4>::castString("0.00");

   Hashmapx<std::tuple<types::Integer, types::Date, types::Integer>,
            types::Numeric<12, 4>, hash, false>
       groups;

   auto groupOp =
       make_GroupBy<std::tuple<types::Integer, types::Date, types::Integer>,
                    types::Numeric<12, 4>, hash>(
           [](auto& acc, auto&& value) { acc += value; }, zero, 1);

   // preaggregation

   auto locals = groupOp.preAggLocals();

   for (size_t i = 0, end = li.nrTuples; i != end; ++i) {
      decltype(ht2)::value_type* v;
      if (l_shipdate[i] > c2 && (v = ht2.findOne(l_orderkey[i]))) {
         locals.consume(make_tuple(l_orderkey[i], get<0>(*v), get<1>(*v)),
                        l_extendedprice[i] * (one - l_discount[i]));
      }
   }
#ifdef VERBOSE
   // cout << "Preparing results" << endl;
#endif
   // --- output
   auto& result = resources.query->result;
   auto revAttr =
       result->addAttribute("revenue", sizeof(types::Numeric<12, 4>));
   auto orderAttr = result->addAttribute("l_orderkey", sizeof(types::Integer));
   auto dateAttr = result->addAttribute("o_orderdate", sizeof(types::Date));
   auto prioAttr =
       result->addAttribute("o_shippriority", sizeof(types::Integer));

   groupOp.forallGroups([&](auto& entries) {
      // write aggregates to result
      auto n = entries.size();
#ifdef VERBOSE
      cout << "Group size" << n << endl;
#endif
      auto block = result->createBlock(n);
      auto rev = reinterpret_cast<types::Numeric<12, 4>*>(block.data(revAttr));
      auto order = reinterpret_cast<types::Integer*>(block.data(orderAttr));
      auto date = reinterpret_cast<types::Date*>(block.data(dateAttr));
      auto prio = reinterpret_cast<types::Integer*>(block.data(prioAttr));
      for (auto block : entries)
         for (auto& entry : block) {
            *order++ = get<0>(entry.k);
            *date++ = get<1>(entry.k);
            *prio++ = get<2>(entry.k);
            *rev++ = entry.v;
         }
      block.addedElements(n);
   });
#ifdef VERBOSE
   cout << "done results" << endl;
#endif
   leaveQuery(1);
   return move(resources.query);
}
#endif

#ifdef TWO_INDEX
NOVECTORIZE std::unique_ptr<runtime::Query>
q3_hyper_index(runtime::Database& db) {
#ifdef VERBOSE
   cout << "hi from q3_hyper index" << endl;
#endif
   // --- aggregates
   auto resources = initQuery(1);

   // --- constants
   auto c1 = types::Date::castString("1995-03-15");
   auto c2 = types::Date::castString("1995-03-15");
   string b = "BUILDING";
   auto c3 = types::Char<10>::castString(b.data(), b.size());

   auto& indx = db.getindex("cust_ord");
   auto& indx_val = db.getindex("cust_ord_vals");

   auto& iord = db.getindex("ord_li");
   auto& iord_val = db.getindex("ord_li_vals");

   auto& cu = db["customer"];
   auto& ord = db["orders"];
   auto& li = db["lineitem"];

   auto c_mktsegment = cu["c_mktsegment"].data<types::Char<10>>();
   auto c_custkey = cu["c_custkey"].data<types::Integer>();
   auto o_custkey = ord["o_custkey"].data<types::Integer>();
   auto o_orderkey = ord["o_orderkey"].data<types::Integer>();
   auto o_orderdate = ord["o_orderdate"].data<types::Date>();
   auto o_shippriority = ord["o_shippriority"].data<types::Integer>();
   auto l_orderkey = li["l_orderkey"].data<types::Integer>();
   auto l_shipdate = li["l_shipdate"].data<types::Date>();
   auto l_extendedprice = li["l_extendedprice"].data<types::Numeric<12, 2>>();
   auto l_discount = li["l_discount"].data<types::Numeric<12, 2>>();

   std::vector<std::tuple<types::Integer, types::Date, types::Integer,
                          types::Numeric<12, 4>>>
       entries;
   const auto one = types::Numeric<12, 2>::castString("1.00");
   const auto zero = types::Numeric<12, 4>::castString("0.00");
#ifdef VERBOSE
   cout << "query" << endl;
#endif
   for (size_t i = 0, end = cu.nrTuples; i != end; ++i) {
      if (c_mktsegment[i] == c3) {
         size_t b = 0;
         if (i > 0) b = indx[i - 1];
         size_t e = indx[i];

         for (size_t j = b; j < e; j++) {
            size_t m = indx_val[j];
            if (o_orderdate[m] < c1) {
               // do not add it to the hash table
               // we can compute the value now
               size_t order_begin = 0;
               size_t order_end = 0;
               if (j > 0) order_begin = iord[j - 1];
               order_end = iord[j];
               types::Numeric<12, 4> agg = 0;
               for (size_t k = order_begin; j < order_end; j++) {
                  auto li_indx = iord_val[k];
                  if (l_shipdate[li_indx] > c2) {
                     agg += (l_extendedprice[li_indx] *
                             (one - l_discount[li_indx]));
                  }
               }
               entries.emplace_back(make_tuple(l_orderkey[m], o_orderdate[m],
                                               o_shippriority[m], agg));
            }
         }
      }
   }
#ifdef VERBOSE
   cout << "Preparing results" << endl;
#endif
   // --- output
   auto& result = resources.query->result;
   auto revAttr =
       result->addAttribute("revenue", sizeof(types::Numeric<12, 4>));
   auto orderAttr = result->addAttribute("l_orderkey", sizeof(types::Integer));
   auto dateAttr = result->addAttribute("o_orderdate", sizeof(types::Date));
   auto prioAttr =
       result->addAttribute("o_shippriority", sizeof(types::Integer));

   // write aggregates to result
   auto n = entries.size();
   auto block = result->createBlock(n);
   auto rev = reinterpret_cast<types::Numeric<12, 4>*>(block.data(revAttr));
   auto order = reinterpret_cast<types::Integer*>(block.data(orderAttr));
   auto date = reinterpret_cast<types::Date*>(block.data(dateAttr));
   auto prio = reinterpret_cast<types::Integer*>(block.data(prioAttr));
   for (auto& entry : entries) {
      *order++ = get<0>(entry);
      *date++ = get<1>(entry);
      *prio++ = get<2>(entry);
      *rev++ = get<3>(entry);
   }
   block.addedElements(n);

#ifdef VERBOSE
   cout << "done results" << endl;
   cout << "size" << n << endl;
#endif
   leaveQuery(1);

   return move(resources.query);
}
#endif

NOVECTORIZE std::unique_ptr<runtime::Query> q3_hyper1(Database& db) {

   // --- aggregates
   size_t nrThreads = 1;
   auto resources = initQuery(nrThreads);

   // --- constants
   auto c1 = types::Date::castString("1995-03-15");
   auto c2 = types::Date::castString("1995-03-15");
   string b = "BUILDING";
   auto c3 = types::Char<10>::castString(b.data(), b.size());

   auto& cu = db["customer"];
   auto& ord = db["orders"];
   auto& li = db["lineitem"];

   auto c_mktsegment = cu["c_mktsegment"].data<types::Char<10>>();
   auto c_custkey = cu["c_custkey"].data<types::Integer>();
   auto o_custkey = ord["o_custkey"].data<types::Integer>();
   auto o_orderkey = ord["o_orderkey"].data<types::Integer>();
   auto o_orderdate = ord["o_orderdate"].data<types::Date>();
   auto o_shippriority = ord["o_shippriority"].data<types::Integer>();
   auto l_orderkey = li["l_orderkey"].data<types::Integer>();
   auto l_shipdate = li["l_shipdate"].data<types::Date>();
   auto l_extendedprice = li["l_extendedprice"].data<types::Numeric<12, 2>>();
   auto l_discount = li["l_discount"].data<types::Numeric<12, 2>>();

   using hash = runtime::CRC32Hash;
   using range = tbb::blocked_range<size_t>;

   const auto add = [](const size_t& a, const size_t& b) { return a + b; };
   const size_t morselSize = 100000;

   // build ht for first join
   Hashset<types::Integer, hash> ht1;
   tbb::enumerable_thread_specific<runtime::Stack<decltype(ht1)::Entry>>
       entries1;
   auto found1 = tbb::parallel_reduce(
       range(0, cu.nrTuples, morselSize), 0,
       [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
          auto found = f;
          auto& entries = entries1.local();
          for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
             if (c_mktsegment[i] == c3) {
                entries.emplace_back(ht1.hash(c_custkey[i]), c_custkey[i]);
                found++;
             }
          }
          return found;
       },
       add);
   ht1.setSize(found1);
   parallel_insert(entries1, ht1);

   // join and build second ht
   Hashmapx<types::Integer, std::tuple<types::Date, types::Integer>, hash> ht2;
   tbb::enumerable_thread_specific<runtime::Stack<decltype(ht2)::Entry>>
       entries2;
   auto found2 = tbb::parallel_reduce(
       range(0, ord.nrTuples, morselSize), 0,
       [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
          auto& entries = entries2.local();
          auto found = f;
          for (size_t i = r.begin(), end = r.end(); i != end; ++i)
             if (o_orderdate[i] < c1 && ht1.contains(o_custkey[i])) {
#ifdef VERBOSE
                cout << "-\t" << o_custkey[i] << "\t" << o_orderkey[i] << endl;
#endif
                entries.emplace_back(
                    ht2.hash(o_orderkey[i]), o_orderkey[i],
                    make_tuple(o_orderdate[i], o_shippriority[i]));
                found++;
             }
          return found;
       },
       add);
   ht2.setSize(found2);
   parallel_insert(entries2, ht2);

   const auto one = types::Numeric<12, 2>::castString("1.00");
   const auto zero = types::Numeric<12, 4>::castString("0.00");

   tbb::enumerable_thread_specific<
       Hashmapx<std::tuple<types::Integer, types::Date, types::Integer>,
                types::Numeric<12, 4>, hash, false>>
       groups;

   auto groupOp =
       make_GroupBy<std::tuple<types::Integer, types::Date, types::Integer>,
                    types::Numeric<12, 4>, hash>(
           [](auto& acc, auto&& value) { acc += value; }, zero, nrThreads);

   // preaggregation
   tbb::parallel_for(
       tbb::blocked_range<size_t>(0, li.nrTuples, morselSize),
       [&](const tbb::blocked_range<size_t>& r) {
          auto locals = groupOp.preAggLocals();

          for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
             decltype(ht2)::value_type* v;
             if (l_shipdate[i] > c2 && (v = ht2.findOne(l_orderkey[i]))) {
                locals.consume(
                    make_tuple(l_orderkey[i], get<0>(*v), get<1>(*v)),
                    l_extendedprice[i] * (one - l_discount[i]));
             }
          }
       });

#ifdef VERBOSE
   cout << "prepaing results" << endl;
#endif
   // --- output
   auto& result = resources.query->result;
   auto revAttr =
       result->addAttribute("revenue", sizeof(types::Numeric<12, 4>));
   auto orderAttr = result->addAttribute("l_orderkey", sizeof(types::Integer));
   auto dateAttr = result->addAttribute("o_orderdate", sizeof(types::Date));
   auto prioAttr =
       result->addAttribute("o_shippriority", sizeof(types::Integer));

   groupOp.forallGroups([&](auto& entries) {
      // write aggregates to result
      auto n = entries.size();
      auto block = result->createBlock(n);
      auto rev = reinterpret_cast<types::Numeric<12, 4>*>(block.data(revAttr));
      auto order = reinterpret_cast<types::Integer*>(block.data(orderAttr));
      auto date = reinterpret_cast<types::Date*>(block.data(dateAttr));
      auto prio = reinterpret_cast<types::Integer*>(block.data(prioAttr));
      for (auto block : entries)
         for (auto& entry : block) {
            *order++ = get<0>(entry.k);
            *date++ = get<1>(entry.k);
            *prio++ = get<2>(entry.k);
            *rev++ = entry.v;
         }
      block.addedElements(n);
   });

   leaveQuery(nrThreads);
   return move(resources.query);
}

NOVECTORIZE std::unique_ptr<runtime::Query> q3_hyper(Database& db,
                                                     size_t nrThreads) {

   // --- aggregates
#ifdef VERBOSE
   cout << "hi from q3_hyper" << endl;
#endif
   auto resources = initQuery(nrThreads);

   // --- constants
   auto c1 = types::Date::castString("1995-03-15");
   auto c2 = types::Date::castString("1995-03-15");
   string b = "BUILDING";
   auto c3 = types::Char<10>::castString(b.data(), b.size());

   auto& cu = db["customer"];
   auto& ord = db["orders"];
   auto& li = db["lineitem"];

   auto c_mktsegment = cu["c_mktsegment"].data<types::Char<10>>();
   auto c_custkey = cu["c_custkey"].data<types::Integer>();
   auto o_custkey = ord["o_custkey"].data<types::Integer>();
   auto o_orderkey = ord["o_orderkey"].data<types::Integer>();
   auto o_orderdate = ord["o_orderdate"].data<types::Date>();
   auto o_shippriority = ord["o_shippriority"].data<types::Integer>();
   auto l_orderkey = li["l_orderkey"].data<types::Integer>();
   auto l_shipdate = li["l_shipdate"].data<types::Date>();
   auto l_extendedprice = li["l_extendedprice"].data<types::Numeric<12, 2>>();
   auto l_discount = li["l_discount"].data<types::Numeric<12, 2>>();

   using hash = runtime::CRC32Hash;
   using range = tbb::blocked_range<size_t>;

   const auto add = [](const size_t& a, const size_t& b) { return a + b; };
   const size_t morselSize = 100000;

   // build ht for first join
   Hashset<types::Integer, hash> ht1;
   tbb::enumerable_thread_specific<runtime::Stack<decltype(ht1)::Entry>>
       entries1;
   auto found1 = tbb::parallel_reduce(
       range(0, cu.nrTuples, morselSize), 0,
       [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
          auto found = f;
          auto& entries = entries1.local();
          for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
             if (c_mktsegment[i] == c3) {
                entries.emplace_back(ht1.hash(c_custkey[i]), c_custkey[i]);
                found++;
             }
          }
          return found;
       },
       add);
   ht1.setSize(found1);
   parallel_insert(entries1, ht1);

   // join and build second ht
   Hashmapx<types::Integer, std::tuple<types::Date, types::Integer>, hash> ht2;
   tbb::enumerable_thread_specific<runtime::Stack<decltype(ht2)::Entry>>
       entries2;
   auto found2 = tbb::parallel_reduce(
       range(0, ord.nrTuples, morselSize), 0,
       [&](const tbb::blocked_range<size_t>& r, const size_t& f) {
          auto& entries = entries2.local();
          auto found = f;
          for (size_t i = r.begin(), end = r.end(); i != end; ++i)
             if (o_orderdate[i] < c1 && ht1.contains(o_custkey[i])) {
#ifdef VERBOSE
                cout << "-\t" << o_custkey[i] << "\t" << o_orderkey[i] << endl;
#endif
                entries.emplace_back(
                    ht2.hash(o_orderkey[i]), o_orderkey[i],
                    make_tuple(o_orderdate[i], o_shippriority[i]));
                found++;
             }
          return found;
       },
       add);
   ht2.setSize(found2);
   parallel_insert(entries2, ht2);

   const auto one = types::Numeric<12, 2>::castString("1.00");
   const auto zero = types::Numeric<12, 4>::castString("0.00");

   tbb::enumerable_thread_specific<
       Hashmapx<std::tuple<types::Integer, types::Date, types::Integer>,
                types::Numeric<12, 4>, hash, false>>
       groups;

   auto groupOp =
       make_GroupBy<std::tuple<types::Integer, types::Date, types::Integer>,
                    types::Numeric<12, 4>, hash>(
           [](auto& acc, auto&& value) { acc += value; }, zero, nrThreads);

   // preaggregation
   tbb::parallel_for(
       tbb::blocked_range<size_t>(0, li.nrTuples, morselSize),
       [&](const tbb::blocked_range<size_t>& r) {
          auto locals = groupOp.preAggLocals();

          for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
             decltype(ht2)::value_type* v;
             if (l_shipdate[i] > c2 && (v = ht2.findOne(l_orderkey[i]))) {
                locals.consume(
                    make_tuple(l_orderkey[i], get<0>(*v), get<1>(*v)),
                    l_extendedprice[i] * (one - l_discount[i]));
             }
          }
       });

   // --- output
   auto& result = resources.query->result;
   auto revAttr =
       result->addAttribute("revenue", sizeof(types::Numeric<12, 4>));
   auto orderAttr = result->addAttribute("l_orderkey", sizeof(types::Integer));
   auto dateAttr = result->addAttribute("o_orderdate", sizeof(types::Date));
   auto prioAttr =
       result->addAttribute("o_shippriority", sizeof(types::Integer));
   std::atomic<size_t> n_t;
   groupOp.forallGroups([&](auto& entries) {
      // write aggregates to result
      auto n = entries.size();
      n_t.fetch_add(n, std::memory_order_relaxed);
      auto block = result->createBlock(n);
      auto rev = reinterpret_cast<types::Numeric<12, 4>*>(block.data(revAttr));
      auto order = reinterpret_cast<types::Integer*>(block.data(orderAttr));
      auto date = reinterpret_cast<types::Date*>(block.data(dateAttr));
      auto prio = reinterpret_cast<types::Integer*>(block.data(prioAttr));
      for (auto block : entries)
         for (auto& entry : block) {
            *order++ = get<0>(entry.k);
            *date++ = get<1>(entry.k);
            *prio++ = get<2>(entry.k);
            *rev++ = entry.v;
         }
      block.addedElements(n);
   });

   leaveQuery(nrThreads);
#ifdef VERBOSE
   cout << "size :" << n_t << endl;
#endif
   return move(resources.query);
}

std::unique_ptr<Q3Builder::Q3> Q3Builder::getQuery() {
   using namespace vectorwise;
   auto result = Result();
   previous = result.resultWriter.shared.result->participate();
   auto r = make_unique<Q3>();
   auto customer = Scan("customer");
   Select(Expression().addOp(
       BF(primitives::sel_equal_to_Char_10_col_Char_10_val), //
       Buffer(sel_cust, sizeof(pos_t)),                      //
       Column(customer, "c_mktsegment"),                     //
       Value(&r->c1)));                                      //
   auto order = Scan("orders");
   Select(Expression().addOp(BF(primitives::sel_less_Date_col_Date_val), //
                             Buffer(sel_order, sizeof(pos_t)),           //
                             Column(order, "o_orderdate"),               //
                             Value(&r->c2)));
   HashJoin(Buffer(cust_ord, sizeof(pos_t)), conf.joinAll())
       .setProbeSelVector(Buffer(sel_order), conf.joinSel())
       .addBuildKey(Column(customer, "c_custkey"),       //
                    Buffer(sel_cust),                    //
                    conf.hash_sel_int32_t_col(),         //
                    primitives::scatter_sel_int32_t_col) //
       .addProbeKey(Column(order, "o_custkey"),          //
                    Buffer(sel_order),                   //
                    conf.hash_sel_int32_t_col(),         //
                    primitives::keys_equal_int32_t_col);
   auto lineitem = Scan("lineitem");
   Select(Expression().addOp(BF(primitives::sel_greater_Date_col_Date_val), //
                             Buffer(sel_lineitem, sizeof(pos_t)),           //
                             Column(lineitem, "l_shipdate"),                //
                             Value(&r->c3)));
   HashJoin(Buffer(j1_lineitem, sizeof(pos_t)), conf.joinAll()) //
       .setProbeSelVector(Buffer(sel_lineitem), conf.joinSel())
       .addBuildKey(Column(order, "o_orderkey"), //
                    Buffer(cust_ord),            //
                    conf.hash_sel_int32_t_col(), //
                    primitives::scatter_sel_int32_t_col)
       .addBuildValue(Column(order, "o_orderdate"), Buffer(cust_ord),
                      primitives::scatter_sel_Date_col,
                      Buffer(o_orderdate, sizeof(types::Date)),
                      primitives::gather_col_Date_col)
       .addBuildValue(Column(order, "o_shippriority"), Buffer(cust_ord),
                      primitives::scatter_sel_int32_t_col,
                      Buffer(o_shippriority, sizeof(types::Integer)),
                      primitives::gather_col_int32_t_col)
       .addProbeKey(Column(lineitem, "l_orderkey"), //
                    Buffer(sel_lineitem),           //
                    conf.hash_sel_int32_t_col(),    //
                    primitives::keys_equal_int32_t_col);
   // build value o_orderdate, o_shippriority
   Project().addExpression(
       Expression() //
           .addOp(primitives::proj_sel_minus_int64_t_val_int64_t_col,
                  Buffer(j1_lineitem), //
                  Buffer(result_proj_minus, sizeof(int64_t)), Value(&r->one),
                  Column(lineitem, "l_discount"))
           .addOp(primitives::proj_multiplies_sel_int64_t_col_int64_t_col,
                  Buffer(j1_lineitem),                     //
                  Buffer(result_project, sizeof(int64_t)), //
                  Column(lineitem, "l_extendedprice"),
                  Buffer(result_proj_minus, sizeof(int64_t))));
   // keys: o_orderdate, o_shippriority, l_orderkey
   // sum: revenue
   HashGroup()
       .pushKeySelVec(Buffer(j1_lineitem),
                      Buffer(j1_lineitem_grouped, sizeof(pos_t)))
       .addKey(Column(lineitem, "l_orderkey"), Buffer(j1_lineitem),
               conf.hash_sel_int32_t_col(),
               primitives::keys_not_equal_sel_int32_t_col,
               primitives::partition_by_key_sel_int32_t_col,
               Buffer(j1_lineitem_grouped, sizeof(pos_t)),
               primitives::scatter_sel_int32_t_col,
               primitives::keys_not_equal_row_int32_t_col,
               primitives::partition_by_key_row_int32_t_col,
               primitives::scatter_sel_row_int32_t_col,
               primitives::gather_val_int32_t_col,
               Buffer(l_orderkey, sizeof(int32_t)))
       .addKey(Buffer(o_orderdate),
               primitives::rehash_Date_col, // conf.rehash_Date_col(),
               primitives::keys_not_equal_Date_col,
               primitives::partition_by_key_Date_col,
               primitives::scatter_sel_Date_col,
               primitives::keys_not_equal_row_Date_col,
               primitives::partition_by_key_row_Date_col,
               primitives::scatter_sel_row_Date_col,
               primitives::gather_val_Date_col, Buffer(o_orderdate))
       .addKey(Buffer(o_shippriority), conf.rehash_int32_t_col(),
               primitives::keys_not_equal_int32_t_col,
               primitives::partition_by_key_int32_t_col,
               primitives::scatter_sel_int32_t_col,
               primitives::keys_not_equal_row_int32_t_col,
               primitives::partition_by_key_row_int32_t_col,
               primitives::scatter_sel_row_int32_t_col,
               primitives::gather_val_int32_t_col, Buffer(o_shippriority))
       .addValue(Buffer(result_project), primitives::aggr_init_plus_int64_t_col,
                 primitives::aggr_plus_int64_t_col,
                 primitives::aggr_row_plus_int64_t_col,
                 primitives::gather_val_int64_t_col, Buffer(result_project));

   result.addValue("revenue", Buffer(result_project))
       .addValue("o_shippriority", Buffer(o_shippriority))
       .addValue("o_orderdate", Buffer(o_orderdate))
       .addValue("l_orderkey", Buffer(l_orderkey))
       .finalize();

   r->rootOp = popOperator();
   return r;
}

std::unique_ptr<runtime::Query> q3_vectorwise(Database& db, size_t nrThreads,
                                              size_t vectorSize) {
   using namespace vectorwise;
   WorkerGroup workers(nrThreads);
   vectorwise::SharedStateManager shared;
   std::unique_ptr<runtime::Query> result;
   workers.run([&]() {
      Q3Builder builder(db, shared, vectorSize);
      auto query = builder.getQuery();
      /* auto found = */ query->rootOp->next();
      auto leader = barrier();
      if (leader)
         result = move(
             dynamic_cast<ResultWriter*>(query->rootOp.get())->shared.result);
   });

   return result;
}
