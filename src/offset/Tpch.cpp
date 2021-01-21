#include <algorithm>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <random>
#include <stdlib.h>
#include <vector>

#include "errno.h"
#include "sys/stat.h"
#include "sys/time.h"
#include "tbb/tbb.h"

#include "benchmarks/tpch/Queries.hpp"
#include "common/runtime/Hash.hpp"
#include "common/runtime/Hashmap.hpp"
#include "common/runtime/Mmap.hpp"
#include "common/runtime/Types.hpp"
#include "hyper/ParallelHelper.hpp"

#include "vectorwise/Operations.hpp"
#include "vectorwise/Operators.hpp"
#include "vectorwise/Primitives.hpp"
#include "vectorwise/QueryBuilder.hpp"
#include "vectorwise/VectorAllocator.hpp"
#include "vectorwise/defs.hpp"

#include "offset/Tpch.hpp"
#include "offset/Types.hpp"

using namespace runtime;
using namespace std;

template<class T> inline std::vector<T> getValues(std::multimap<T, int> mm) {
   std::vector<T> keys;
   unsigned i = 0;
   for(auto it = mm.begin(), end = mm.end(); it != end; it = mm.upper_bound(it->first)) {
	   auto key = offset::runtime_types::cast(it->first, i++);
      keys.emplace_back(key);
   }
   return keys;
}

template<class T> inline std::vector<unsigned> computeOffsets(std::vector<T> values, std::multimap<T, int> ocurrences) {
   unsigned i = 0;
   std::vector<unsigned> row_indexes;
   row_indexes.reserve(ocurrences.size());
   for (auto& value: values) {
      auto range = ocurrences.equal_range(value);
      row_indexes.insert(row_indexes.end(), range.begin(), range.end());
   }
}

static inline double gettime() {
   struct timeval now_tv;
   gettimeofday(&now_tv, NULL);
   return ((double)now_tv.tv_sec) + ((double)now_tv.tv_usec) / 1000000.0;
}

enum RTType {
   Integer,
   Numeric_12_2,
   Numeric_18_2,
   Date,
   Char_1,
   Char_6,
   Char_7,
   Char_9,
   Char_10,
   Char_11,
   Char_12,
   Char_15,
   Char_18,
   Char_22,
   Char_25,
   Varchar_11,
   Varchar_12,
   Varchar_22,
   Varchar_23,
   Varchar_25,
   Varchar_40,
   Varchar_44,
   Varchar_55,
   Varchar_79,
   Varchar_101,
   Varchar_117,
   Varchar_152,
   Varchar_199
};
RTType algebraToRTType(algebra::Type* t) {
   if (dynamic_cast<algebra::Integer*>(t)) {
      return RTType::Integer;
   } else if (auto e = dynamic_cast<algebra::Numeric*>(t)) {
      if (e->size == 12 and e->precision == 2) return Numeric_12_2;
      if (e->size == 18 and e->precision == 2)
         return Numeric_18_2;
      else
         throw runtime_error("Unknown Numeric precision");
   } else if (auto e = dynamic_cast<algebra::Char*>(t)) {
      switch (e->size) {
      case 1: return Char_1;
      case 6: return Char_6;
      case 7: return Char_7;
      case 9: return Char_9;
      case 10: return Char_10;
      case 11: return Char_11;
      case 12: return Char_12;
      case 15: return Char_15;
      case 18: return Char_18;
      case 22: return Char_22;
      case 25: return Char_25;
      default: throw runtime_error("Unknown Char size");
      }
   } else if (auto e = dynamic_cast<algebra::Varchar*>(t)) {
      switch (e->size) {
      case 11: return Varchar_11;
      case 12: return Varchar_12;
      case 22: return Varchar_22;
      case 23: return Varchar_23;
      case 25: return Varchar_25;
      case 40: return Varchar_40;
      case 44: return Varchar_44;
      case 55: return Varchar_55;
      case 79: return Varchar_79;
      case 101: return Varchar_101;
      case 117: return Varchar_117;
      case 152: return Varchar_152;
      case 199: return Varchar_199;

      default:
         throw runtime_error("Unknown Varchar size" + std::to_string(e->size));
      }
   } else if (dynamic_cast<algebra::Date*>(t)) {
      return Date;
   } else {
      throw runtime_error("Unknown type");
   }
}
struct ColumnConfigOwning {
   string name;
   unique_ptr<algebra::Type> type;
   ColumnConfigOwning(string n, unique_ptr<algebra::Type>&& t)
       : name(n), type(move(t)) {}
   ColumnConfigOwning(ColumnConfigOwning&&) = default;
};
struct ColumnConfig {
   string name;
   algebra::Type* type;
   ColumnConfig(string n, algebra::Type* t) : name(n), type(t) {}
};

#define COMMA ,
#define EACHTYPE                                                               \
   case Integer: D(types::Integer)                                             \
   case Numeric_12_2: D(types::Numeric<12 COMMA 2>)                            \
   case Numeric_18_2: D(types::Numeric<18 COMMA 2>)                            \
   case Date: D(types::Date)                                                   \
   case Char_1: D(types::Char<1>)                                              \
   case Char_6: D(types::Char<6>)                                              \
   case Char_7: D(types::Char<7>)                                              \
   case Char_9: D(types::Char<9>)                                              \
   case Char_10: D(types::Char<10>)                                            \
   case Char_11: D(types::Char<11>)                                            \
   case Char_12: D(types::Char<12>)                                            \
   case Char_15: D(types::Char<15>)                                            \
   case Char_18: D(types::Char<18>)                                            \
   case Char_22: D(types::Char<22>)                                            \
   case Char_25: D(types::Char<25>)                                            \
   case Varchar_11: D(types::Varchar<11>)                                      \
   case Varchar_12: D(types::Varchar<12>)                                      \
   case Varchar_22: D(types::Varchar<22>)                                      \
   case Varchar_23: D(types::Varchar<23>)                                      \
   case Varchar_25: D(types::Varchar<25>)                                      \
   case Varchar_40: D(types::Varchar<40>)                                      \
   case Varchar_44: D(types::Varchar<44>)                                      \
   case Varchar_55: D(types::Varchar<55>)                                      \
   case Varchar_79: D(types::Varchar<79>)                                      \
   case Varchar_101: D(types::Varchar<101>)                                    \
   case Varchar_117: D(types::Varchar<117>)                                    \
   case Varchar_152: D(types::Varchar<152>)                                    \
   case Varchar_199: D(types::Varchar<199>)

typedef std::unordered_map<std::string, std::multimap<void*, int>> UniqueValuesMap;

inline void parse(ColumnConfig& columnMetaData, UniqueValuesMap* uniqueVals, std::string& line,
                  unsigned& begin, unsigned& end, uint64_t rowNumber) {

   const char* start = line.data() + begin;
   end = line.find_first_of('|', begin);
   size_t size = end - begin;
   if(!uniqueVals->contains(columnMetaData.name)) { 
      throw runtime_error("Column does not exist");
   };
   auto &uniqueValsInColumn = uniqueVals->at(columnMetaData.name);

#define D(type)                                                                \
   reinterpret_cast<std::multimap<type,int>&>(uniqueValsInColumn).emplace(     \
       type::castString(start, size), rowNumber);                              \
   break;

   switch (algebraToRTType(columnMetaData.type)) { EACHTYPE }
#undef D
   begin = end + 1;
}

void writeBinary(ColumnConfig& col, std::vector<void*>& data,
                 std::string path) {
#define D(type)                                                                \
   {                                                                           \
      auto name = path + "_" + col.name;                                       \
      runtime::Vector<type>::writeBinary(                                      \
          name.data(), reinterpret_cast<std::vector<type>&>(data));            \
      break;                                                                   \
   }
   switch (algebraToRTType(col.type)) { EACHTYPE }
#undef D
}

size_t readBinary(runtime::Relation& r, ColumnConfig& col, std::string path) {
#define D(rt_type)                                                             \
   {                                                                           \
      auto name = path + "_" + col.name;                                       \
      auto& attr = r[col.name];                                                \
      /* attr.name = col.name;   */                                            \
      /*attr.type = col.type;  */                                              \
      auto& data = attr.typedAccessForChange<rt_type>();                       \
      data.readBinary(name.data());                                            \
      return data.size();                                                      \
   }
   switch (algebraToRTType(col.type)) {
      EACHTYPE default : throw runtime_error("Unknown type");
   }
#undef D
}

void computeOffsets(std::vector<void*>& col, ColumnConfig& columnMetaData, std::multimap<void*, int> uniqueValsInCol) {
#define D(type)                                                                                    \
   {                                                                                               \
      auto values = getValues<type>(reinterpret_cast<std::multimap<type, int>&>(uniqueValsInCol)); \
      std::move(values.begin(), values.end(),         \
      reinterpret_cast<std::vector<type>&>(col));     \
      break;                                          \
   }

   switch (algebraToRTType(columnMetaData.type)) { EACHTYPE }
#undef D
}

void parseColumns(runtime::Relation& relation, std::vector<ColumnConfigOwning>& cols,
                  std::string dir, std::string fileName) {

   std::vector<ColumnConfig> colsC;
   for (auto& col : cols) {
      colsC.emplace_back(col.name, col.type.get());
      relation.insert(col.name, move(col.type));
   }

   bool allColumnsMMaped = true;
   string cachedir = dir + "/cached/";
   if (!mkdir((dir + "/cached/").c_str(), 0777))
      throw runtime_error("Could not create dir 'cached': " + dir + "/cached/");
  
   // Check if columns are already cached
   for (auto& col : colsC)
      if (!std::ifstream(cachedir + fileName + "_" + col.name))
         allColumnsMMaped = false;

   if (!allColumnsMMaped) {
      UniqueValuesMap uniqueVals;
      for (auto& col : colsC) uniqueVals.emplace(col.name, std::multimap<void*, int>());
      ifstream relationFile(dir + fileName + ".tbl");
      if (!relationFile.is_open())
         throw runtime_error("csv file not found: " + dir);
      string line;
      unsigned begin = 0, end;
      uint64_t rowNumber = 0;
      while (getline(relationFile, line)) {
         rowNumber++;
         unsigned i = 0;
         for (auto& col : colsC) parse(col, &uniqueVals, line, begin, end, rowNumber);
         begin = 0;
      }
      std::vector<std::vector<void*>> attributes;
      std::vector<std::vector<void*>> offsets;
      attributes.assign(colsC.size(), {});
      offsets.assign(colsC.size(), {});
      unsigned i = 0;
      for (auto& colMetaData : colsC) {
         computeOffsets(attributes[i++], colMetaData, uniqueVals.at(colMetaData.name));
      }

      rowNumber = 0;
      for (auto& col : colsC)
         writeBinary(col, attributes[rowNumber++], cachedir + fileName);
   }
   // load mmaped files
   size_t size = 0;
   size_t diffs = 0;
   for (auto& col : colsC) {
      auto oldSize = size;
      size = readBinary(relation, col, cachedir + fileName);
      diffs += (oldSize != size);
   }
   if (diffs > 1)
      throw runtime_error("Columns of " + fileName + " differ in size.");
   relation.nrTuples = size;
}

std::vector<ColumnConfigOwning>
configX(std::initializer_list<ColumnConfigOwning>&& l) {
   std::vector<ColumnConfigOwning> v;
   for (auto& e : l)
      v.emplace_back(e.name, move(const_cast<unique_ptr<Type>&>(e.type)));
   return v;
}

namespace offset {
namespace tpch {
void import(std::string dir, Database& db) {

   //--------------------------------------------------------------------------------
   // part
   {
      auto& rel = db["part"];
      rel.name = "part";
      auto columns =
          configX({{"p_partkey", make_unique<algebra::Integer>()},
                   {"p_name", make_unique<algebra::Varchar>(55)},
                   {"p_mfgr", make_unique<algebra::Char>(25)},
                   {"p_brand", make_unique<algebra::Char>(10)},
                   {"p_type", make_unique<algebra::Varchar>(25)},
                   {"p_size", make_unique<algebra::Integer>()},
                   {"p_container", make_unique<algebra::Char>(10)},
                   {"p_retailprice", make_unique<algebra::Numeric>(12, 2)},
                   {"p_comment", make_unique<algebra::Varchar>(23)}});
      parseColumns(rel, columns, dir, "part");
   }
   //--------------------------------------------------------------------------------
   // supplier
   {
      auto& rel = db["supplier"];
      rel.name = "supplier";
      auto columns =
          configX({{"s_suppkey", make_unique<algebra::Integer>()},
                   {"s_name", make_unique<algebra::Char>(25)},
                   {"s_address", make_unique<algebra::Varchar>(40)},
                   {"s_nationkey", make_unique<algebra::Integer>()},
                   {"s_phone", make_unique<algebra::Char>(15)},
                   {"s_acctbal", make_unique<algebra::Numeric>(12, 2)},
                   {"s_comment", make_unique<algebra::Varchar>(101)}});
      parseColumns(rel, columns, dir, "supplier");
   }
   //--------------------------------------------------------------------------------
   // partsupp
   {
      auto& rel = db["partsupp"];
      rel.name = "partsupp";
      auto columns =
          configX({{"ps_partkey", make_unique<algebra::Integer>()},
                   {"ps_suppkey", make_unique<algebra::Integer>()},
                   {"ps_availqty", make_unique<algebra::Integer>()},
                   {"ps_supplycost", make_unique<algebra::Numeric>(12, 2)},
                   {"ps_comment", make_unique<algebra::Varchar>(199)}});
      parseColumns(rel, columns, dir, "partsupp");
   }
   //------------------------------------------------------------------------------
   // customer
   {
      auto& cu = db["customer"];
      cu.name = "customer";
      auto columns =
          configX({{"c_custkey", make_unique<algebra::Integer>()},
                   {"c_name", make_unique<algebra::Char>(25)},
                   {"c_address", make_unique<algebra::Varchar>(40)},
                   {"c_nationkey", make_unique<algebra::Integer>()},
                   {"c_phone", make_unique<algebra::Char>(15)},
                   {"c_acctbal", make_unique<algebra::Numeric>(12, 2)},
                   {"c_mktsegment", make_unique<algebra::Char>(10)},
                   {"c_comment", make_unique<algebra::Varchar>(117)}});

      parseColumns(cu, columns, dir, "customer");
   }

   //------------------------------------------------------------------------------
   // orders
   {
      auto& od = db["orders"];
      od.name = "orders";
      auto columns =
          configX({{"o_orderkey", make_unique<algebra::Integer>()},
                   {"o_custkey", make_unique<algebra::Integer>()},
                   {"o_orderstatus", make_unique<algebra::Char>(1)},
                   {"o_totalprice", make_unique<algebra::Numeric>(12, 2)},
                   {"o_orderdate", make_unique<algebra::Date>()},
                   {"o_orderpriority", make_unique<algebra::Char>(15)},
                   {"o_clerk", make_unique<algebra::Char>(15)},
                   {"o_shippriority", make_unique<algebra::Integer>()},
                   {"o_comment", make_unique<algebra::Varchar>(79)}});
      parseColumns(od, columns, dir, "orders");
   }
   //--------------------------------------------------------------------------------
   // lineitem
   {
      auto& li = db["lineitem"];
      li.name = "lineitem";
      auto columns =
          configX({{"l_orderkey", make_unique<algebra::Integer>()},
                   {"l_partkey", make_unique<algebra::Integer>()},
                   {"l_suppkey", make_unique<algebra::Integer>()},
                   {"l_linenumber", make_unique<algebra::Integer>()},
                   {"l_quantity", make_unique<algebra::Numeric>(12, 2)},
                   {"l_extendedprice", make_unique<algebra::Numeric>(12, 2)},
                   {"l_discount", make_unique<algebra::Numeric>(12, 2)},
                   {"l_tax", make_unique<algebra::Numeric>(12, 2)},
                   {"l_returnflag", make_unique<algebra::Char>(1)},
                   {"l_linestatus", make_unique<algebra::Char>(1)},
                   {"l_shipdate", make_unique<algebra::Date>()},
                   {"l_commitdate", make_unique<algebra::Date>()},
                   {"l_receiptdate", make_unique<algebra::Date>()},
                   {"l_shipinstruct", make_unique<algebra::Char>(25)},
                   {"l_shipmode", make_unique<algebra::Char>(10)},
                   {"l_comment", make_unique<algebra::Varchar>(44)}});

      parseColumns(li, columns, dir, "lineitem");
   }
   //--------------------------------------------------------------------------------
   // nation
   {
      auto& rel = db["nation"];
      rel.name = "nation";
      auto columns =
          configX({{"n_nationkey", make_unique<algebra::Integer>()},
                   {"n_name", make_unique<algebra::Char>(25)},
                   {"n_regionkey", make_unique<algebra::Integer>()},
                   {"n_comment", make_unique<algebra::Varchar>(152)}});
      parseColumns(rel, columns, dir, "nation");
   }
   //--------------------------------------------------------------------------------
   // region
   {
      auto& rel = db["region"];
      rel.name = "region";
      auto columns =
          configX({{"r_regionkey", make_unique<algebra::Integer>()},
                   {"r_name", make_unique<algebra::Char>(25)},
                   {"r_comment", make_unique<algebra::Varchar>(152)}});
      parseColumns(rel, columns, dir, "region");
   }
   {
      auto start = gettime();

      // build indexes
      {
         auto& indx = db.getindex("cust_ord");
         auto& indx_val = db.getindex("cust_ord_vals");

         auto& cu = db["customer"];
         auto& ord = db["orders"];

         auto c_custkey = cu["c_custkey"].data<types::Integer>();
         auto o_custkey = ord["o_custkey"].data<types::Integer>();
         //  auto o_orderkey = ord["o_orderkey"].data<types::Integer>();

         using hash = runtime::CRC32Hash;
         Hashmapx<types::Integer, size_t, hash> ht;
         runtime::Stack<decltype(ht)::Entry> entries;
         size_t count = 0;
         for (size_t i = ord.nrTuples - 1;; i--) {
            entries.emplace_back(ht.hash(o_custkey[i]), o_custkey[i], i);
            count++;
            if (i == 0) break;
         }
         ht.setSize(count);
         ht.insertAll(entries);
         // cout << "+++++++++++++++++++++++++++++++++++++" << endl;
         // iterate over customer table
         size_t cnt = 0;
         for (size_t i = 0; i < cu.nrTuples; i++) {
            auto h = ht.hash(c_custkey[i]);

            auto entry =
                reinterpret_cast<decltype(ht)::Entry*>(ht.find_chain_tagged(h));

            for (; entry != ht.end();
                 entry = reinterpret_cast<decltype(ht)::Entry*>(entry->h.next))
               if (entry->h.hash == h && entry->k == c_custkey[i]) {
#ifdef VERBOSE
                  cout << "*"
                       << "\t" << i << "\t" << c_custkey[i] << "\t" << entry->v
                       << endl;
#endif
                  indx_val.emplace_back(entry->v);
                  cnt++;
               }
            indx.emplace_back(cnt);
         }
      }

      // build indexes
      {
         auto& indx = db.getindex("ord_li");
         auto& indx_val = db.getindex("ord_li_vals");

         auto& ord = db["orders"];
         auto& li = db["lineitem"];

         auto l_orderkey = li["l_orderkey"].data<types::Integer>();
         auto o_orderkey = ord["o_orderkey"].data<types::Integer>();

         using hash = runtime::CRC32Hash;
         Hashmapx<types::Integer, size_t, hash> ht;
         runtime::Stack<decltype(ht)::Entry> entries;
         size_t count = 0;
         //  cout << "+++++++++++++++++++++++++++++++++++" << endl;
         for (size_t i = li.nrTuples - 1;; i--) {
            entries.emplace_back(ht.hash(l_orderkey[i]), l_orderkey[i], i);
            count++;
            if (i == 0) break;
         }
         // cout << "count " << count << endl;
         ht.setSize(count);
         ht.insertAll(entries);

         // iterate over orders table
         size_t cnt = 0;
         for (size_t i = 0; i < ord.nrTuples; i++) {
            auto h = ht.hash(o_orderkey[i]);

            auto entry =
                reinterpret_cast<decltype(ht)::Entry*>(ht.find_chain_tagged(h));

            for (; entry != ht.end();
                 entry =
                     reinterpret_cast<decltype(ht)::Entry*>(entry->h.next)) {
               if (entry->h.hash == h && entry->k == o_orderkey[i]) {
#ifdef VERBOSE
                  cout << "*"
                       << "\t" << i << "\t" << o_orderkey[i] << "\t" << entry->v
                       << endl;
#endif
                  indx_val.emplace_back(entry->v);
                  cnt++;
               }
            }
            indx.emplace_back(cnt);
         }
         // cout << "cnt " << cnt << endl;
      }
      auto end = gettime();

      cerr << "Building index time " << (end - start) << endl;
   }
#ifdef VERBOSE
   cout << "______________________________________________" << endl;
   {
      auto& ci = db.getindex("cust_ord");
      auto& cv = db.getindex("cust_ord_vals");
      for (size_t i = 0; i < ci.size(); i++) {
         size_t b = i > 0 ? ci[i - 1] : 0;
         size_t e = ci[i];
         std::cout << i << "\tcnt " << (e - b) << endl;
         for (size_t j = b; j < e; j++) cout << "\t" << cv[j] << endl;
      }
   }
   {
      cout << "______________________________________________" << endl;
      auto& oi = db.getindex("ord_li");
      auto& ov = db.getindex("ord_li_vals");

      for (size_t i = 0; i < oi.size(); i++) {
         size_t b = i > 0 ? oi[i - 1] : 0;
         size_t e = oi[i];
         std::cout << i << "\tcnt " << (e - b) << endl;
         for (size_t j = b; j < e; j++) cout << "\t" << ov[j] << endl;
      }
   }
   cout << "______________________________________________" << endl;
#endif

   {
      auto& indx = db.getindex("orders_key");

      auto& ord = db["orders"];

      auto o_orderkey = ord["o_orderkey"].data<types::Integer>();
      auto max_order_key = o_orderkey[0];
      for (size_t i = 1; i < ord.nrTuples; i++)
         max_order_key = max(max_order_key, o_orderkey[i]);

      indx.resize(max_order_key.value);
      for (size_t i = 0; i < ord.nrTuples; i++) {
         auto j = o_orderkey[i].value;
         indx[j] = i;
      }
   }

   {
      auto& indx = db.getindex("customer_key");
      auto& cu = db["customer"];
      auto c_custkey = cu["c_custkey"].data<types::Integer>();

      auto max_cust_key = c_custkey[0];
      for (size_t i = 1; i < cu.nrTuples; i++)
         max_cust_key = max(max_cust_key, c_custkey[i]);

      indx.resize(max_cust_key.value);
      for (size_t i = 0; i < cu.nrTuples; i++) {
         auto j = c_custkey[i].value;
         indx[j] = i;
      }
   }
}

void importSSB(std::string dir, Database& db) {

   //--------------------------------------------------------------------------------
   // lineorder
   {
      auto& rel = db["lineorder"];
      rel.name = "lineorder";
      auto columns =
          configX({{"lo_orderkey", make_unique<algebra::Integer>()},
                   {"lo_linenumber", make_unique<algebra::Integer>()},
                   {"lo_custkey", make_unique<algebra::Integer>()},
                   {"lo_partkey", make_unique<algebra::Integer>()},
                   {"lo_suppkey", make_unique<algebra::Integer>()},
                   {"lo_orderdate", make_unique<algebra::Integer>()},
                   {"lo_orderpriority", make_unique<algebra::Char>(15)},
                   {"lo_shippriority", make_unique<algebra::Char>(1)},
                   {"lo_quantity", make_unique<algebra::Integer>()},
                   {"lo_extendedprice", make_unique<algebra::Numeric>(18, 2)},
                   {"lo_ordtotalprice", make_unique<algebra::Numeric>(18, 2)},
                   {"lo_discount", make_unique<algebra::Numeric>(18, 2)},
                   {"lo_revenue", make_unique<algebra::Numeric>(18, 2)},
                   {"lo_supplycost", make_unique<algebra::Numeric>(18, 2)},
                   {"lo_tax", make_unique<algebra::Integer>()},
                   {"lo_commitdate", make_unique<algebra::Integer>()},
                   {"lo_shopmode", make_unique<algebra::Char>(10)}});
      parseColumns(rel, columns, dir, rel.name);
   }
   //--------------------------------------------------------------------------------
   // part
   {
      auto& rel = db["part"];
      rel.name = "part";
      auto columns = configX({{"p_partkey", make_unique<algebra::Integer>()},
                              {"p_name", make_unique<algebra::Varchar>(22)},
                              {"p_mfgr", make_unique<algebra::Char>(6)},
                              {"p_category", make_unique<algebra::Char>(7)},
                              {"p_brand1", make_unique<algebra::Char>(9)},
                              {"p_color", make_unique<algebra::Varchar>(11)},
                              {"p_type", make_unique<algebra::Varchar>(25)},
                              {"p_size", make_unique<algebra::Integer>()},
                              {"p_container", make_unique<algebra::Char>(10)}});
      parseColumns(rel, columns, dir, rel.name);
   }
   //--------------------------------------------------------------------------------
   // supplier
   {
      auto& rel = db["supplier"];
      rel.name = "supplier";
      auto columns = configX({{"s_suppkey", make_unique<algebra::Integer>()},
                              {"s_name", make_unique<algebra::Char>(25)},
                              {"s_address", make_unique<algebra::Varchar>(25)},
                              {"s_city", make_unique<algebra::Char>(10)},
                              {"s_nation", make_unique<algebra::Char>(15)},
                              {"s_region", make_unique<algebra::Char>(12)},
                              {"s_phone", make_unique<algebra::Char>(15)}});
      parseColumns(rel, columns, dir, rel.name);
   }
   //--------------------------------------------------------------------------------
   // customer
   {
      auto& rel = db["customer"];
      rel.name = "customer";
      auto columns =
          configX({{"c_custkey", make_unique<algebra::Integer>()},
                   {"c_name", make_unique<algebra::Varchar>(25)},
                   {"c_address", make_unique<algebra::Varchar>(25)},
                   {"c_city", make_unique<algebra::Char>(10)},
                   {"c_nation", make_unique<algebra::Char>(15)},
                   {"c_region", make_unique<algebra::Char>(12)},
                   {"c_phone", make_unique<algebra::Char>(15)},
                   {"c_mktsegment", make_unique<algebra::Char>(10)}});
      parseColumns(rel, columns, dir, rel.name);
   }
   //--------------------------------------------------------------------------------
   // date
   {
      auto& rel = db["date"];
      rel.name = "date";
      auto columns =
          configX({{"d_datekey", make_unique<algebra::Integer>()},
                   {"d_date", make_unique<algebra::Char>(18)},
                   {"d_dayofweek", make_unique<algebra::Char>(9)},
                   {"d_month", make_unique<algebra::Char>(9)},
                   {"d_year", make_unique<algebra::Integer>()},
                   {"d_yearmonthnum", make_unique<algebra::Integer>()},
                   {"d_yearmonth", make_unique<algebra::Char>(7)},
                   {"d_daynuminweek", make_unique<algebra::Integer>()},
                   {"d_daynuminmonth", make_unique<algebra::Integer>()},
                   {"d_daynuminyear", make_unique<algebra::Integer>()},
                   {"d_monthnuminyear", make_unique<algebra::Integer>()},
                   {"d_weeknuminyear", make_unique<algebra::Integer>()},
                   {"d_sellingseasin", make_unique<algebra::Varchar>(12)},
                   {"d_lastdayinweekfl", make_unique<algebra::Integer>()},
                   {"d_lastdayinmonthfl", make_unique<algebra::Integer>()},
                   {"d_holidayfl", make_unique<algebra::Integer>()},
                   {"d_weekdayfl", make_unique<algebra::Integer>()}});
      parseColumns(rel, columns, dir, rel.name);
   }
}
} // namespace tpch
} // namespace offset