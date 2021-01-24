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

#include "offset/Database.hpp"
#include "offset/Tpch.hpp"
#include "offset/Types.hpp"

using namespace runtime;
using namespace std;

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

typedef std::unordered_map<std::string, std::multimap<void*, unsigned>>
    UniqueValuesMap;

template <class T>
inline std::vector<T> getValues(std::multimap<T, unsigned> mm) {
   std::vector<T> keys;
   for (auto it = mm.begin(), end = mm.end(); it != end;
        it = mm.upper_bound(it->first)) {
      auto key = it->first;
      keys.emplace_back(key);
   }
   return keys;
}

template <class Type, class OffsetType>
inline void computeRowIndexesAndFixOffsets(
    ColumnConfig& columnMetaData, std::vector<Type>& values,
    std::vector<void*>& col, std::vector<unsigned>& colRowIndexes,
    std::multimap<Type, unsigned>& ocurrences) {

   colRowIndexes.reserve(ocurrences.size());
   for (auto value : values) {
      auto range = ocurrences.equal_range(value);
      auto offsetValue = OffsetType(value, colRowIndexes.size());
      std::transform(
          range.first, range.second, std::back_inserter(colRowIndexes),
          [](std::pair<Type, unsigned> element) { return element.second; });
      reinterpret_cast<vector<OffsetType>&>(col).emplace_back(offsetValue);
   }
}

inline void parse(ColumnConfig& columnMetaData, UniqueValuesMap* uniqueVals,
                  std::string& line, unsigned& begin, unsigned& end,
                  uint64_t rowNumber) {

   const char* start = line.data() + begin;
   end = line.find_first_of('|', begin);
   size_t size = end - begin;
   if (!uniqueVals->contains(columnMetaData.name)) {
      throw runtime_error("Column does not exist");
   };
   auto& uniqueValsInColumn = uniqueVals->at(columnMetaData.name);

#define D(type)                                                                \
   reinterpret_cast<std::multimap<type, int>&>(uniqueValsInColumn)             \
       .emplace(type::castString(start, size), rowNumber);                     \
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

void writeBinary(ColumnConfig& col, offset::RowIndex& data, std::string path) {
   std::ofstream file;
   auto fname = path + "_" + col.name + "_rowIndex";
   file.open(fname, std::ios::out | std::ios::binary);
   if (!data.empty())
      file.write(reinterpret_cast<char*>(&data[0]),
                 data.size() * sizeof(data[0]));
}

bool readBinary(ColumnConfig& col, offset::RowIndex& data, std::string path) {
   auto fname = path + "_" + col.name + "_rowIndex";
   std::ifstream file(fname, std::ios::binary);
   unsigned element;
   while (file.read(reinterpret_cast<char*>(&element), sizeof(element)))
      data.emplace_back(element);
   return true;
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

void computeOffsets(std::vector<void*>& col,
                    std::vector<unsigned>& colRowIndexes,
                    ColumnConfig& columnMetaData,
                    std::multimap<void*, unsigned> uniqueValsInCol) {
#define D(type)                                                                \
   {                                                                           \
      auto values = getValues<type>(                                           \
          reinterpret_cast<std::multimap<type, unsigned>&>(uniqueValsInCol));  \
      computeRowIndexesAndFixOffsets<type, offset::type>(                                    \
          columnMetaData, values, col, colRowIndexes,                          \
          reinterpret_cast<std::multimap<type, unsigned>&>(uniqueValsInCol));  \
      break;                                                                   \
   }

   switch (algebraToRTType(columnMetaData.type)) { EACHTYPE }

#undef D
}

void parseColumns(offset::Relation& relation,
                  std::vector<ColumnConfigOwning>& cols, std::string dir,
                  std::string fileName) {

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
      for (auto& col : colsC)
         uniqueVals.emplace(col.name, std::multimap<void*, unsigned>());
      ifstream relationFile(dir + fileName + ".tbl");
      if (!relationFile.is_open())
         throw runtime_error("csv file not found: " + dir);
      string line;
      unsigned begin = 0, end;
      uint64_t rowNumber = 0;
      while (getline(relationFile, line)) {
         rowNumber++;
         unsigned i = 0;
         for (auto& col : colsC)
            parse(col, &uniqueVals, line, begin, end, rowNumber);
         begin = 0;
      }
      std::vector<std::vector<void*>> attributes;
      attributes.assign(colsC.size(), {});
      unsigned i = 0;
      for (auto& colMetaData : colsC) {
         auto& colName = colMetaData.name;
         auto& rowIndex = relation.getRowIndex(colName);
         computeOffsets(attributes[i++], rowIndex, colMetaData,
                        uniqueVals.at(colName));
      }

      rowNumber = 0;
      for (auto& col : colsC) {
         auto& colName = col.name;
         auto& rowIndex = relation.getRowIndex(colName);
         writeBinary(col, attributes[rowNumber++], cachedir + fileName);
         writeBinary(col, rowIndex, cachedir + fileName);
      }
   }
   // load mmaped files
   size_t size = 0;
   size_t diffs = 0;
   for (auto& col : colsC) {
      auto oldSize = size;
      auto& colName = col.name;
      auto& rowIndex = relation.getRowIndex(colName);
      if (rowIndex.empty()) readBinary(col, rowIndex, cachedir + fileName);
      size = rowIndex.size();
      // Relation will store unique values with their respective offset,
      // thus size may differ
      readBinary(relation, col, cachedir + fileName);
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
void import(std::string dir, offset::Database& db) {
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
}
} // namespace tpch
} // namespace offset
