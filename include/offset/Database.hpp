#pragma once
#include <unordered_map>
#include <vector>

#include "common/runtime/Database.hpp"

namespace offset {

typedef std::vector<unsigned> RowIndex;

class Relation : public runtime::Relation {
   std::unordered_map<std::string, RowIndex> rowIndexes;
   
   public:
    RowIndex& getRowIndex(std::string name);
};

class Database : public runtime::Database {
   std::unordered_map<std::string, Relation> relations;

   public:
    Relation& operator[](std::string key);
};

} // namespace offset