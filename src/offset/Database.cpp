#include "offset/Database.hpp"

namespace offset {

RowIndex& Relation::getRowIndex(std::string name) {
    return rowIndexes[name];
};

Relation& Database::operator[](std::string key) { return relations[key]; }

} // namespace offset