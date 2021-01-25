#pragma once
#include <string>
#include <map>

#include "offset/Database.hpp"

namespace offset {
namespace tpch {
/// imports tpch relations from CSVs in dir into db
void import(std::string dir, offset::Database& db);
} // namespace tpch
} // namespace offset
