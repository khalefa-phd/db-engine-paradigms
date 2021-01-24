#pragma once
#include "common/runtime/Database.hpp"
#include <string>
#include <map>

namespace offset {
namespace tpch {
/// imports tpch relations from CSVs in dir into db
void import(std::string dir, runtime::Database& db);
} // namespace tpch
} // namespace offset
