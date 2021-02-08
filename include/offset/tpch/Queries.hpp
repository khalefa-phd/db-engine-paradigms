
#pragma once
#include "common/runtime/Query.hpp"
#include "offset/Database.hpp"

namespace offset {
namespace tpch {
namespace queries {
std::unique_ptr<runtime::Query> q1_offset(offset::Database& db,
                                          size_t nrThreads);
}
} // namespace tpch
} // namespace offset