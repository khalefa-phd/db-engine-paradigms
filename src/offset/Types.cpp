#include "offset/Types.hpp"

namespace offset {

namespace runtime_types {};

namespace algebra_types {

Integer::operator std::string() const { return "Integer"; }
size_t Integer::rt_size() const { return sizeof(runtime_types::Integer); };
const std::string& Integer::cppname() const {
   static std::string cppname = "Integer";
   return cppname;
}

}; // namespace algebra_types

}; // namespace offset