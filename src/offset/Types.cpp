#include "offset/Types.hpp"

namespace offset {

namespace runtime_types {
static Integer cast(types::Integer number, uint32_t offset) {
   return Integer(number.value, offset);
}
template <unsigned len, unsigned precision> static Numeric<len, precision> cast(types::Numeric<len, precision> number,
                                    uint32_t offset) {
   return Numeric<len, precision>(number.value, offset);
}
}; // namespace runtime_types

namespace algebra_types {

Integer::operator std::string() const { return "Integer"; }
size_t Integer::rt_size() const { return sizeof(runtime_types::Integer); };
const std::string& Integer::cppname() const {
   static std::string cppname = "Integer";
   return cppname;
}

}; // namespace algebra_types

}; // namespace offset