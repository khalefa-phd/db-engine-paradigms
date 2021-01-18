#ifndef Offset_Types
#define Offset_Types

#include "common/algebra/Types.hpp"
#include "common/runtime/Types.hpp"

namespace offset {

namespace runtime_types {
class Integer : public types::Integer {
 public:
   int32_t value;
   int32_t offset;
   Integer() {}
   Integer(int32_t value, int32_t offset)
       : value(value), offset(offset) {}
};
}; // namespace runtime_types

namespace algebra_types {
struct Integer : public algebra::Type {
   virtual operator std::string() const override;
   virtual const std::string& cppname() const override;
   virtual size_t rt_size() const override;
};
}; // namespace algebra_types

}; // namespace offset
#endif