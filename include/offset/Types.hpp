#pragma once

#include "common/algebra/Types.hpp"
#include "common/runtime/Types.hpp"

namespace offset {

namespace runtime_types {
  
struct ContainerCmp {
  bool operator()(const types::Integer& lhs, const types::Integer& rhs) const {
    return lhs == rhs;
  }
  template <unsigned len, unsigned precision> bool operator()(const types::Numeric<len,precision>& lhs, const types::Numeric<len,precision>& rhs) const {
     return lhs == rhs;
  }
};

class Integer : public types::Integer {
 public:
   int32_t value;
   uint32_t offset;

   Integer() {}
   Integer(int32_t value, uint32_t offset) : value(value), offset(offset) {}

   inline bool operator==(const Integer& n) const {
      return (value == n.value) && (offset == n.offset);
   }
};

template <unsigned len, unsigned precision> class Numeric : public types::Numeric<len, precision> {
 public:
   int64_t value;
   uint32_t offset;
   Numeric(types::Integer x, uint32_t offset)
       : value(x.value * types::numericShifts[precision]), offset(offset) {}
   Numeric(Integer x)
       : value(x.value * types::numericShifts[precision]), offset(x.offset) {}
   Numeric(int64_t x, uint32_t offset) : value(x), offset(offset) {}

   bool operator==(const Numeric<len, precision>& n) const {
      return (value == n.value) && (offset == n.offset);
   }
};

static Integer cast(types::Integer number, uint32_t offset);
template <unsigned len, unsigned precision> static Numeric<len, precision> cast(types::Numeric<len, precision> number,
                                    uint32_t offset);
}; // namespace runtime_types

namespace algebra_types {
struct Integer : public algebra::Type {
   virtual operator std::string() const override;
   virtual const std::string& cppname() const override;
   virtual size_t rt_size() const override;
};
}; // namespace algebra_types

}; // namespace offset
