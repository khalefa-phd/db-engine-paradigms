#pragma once

#include "common/algebra/Types.hpp"
#include "common/runtime/Types.hpp"

namespace runtime_types = types;

namespace offset {

namespace types {

struct ContainerCmp {
   bool operator()(const runtime_types::Integer& lhs,
                   const runtime_types::Integer& rhs) const {
      return lhs == rhs;
   }
   template <unsigned len, unsigned precision>
   bool operator()(const runtime_types::Numeric<len, precision>& lhs,
                   const runtime_types::Numeric<len, precision>& rhs) const {
      return lhs == rhs;
   }
};

class Integer : public runtime_types::Integer {
 public:
   int32_t value;
   uint32_t offset;

   Integer() {}
   Integer(runtime_types::Integer value, uint32_t offset)
       : value(value.value), offset(offset) {}

   inline bool operator==(const Integer& n) const {
      return (value == n.value) && (offset == n.offset);
   }
};

template <unsigned len, unsigned precision>
class Numeric : public runtime_types::Numeric<len, precision> {
 public:
   int64_t value;
   uint32_t offset;
   Numeric() : value(0), offset(NULL) {}
   Numeric(runtime_types::Numeric<len, precision> value)
       : value(value.value), offset(NULL) {}
   Numeric(runtime_types::Numeric<len, precision> value, uint32_t offset)
       : value(value.value), offset(offset) {}
   Numeric(runtime_types::Integer x, uint32_t offset)
       : value(x.value * runtime_types::numericShifts[precision]),
         offset(offset) {}
   Numeric(Integer x)
       : value(x.value * runtime_types::numericShifts[precision]),
         offset(x.offset) {}
   Numeric(int64_t x, uint32_t offset) : value(x), offset(offset) {}

   bool operator==(const Numeric<len, precision>& n) const {
      return (value == n.value) && (offset == n.offset);
   }
};

class Date : public runtime_types::Date {
 public:
   int32_t value;
   uint32_t offset;

   Date(runtime_types::Date value) : value(value.value), offset(NULL) {}
   Date(runtime_types::Date value, uint32_t offset)
       : value(value.value), offset(offset) {}
   Date(int32_t value, uint32_t offset) : value(value), offset(offset) {}

   inline bool operator==(const Date& n) const {
      return (value == n.value) && (offset == n.offset);
   }
};

template <unsigned maxLen> class Char : runtime_types::Char<maxLen> {
 public:
   uint32_t offset;

   Char(runtime_types::Char<maxLen> value, uint32_t offset) : offset(offset) {
      this->build(value.value);
   };
};

template <> class Char<1> : public runtime_types::Char<1> {
 public:
   /// The value
   char value;
   uint32_t offset;

   Char(runtime_types::Char<1> value, uint32_t offset)
       : value(value.value), offset(offset) {}
};

template <unsigned maxLen> class Varchar : runtime_types::Varchar<maxLen> {
 public:
   uint32_t offset;

   Varchar(runtime_types::Varchar<maxLen> value, uint32_t offset)
       : offset(offset) {
      this->build(value.value);
   }
};

}; // namespace types

}; // namespace offset
