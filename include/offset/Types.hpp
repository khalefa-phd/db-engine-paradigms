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

/* #define COMMA ,
#define APPLY_TO_OFFSET_TYPE                                                   \
   case Integer: APPLY_TO_PROCEDURE(offset::runtime_types::Integer)            \
   case Numeric_12_2:                                                          \
      APPLY_TO_PROCEDURE(offset::runtime_types::Numeric<12 COMMA 2>)           \
   case Numeric_18_2:                                                          \
      APPLY_TO_PROCEDURE(offset::runtime_types::Numeric<18 COMMA 2>)           \
   case Date: APPLY_TO_PROCEDURE(offset::runtime_types::Date)                  \
   case Char_1: APPLY_TO_PROCEDURE(offset::runtime_types::Char<1>)             \
   case Char_6: APPLY_TO_PROCEDURE(offset::runtime_types::Char<6>)             \
   case Char_7: APPLY_TO_PROCEDURE(offset::runtime_types::Char<7>)             \
   case Char_9: APPLY_TO_PROCEDURE(offset::runtime_types::Char<9>)             \
   case Char_10: APPLY_TO_PROCEDURE(offset::runtime_types::Char<10>)           \
   case Char_11: APPLY_TO_PROCEDURE(offset::runtime_types::Char<11>)           \
   case Char_12: APPLY_TO_PROCEDURE(offset::runtime_types::Char<12>)           \
   case Char_15: APPLY_TO_PROCEDURE(offset::runtime_types::Char<15>)           \
   case Char_18: APPLY_TO_PROCEDURE(offset::runtime_types::Char<18>)           \
   case Char_22: APPLY_TO_PROCEDURE(offset::runtime_types::Char<22>)           \
   case Char_25: APPLY_TO_PROCEDURE(offset::runtime_types::Char<25>)           \
   case Varchar_11: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<11>)     \
   case Varchar_12: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<12>)     \
   case Varchar_22: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<22>)     \
   case Varchar_23: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<23>)     \
   case Varchar_25: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<25>)     \
   case Varchar_40: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<40>)     \
   case Varchar_44: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<44>)     \
   case Varchar_55: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<55>)     \
   case Varchar_79: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<79>)     \
   case Varchar_101: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<101>)   \
   case Varchar_117: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<117>)   \
   case Varchar_152: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<152>)   \
   case Varchar_199: APPLY_TO_PROCEDURE(offset::runtime_types::Varchar<199>) */
