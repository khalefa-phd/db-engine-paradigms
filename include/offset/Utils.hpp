#pragma once

#include <cstddef>
#include <immintrin.h>

namespace offset {
namespace utils {
inline size_t avx2_count_matches(unsigned* firstBegin, unsigned* firstEnd,
                                 unsigned* secondBegin, unsigned* secondEnd);

};
}; // namespace offset