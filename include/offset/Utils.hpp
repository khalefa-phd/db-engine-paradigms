#pragma once

#include <cstddef>
#include <immintrin.h>
#include <numeric>

namespace offset {
namespace utils {
size_t avx2_count_matches(unsigned* firstBegin, unsigned* firstEnd,
                                 unsigned* secondBegin, unsigned* secondEnd);
unsigned* avx2_inplace_set_intersection(unsigned* firstBegin,
                                               unsigned* firstEnd,
                                               unsigned* secondBegin,
                                               unsigned* secondEnd);
unsigned avx2_find_one_match(unsigned* firstBegin, unsigned* firstEnd,
                                    unsigned* secondBegin, unsigned* secondEnd);

}; // namespace utils
}; // namespace offset