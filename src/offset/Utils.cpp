#include "offset/Utils.hpp"

inline __m256i _mm256_cmplt_epi32(__m256i a, __m256i b) {
   return _mm256_cmpgt_epi32(b, a);
}

/**
 * Counts matching elements between two ranges.
 *
 * If size of ranges differ, then the smaller range must be passed first.
 * Elements must be sorted in ascending order.
 *
 * @param firstBegin iterator to first element in smaller range
 * @param firstEnd iterator to final element in smaller range
 * @param secondBegin iterator to first element in larger range
 * @param secondEnd iterator to final element in larger range
 * @return count of matching elements between ranges
 */
inline size_t offset::utils::avx2_count_matches(unsigned* firstBegin,
                                                unsigned* firstEnd,
                                                unsigned* secondBegin,
                                                unsigned* secondEnd) {
   size_t counter = 0;
   __m256i a_rep = _mm256_set1_epi32(*firstBegin);
   __m256i b =
       _mm256_loadu_si256(reinterpret_cast<const __m256i*>(secondBegin));

   while (firstBegin < firstEnd && secondBegin < secondEnd) {
      const __m256i lt = _mm256_cmplt_epi32(b, a_rep);
      const auto mask = _mm256_movemask_epi8(lt);
      if (mask == 0xffff) {
         // all elements in second vector are smaller, so fetch the next chunk
         secondBegin += 8;
         b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(secondBegin));
      } else {
         // there might be an element equal to the element pointed to by the
         // first vector iterator, so do a simple linear search, as there're
         // only 8 elements to search in
         for (size_t i = 0; i < 8; i++) {
            auto possibleMatch = secondBegin + i;
            if ((possibleMatch < secondEnd) &&
                (*possibleMatch == *firstBegin)) {
               counter++;
               break;
            }
         }

         // fetch the next value from the first vector's iterator
         firstBegin++;
         a_rep = _mm256_set1_epi32(*firstBegin);
      }
   }
   return counter;
}

inline unsigned* offset::utils::avx2_inplace_set_intersection(
    unsigned* firstBegin, unsigned* firstEnd, unsigned* secondBegin,
    unsigned* secondEnd) {
   __m256i a_rep;
   __m256i b;

   a_rep = _mm256_set1_epi32(*firstBegin);
   b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(secondBegin));
   auto intersectionEnd = firstBegin;

   while (firstBegin < firstEnd && secondBegin < secondEnd) {

      const __m256i lt = _mm256_cmplt_epi32(b, a_rep);
      const auto mask = _mm256_movemask_epi8(lt);
      if (mask == 0xffff) {
         secondBegin += 8;
         b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(secondBegin));
      } else {
         for (size_t i = 0; i < 8; i++) {
            auto possibleMatch = secondBegin + i;
            if ((possibleMatch < secondEnd) &&
                (*possibleMatch == *firstBegin)) {
               *intersectionEnd = *firstBegin;
               intersectionEnd++;
               break;
            }
         }

         // fetch the next value from A
         firstBegin++;
         a_rep = _mm256_set1_epi32(*firstBegin);
      }
   }
   return intersectionEnd;
}