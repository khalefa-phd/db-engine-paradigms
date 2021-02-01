#include <immintrin.h>

inline __m256i _mm256_cmplt_epi32(__m256i a, __m256i b) {
   return _mm256_cmpgt_epi32(b, a);
}

inline size_t avx2_count_matches(unsigned* firstBegin, unsigned* firstEnd,
                            unsigned* secondBegin, unsigned* secondEnd,
                            unsigned limitStart, unsigned limitEnd) {
   size_t counter = 0;
   __m256i a_rep;
   __m256i b;

   a_rep = _mm256_set1_epi32(*firstBegin);
   b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(secondBegin));

   while (firstBegin < firstEnd && secondBegin < secondEnd) {
      const __m256i lt = _mm256_cmplt_epi32(b, a_rep);
      const auto mask = _mm256_movemask_epi8(lt);
      if (mask == 0xffff) {
         // all elements in second vector are smaller, so fetch the next chunk
         secondBegin += 8;
         b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(secondBegin));
      } else {
         // there might be an element equal to the element pointed to by the first vector iterator, so
         // do a simple linear search, as there're only 8 elements to search in
         for (size_t i = 0; i < 8; i++) {
            if (*(secondBegin + i) == *firstBegin) {
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