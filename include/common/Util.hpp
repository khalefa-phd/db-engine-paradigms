#pragma once

#include <cstddef>
#include <functional>
#include <iostream>
#include <tuple>
#include <type_traits>
#include <utility>

template <class F, class... Ts, std::size_t... Is>
void for_each_in_tuple(const std::tuple<Ts...> &tuple, F func,
                       std::index_sequence<Is...>) {
  using expander = int[];
  (void)expander{0, ((void)func(std::get<Is>(tuple)), 0)...};
}

template <class F, class... Ts>
void for_each_in_tuple(const std::tuple<Ts...> &tuple, F func) {
  for_each_in_tuple(tuple, func, std::make_index_sequence<sizeof...(Ts)>());
}

template <class F, class... Ts, std::size_t... Is>
void for_each_in_tuple(const std::tuple<Ts...> &&tuple, F func,
                       std::index_sequence<Is...>) {
  using expander = int[];
  (void)expander{0, ((void)func(std::forward(std::get<Is>(tuple))), 0)...};
}

template <class F, class... Ts>
void for_each_in_tuple(const std::tuple<Ts...> &&tuple, F func) {
  for_each_in_tuple(tuple, func, std::make_index_sequence<sizeof...(Ts)>());
}

template <typename T, std::size_t... I, typename F>
void tuple_foreach_impl(T &&tuple, std::index_sequence<I...>, F &&func) {
  // In C++17 we would use a fold expression here, but in C++14 we have to
  // resort to this.
  using dummy_array = int[];
  dummy_array{(void(func(std::get<I>(tuple))), 0)..., 0};
}

template <typename T, typename F>
void tuple_foreach_(T &&tuple, F &&func) {
  constexpr int size = std::tuple_size<std::remove_reference_t<T>>::value;
  tuple_foreach_impl(std::forward<T>(tuple), std::make_index_sequence<size>{},
                     std::forward<F>(func));
}

template <class... Ts>
void print_tuple(std::tuple<Ts...> &tuple) {
  for_each_in_tuple(tuple, [](auto &&value) { std::cout << value << " , "; });
}

template <class Ts>
void print_tuple(Ts &t) {
  std::cout << t.value << " , ";
}

template <class TupType, size_t... I>
void print(const TupType &_tup, std::index_sequence<I...>) {
  std::cout << "(";
  (..., (std::cout << (I == 0 ? "" : ", ") << std::get<I>(_tup)));
  std::cout << ")\n";
}

template <class... T>
void print(const std::tuple<T...> &_tup) {
  print(_tup, std::make_index_sequence<sizeof...(T)>());
}

template <class T>
void print(const T &t) {
  std::cout << t;  // print(_tup, std::make_index_sequence<sizeof...(T)>());
}

template <class... T>
void print(std::string s, const std::tuple<T...> &_tup) {
  std::cout << s << ":";
  print(_tup, std::make_index_sequence<sizeof...(T)>());
}
