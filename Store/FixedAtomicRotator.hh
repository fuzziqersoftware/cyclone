#pragma once

#include <stdint.h>

#include <vector>


template <typename T>
class FixedAtomicRotator {
public:
  FixedAtomicRotator() = delete;
  explicit FixedAtomicRotator(size_t num_items);
  FixedAtomicRotator(const FixedAtomicRotator&) = delete;
  FixedAtomicRotator(FixedAtomicRotator&&) = delete;
  ~FixedAtomicRotator() = default;
  FixedAtomicRotator& operator=(const FixedAtomicRotator&) = delete;

  T& operator[](size_t which);
  const T& operator[](size_t which) const;
  void rotate();

private:
  std::vector<T> items;
  std::atomic<size_t> current_index;
};

#include "FixedAtomicRotator-inl.hh"
