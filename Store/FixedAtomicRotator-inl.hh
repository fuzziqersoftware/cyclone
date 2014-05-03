#include <stdexcept>


template <typename T>
FixedAtomicRotator<T>::FixedAtomicRotator(size_t num_items) : items(num_items + 1),
    current_index(0) { }

template <typename T>
T& FixedAtomicRotator<T>::operator[](size_t which) {
  if (which >= this->items.size() - 1) {
    throw std::out_of_range(std::to_string(which));
  }

  size_t current = this->current_index % this->items.size();
  if (which > current) {
    return this->items[current + this->items.size() - which];
  } else {
    return this->items[current - which];
  }
}

template <typename T>
const T& FixedAtomicRotator<T>::operator[](size_t which) const {
  if (which >= this->items.size() - 1) {
    throw std::out_of_range(std::to_string(which));
  }

  size_t current = this->current_index % this->items.size();
  if (which > current) {
    return this->items[current + this->items.size() - which];
  } else {
    return this->items[current - which];
  }
}

template <typename T>
void FixedAtomicRotator<T>::rotate() {
  size_t next = (this->current_index + 1) % this->items.size();

  this->items[next] = T();
  this->current_index++;
}
