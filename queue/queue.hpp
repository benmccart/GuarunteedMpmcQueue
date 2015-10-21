//  (C) Copyright 2015 Ben McCart
//  Use, modification and distribution are subject to the Boost Software License,
//  Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt).


#ifndef GUARUNTEED_MPMC_QUEUE_HPP
#define GUARUNTEED_MPMC_QUEUE_HPP


#include <atomic>
#include <cassert>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace detail
{
	// Add macro magic here for cache line size depending on hardware ...
	static const size_t cache_line_size = 64;

	// Add macro magic here for the correct number of wait iterations for trailing back/front value before a yeild, this is hardware dependant...
	// TODO: This has little to do with concurrency, and a lot more to do with oversubscription...
	static const uint32_t concurrency = 256;


	// My current compiler doesn't include experimental/optional...
	template <class T>
	class optional
	{
	public:
		optional() : has_value_(false) {}
		optional(optional<T> const &o) : has_value_(o.has_value_)
		{
			if (has_value_)
				new (&storage_) T(o);
		}
		
		optional(optional<T>&& o) : has_value_(std::move(o.has_value_))
		{
			if (has_value_)
				new (&storage_) T(std::move(reinterpret_cast<T&>(o.storage_)));
		}

		optional(T&& t) : has_value_(true)
		{
			new (&storage_) T(std::move(reinterpret_cast<T&>(t)));
		}

		~optional()
		{
			if (has_value_)
				reinterpret_cast<T*>(&storage_)->~T();
		}

		optional<T>& operator=(optional<T> const &o)
		{
			if (has_value_)
				reinterpret_cast<T*>(&storage_)->~();

			has_value_ = o.has_value_;
			if (has_value_)
				new (&storage_) T(reinterpret_cast<T const&>(o.storage_));

			return *this;
		}

		optional<T>& operator=(optional<T> &&o)
		{
			if (has_value_)
				reinterpret_cast<T*>(&storage_)->~T();

			has_value_ = std::move(o.has_value_);
			if (has_value_)
				new (&storage_) T(std::move(reinterpret_cast<T&>(o.storage_)));

			return *this;
		}

		optional<T>& operator=(T &&t)
		{
			if (has_value_)
				reinterpret_cast<T*>(&storage_)->~T();

			has_value_ = true;
			new (&storage_) T(std::move(t));

			return *this;
		}

		operator bool() const
		{
			return has_value_;
		}

		T& get()
		{
			return reinterpret_cast<T&>(storage_);
		}

		T const& get() const
		{
			reinterpret_cast<T&>(o.storage_)
		}

		T const& operator*() const
		{
			return get();
		}

		T& operator*()
		{
			return get();
		}

		T const* operator->() const
		{
			return &get();
		}

		T* operator->()
		{
			return &get();
		}

		T release()
		{
			has_value_ = false;
			return std::move(reinterpret_cast<T&>(storage_));
		}

	private:
		typedef typename std::aligned_storage<sizeof(T), alignof(T)>::type storage_type;
		storage_type storage_;
		bool has_value_;
	};

	template <typename SizeType>
	struct queue_size
	{
	};

	template <>
	struct queue_size<uint32_t>
	{
		typedef int32_t type;
		typedef std::atomic_int32_t atomic_type;
		// The limited capacity is to allow for a reasonable safety range (~ 1 billion) to prevent corruption of state, by overflowing the range (numeric overflow), it is 
		// assumed that there will not be enough threads (more than numeric_limits<int32_t>::max() - max_capacity) to violate this condition.
		static const size_t max_capacity = static_cast<size_t>(static_cast<int32_t>(1) << 30);
		inline static size_t round_up_to_power_of_2(uint32_t c)
		{
			c--;
			c |= c >> 1;
			c |= c >> 2;
			c |= c >> 4;
			c |= c >> 8;
			c |= c >> 16;
			c++;

			return c;
		}
	};

	template <>
	struct queue_size<uint64_t>
	{
		typedef int64_t type;
		typedef std::atomic_int64_t atomic_type;
		// The limited capacity is to allow for a reasonable safety range (~ 4.6 quintillion) to prevent corruption of state, by overflowing the range (numeric overflow), it is 
		// assumed that there will not be enough threads (more than numeric_limits<int32_t>::max() - max_capacity) to violate this condition.
		static const size_t max_capacity = static_cast<size_t>(static_cast<int64_t>(1) << 62);
		inline static size_t round_up_to_power_of_2(uint64_t c)
		{
			c--;
			c |= c >> 1;
			c |= c >> 2;
			c |= c >> 4;
			c |= c >> 8;
			c |= c >> 16;
			c |= c >> 32;
			c++;

			return c;
		}
	};
}


template <class T>
class queue
{
public:

	typedef detail::optional<T> optional_t;

	queue(size_t);

	void push(T&&);
	bool try_push(T&, uint16_t);
	T pop();
	optional_t try_pop(uint16_t);
	
	size_t size() const;
	size_t empty() const;
	size_t capacity() const;

private:
	typedef detail::queue_size<size_t>::type queue_size_t;
	typedef detail::queue_size<size_t>::atomic_type atomic_queue_size_t;

	size_t bounded_index(size_t) const;
	void push_impl(T&&);
	T pop_impl();


	// Tracks the queue size upper bound.  The size upper bound is the number of queue slots either holding a T object, holding a partially formed T object, or reserved (by push operation) to write a T object.
	alignas(detail::cache_line_size) atomic_queue_size_t size_upper_bound_;

	// Tracks the queue size lower bound.  The size lower bound is the number of fully formed T objects in the queue, not reserved (by pop operation) to read a T object.
	alignas(detail::cache_line_size) atomic_queue_size_t size_lower_bound_;

	// The back of the queue is where items are inserted (pushed). back_lead_ is the leading (edge of 'back' of the queue) index where slots in the queue are reserved for writing a T object.
	alignas(detail::cache_line_size) std::atomic_size_t back_lead_;
	
	// The back of the queue is where items are 'pushed'.  back_trail_ is the trailing (edge of 'back' of queue) index where fully formed T objects have been written.
	alignas(detail::cache_line_size) std::atomic_size_t back_trail_;

	// The front of the queue is where items are removed from (poped). front_lead_ is the leading (edge of 'front') index reserved (by pop operation) to read a T object.
	alignas(detail::cache_line_size) std::atomic_size_t front_lead_;

	// The front of the queue is where items are 'poped'.  front_trail_ is the trailing (edge of 'front' of queue) index where T objects are read from.
	alignas(detail::cache_line_size) std::atomic_size_t front_trail_;

	// A buffer sized for holding elements of queue.
	alignas(detail::cache_line_size) std::vector<optional_t> buffer_;
};


template <class T>
queue<T>::queue(size_t capacity) : size_upper_bound_(0), size_lower_bound_(0), back_lead_(0), back_trail_(0), front_lead_(0), front_trail_(0)
{
	// The inc logic for back/front lead/trail edges working correctly depends on buffer_.size() dividing evenly into range of size_t, so that modulus
	// always returns the next valid index in buffer as if it were w ring buffer (it is emulating a ring buffer...)
	capacity = detail::queue_size<size_t>::round_up_to_power_of_2(capacity);
	if (capacity > detail::queue_size<size_t>::max_capacity)
		throw std::invalid_argument("specified capacity is larger than max allowable capacity of queue");
	else if (capacity == 0)
		throw std::invalid_argument("specified capacity is zero - queue must have non zero capacity");

	buffer_.resize(capacity);
}

template <class T>
void queue<T>::push(T&& t)
{
	// Increase queueu upper bound size, wait while there are no completely empty slots in queue.
	for (queue_size_t size = size_upper_bound_.fetch_add(1) + 1; size > static_cast<queue_size_t>(buffer_.size()); size = size_upper_bound_.fetch_add(1) + 1)
	{
		size_upper_bound_.fetch_sub(1); // Back off and retry.
	}

	push_impl(std::move(t));
}

template<class T>
bool queue<T>::try_push(T &t, uint16_t attempts)
{
	// Increase queueu upper bound size, wait while there are no completely empty slots in queue.
	uint16_t attempt = 0;
	for (queue_size_t size = size_upper_bound_.fetch_add(1) + 1; size > static_cast<queue_size_t>(buffer_.size()); size = size_upper_bound_.fetch_add(1) + 1)
	{
		size_upper_bound_.fetch_sub(1); // Back off and retry.
		if (attempt == attempts)
		{
			return false;
		}
		++attempt;
	}

	push_impl(std::move(t));
	return true;
}

template <class T>
T queue<T>::pop()
{
	// Decrease queueu lower bound size, wait while there are no completely filled slots in queue.
	uint16_t attempt = 0;
	for (queue_size_t size = size_lower_bound_.fetch_sub(1) - 1; size < 0; size = size_lower_bound_.fetch_sub(1) - 1)
	{
		size_lower_bound_.fetch_add(1); // Back off and retry.
	}

	return pop_impl();
}

template<class T>
typename queue<T>::optional_t queue<T>::try_pop(uint16_t attempts)
{
	// Decrease queueu lower bound size, wait while there are no completely filled slots in queue.
	optional_t ot;
	uint16_t attempt = 0;
	for (queue_size_t size = size_lower_bound_.fetch_sub(1) - 1; size < 0; size = size_lower_bound_.fetch_sub(1) - 1)
	{
		size_lower_bound_.fetch_add(1); // Back off and retry.
		if (attempt == attempts)
		{
			return ot;
		}
		++attempt;
	}

	return pop_impl();
}

template <class T>
size_t queue<T>::size() const
{
	 return size_upper_bound_;
}

template <class T>
size_t queue<T>::empty() const
{
	return size_lower_bound_ == 0;
}

template <class T>
size_t queue<T>::capacity() const
{
	return buffer_.size();
}

template <class T>
size_t queue<T>::bounded_index(size_t unbounded_index) const
{
	return unbounded_index % buffer_.size();
}

template<class T>
inline void queue<T>::push_impl(T&& t)
{
	// Reserve slot index for insertion.
	size_t safe_index = bounded_index(back_lead_.fetch_add(1));
	assert(safe_index < buffer_.size());
	auto &slot = buffer_[safe_index];

	// Set the value.
	slot = std::move(t);

	// Wait on trailing edge, then inc it.
	for (uint32_t wait_count = 0; bounded_index(back_trail_) != safe_index; ++ wait_count)
	{
		if ((wait_count % detail::concurrency) + 1 == detail::concurrency)
			std::this_thread::yield(); // Deal with oversubscription...
	}
	back_trail_.fetch_add(1);

	// Increment lower bound (no need to check size, it is dependant on that being established previously by check on size upper bound).
	size_lower_bound_.fetch_add(1);
}

template<class T>
inline T queue<T>::pop_impl()
{
	// Reserve slot index for removal.
	size_t safe_index = bounded_index(front_lead_.fetch_add(1));
	assert(safe_index < buffer_.size());
	auto &slot = buffer_[safe_index];

	// Get the value.
	T t{ slot.release() };

	// Wait on trailing edge, then inc it.
	for (uint32_t wait_count = 0; bounded_index(front_trail_) != safe_index; ++wait_count)
	{
		if ((wait_count % detail::concurrency) + 1 == detail::concurrency)
			std::this_thread::yield(); // Deal with oversubscription...
	}
	front_trail_.fetch_add(1);

	// Increment upper bound (no need to check size, it is dependant on that being established previously by check on size lower bound).
	size_upper_bound_.fetch_sub(1);

	return t;
}

#endif // GUARUNTEED_MPMC_QUEUE_HPP
