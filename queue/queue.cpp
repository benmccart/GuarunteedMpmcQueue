// queue.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"

#include "queue.hpp"

#include <iomanip>
#include <iostream>
#include <thread>
#include <boost/chrono.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/thread/barrier.hpp>


namespace
{
	static const size_t c_10k = 10000;
	static const size_t c_100k = 100000;
	static const size_t c_million = 1000000;
	static const size_t c_billion = 1000000000;
}

using std::cout;
using std::endl;
using std::move;
using std::thread;
using boost::barrier;


typedef queue<size_t> queue_t;
typedef boost::lockfree::queue<size_t, boost::lockfree::fixed_sized<true>> boost_queue_t;
typedef boost::chrono::duration<double> seconds;
typedef boost::chrono::high_resolution_clock timer;

#define TRY_PUSH_POP__
static const uint16_t attempts = 4;

void consecutive_producer(size_t count, barrier &barrier, queue_t &queue)
{
	barrier.wait();
	for (size_t i = 0; i != count; ++i)
	{
		size_t ip = i;
#if !defined(TRY_PUSH_POP__)
		queue.push(move(ip));
#else
		while (!queue.try_push(ip, attempts))
		{
			std::this_thread::yield();
		}
#endif
	}
}

void boost_consecutive_producer(size_t count, barrier &barrier, boost_queue_t &queue)
{
	barrier.wait();
	for (size_t i = 0; i != count; ++i)
	{
		size_t ip = i;
		while (!queue.push(ip)) {}
	}
}

void consecutive_consumer(size_t count, barrier &barrier, queue_t &queue)
{
	barrier.wait();
	for (size_t i = 0; i != count; ++i)
	{
#if !defined(TRY_PUSH_POP__)
		size_t v = queue.pop();
		assert(v == i);
#else
		queue_t::optional_t v;
		for (v = queue.try_pop(attempts); !static_cast<bool>(v); v = queue.try_pop(attempts))
		{
			std::this_thread::yield();
		}
		assert(*v == i);
#endif
		
	}
}

void boost_consecutive_consumer(size_t count, barrier &barrier, boost_queue_t &queue)
{
	barrier.wait();
	for (size_t i = 0; i != count; ++i)
	{
		size_t v = std::numeric_limits<size_t>::max();
		while (!queue.pop(v)) {}
		assert(v == i);
	}
}

void bounded_consumer(size_t count, size_t bound, barrier &barrier, queue_t &queue)
{
	barrier.wait();
	for (size_t i = 0; i != count; ++i)
	{
#if !defined(TRY_PUSH_POP__)
		size_t v = queue.pop();
		assert(v < i);
#else
		queue_t::optional_t v;
		for (v = queue.try_pop(attempts); !static_cast<bool>(v); v = queue.try_pop(attempts))
		{
			std::this_thread::yield();
		}
		assert(*v < bound);
#endif
	}
}

void boost_bounded_consumer(size_t count, size_t bound, barrier &barrier, boost_queue_t &queue)
{
	barrier.wait();
	for (size_t i = 0; i != count; ++i)
	{
		size_t v = std::numeric_limits<size_t>::max();
		while (!queue.pop(v)) {}
		assert(v < bound);
	}
}

void queue_test(size_t capacity, size_t producer_count, size_t consumer_count, size_t producer_iterations)
{
	queue_t q(capacity);
	barrier b(static_cast<unsigned int>(producer_count + consumer_count + 1));

	std::vector<thread> producers;
	std::vector<thread> consumers;

	size_t total_iterations = producer_count * producer_iterations;
	size_t consumer_iterations = total_iterations / consumer_count;

	for (size_t i = 0; i != producer_count; ++i)
	{
		producers.emplace_back(consecutive_producer, producer_iterations, std::ref(b), std::ref(q));
		
	}
	for (size_t i = 0; i != consumer_count; ++i)
	{
		consumers.emplace_back(bounded_consumer, consumer_iterations, producer_iterations, std::ref(b), std::ref(q));
	}

	b.wait();
	auto t0 = timer::now();
	std::for_each(begin(producers), end(producers), [=](thread &t) -> void
	{
		t.join();
	});
	std::for_each(begin(consumers), end(consumers), [=](thread &t) -> void
	{
		t.join();
	});
	auto t1 = timer::now();
	seconds dur = t1 - t0;
	double rate = static_cast<double>(total_iterations) / dur.count();
	
	cout << "queue size is: " << capacity << " producer count is: " << producer_count << " consumer count is: " << consumer_count << endl;
	cout << "completed " << producer_iterations << " iterations for each producer in " << std::fixed << std::setprecision(5) << dur << " @ " << std::setprecision(1) << rate << " items / second" << endl;
}

void boost_queue_test(size_t capacity, size_t producer_count, size_t consumer_count, size_t producer_iterations)
{
	boost_queue_t q(capacity);
	barrier b(static_cast<unsigned int>(producer_count + consumer_count + 1));

	std::vector<thread> producers;
	std::vector<thread> consumers;

	size_t total_iterations = producer_count * producer_iterations;
	size_t consumer_iterations = total_iterations / consumer_count;

	for (size_t i = 0; i != producer_count; ++i)
	{
		producers.emplace_back(boost_consecutive_producer, producer_iterations, std::ref(b), std::ref(q));

	}
	for (size_t i = 0; i != consumer_count; ++i)
	{
		consumers.emplace_back(boost_bounded_consumer, consumer_iterations, producer_iterations, std::ref(b), std::ref(q));
	}

	b.wait();
	auto t0 = timer::now();
	std::for_each(begin(producers), end(producers), [=](thread &t) -> void
	{
		t.join();
	});
	std::for_each(begin(consumers), end(consumers), [=](thread &t) -> void
	{
		t.join();
	});
	auto t1 = timer::now();
	seconds dur = t1 - t0;
	double rate = static_cast<double>(total_iterations) / dur.count();

	cout << "boost queue size is: " << capacity << " producer count is: " << producer_count << " consumer count is: " << consumer_count << endl;
	cout << "completed " << producer_iterations << " iterations for each producer in " << std::fixed << std::setprecision(5) << dur << " @ " << std::setprecision(1) << rate << " items / second" << endl;
}

void paired_queue_test(size_t capacity, size_t producer_count, size_t consumer_count, size_t producer_iterations)
{

	cout << "\n================================================================================\n" << endl;
	boost_queue_test(capacity, producer_count, consumer_count, producer_iterations);
	cout << "--------------------------------------------------------------------------------" << endl;
	queue_test(capacity, producer_count, consumer_count, producer_iterations);
}


int main()
{
	

#ifdef _M_AMD64
	size_t max_capacity = detail::queue_size<size_t>::max_capacity;
	assert(max_capacity == 4611686018427387904);
#elif _M_IX86
	size_t max_capacity = detail::queue_size<size_t>::max_capacity;
	assert(max_capacity == 1073741824);
#endif


	assert(detail::queue_size<size_t>::round_up_to_power_of_2(2) == 2);
	assert(detail::queue_size<size_t>::round_up_to_power_of_2(3) == 4);
	assert(detail::queue_size<size_t>::round_up_to_power_of_2(4) == 4);
	assert(detail::queue_size<size_t>::round_up_to_power_of_2(5) == 8);
	assert(detail::queue_size<size_t>::round_up_to_power_of_2(1023) == 1024);
	assert(detail::queue_size<size_t>::round_up_to_power_of_2(1024) == 1024);
	assert(detail::queue_size<size_t>::round_up_to_power_of_2(1025) == 2048);

	// Boost sequence test.
	{
		boost_queue_t q(8);
		barrier b(3);

		thread p0(boost_consecutive_producer, c_million, std::ref(b), std::ref(q));
		thread c0(boost_consecutive_consumer, c_million, std::ref(b), std::ref(q));

		b.wait();
		auto t0 = timer::now();
		p0.join();
		c0.join();
		auto t1 = timer::now();
		seconds dur = t1 - t0;
		double rate = static_cast<double>(c_million) / dur.count();
		cout << "boost completed " << c_million << " iterations of consecutive producer/consumer in " << std::fixed << std::setprecision(5) << dur << " @ " << std::setprecision(1) << rate << " items / second" << endl;
	}

	// Sequence test
	{
		queue_t q(8);
		barrier b(3);

		thread p0(consecutive_producer, c_million, std::ref(b), std::ref(q));
		thread c0(consecutive_consumer, c_million, std::ref(b), std::ref(q));

		b.wait();
		auto t0 = timer::now();
		p0.join();
		c0.join();
		auto t1 = timer::now();
		seconds dur = t1 - t0;
		double rate = static_cast<double>(c_million) / dur.count();
		cout << "--------------------------------------------------------------------------------" << endl;
		cout << "completed " << c_million << " iterations of consecutive producer/consumer in " << std::fixed << std::setprecision(5) << dur << " @ " << std::setprecision(1) << rate << " items / second" << endl;
	}

	

	paired_queue_test(4, 2, 2, c_million);
	paired_queue_test(128, 2, 2, c_million);
	paired_queue_test(6, 3, 3, c_million);
	paired_queue_test(128, 3, 3, c_million);
	paired_queue_test(8, 4, 4, c_million);
	paired_queue_test(128, 4, 4, c_million);
	paired_queue_test(16, 8, 8, c_100k);
	paired_queue_test(128, 8, 8, c_100k);
	paired_queue_test(1024, 8, 8, c_10k);
	paired_queue_test(128, 16, 16, c_100k);

	cout << "\n\nCompleted!" << endl;
	::getchar();
    return 0;
}

