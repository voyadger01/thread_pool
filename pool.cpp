#include "pool.hpp"

ThreadPool::ThreadPool(size_t thread_count)
{}

void ThreadPool::wait_idle()
{}

void ThreadPool::pause()
{}

void ThreadPool::resume()
{}

void ThreadPool::stop(bool cancel_pending)
{}

void ThreadPool::grow_to(size_t count)
{}

void ThreadPool::shrink_to(size_t count)
{}

void ThreadPool::resize(size_t count)
{}

size_t ThreadPool::size() const
{}

void ThreadPool::lock()
{}

bool ThreadPool::try_lock()
{}

void ThreadPool::unlock()
{}

ThreadPool::~ThreadPool() noexcept
{}
