#include "pool.hpp"

ThreadPool::ThreadPool(size_t thread_count):
  stopped_(false),
  paused_(false),
  cancel_pending_(false),
  active_tasks_(0)
{
  if (thread_count == 0) {
    thread_count = 1;
  }

  workers_stop_.resize(thread_count, false);

  for (size_t i = 0; i < thread_count; ++i) {
    threads_.emplace_back(&ThreadPool::execute, this, i);
  }
}

void ThreadPool::wait_idle()
{
  std::unique_lock< std::mutex > lock(mutex_);
  idle_condition_.wait(lock, [this]() {
    return tasks_.empty() && active_tasks_ == 0;
  });
}

void ThreadPool::pause()
{
  std::lock_guard< std::mutex > lock(mutex_);
  if (!stopped_) {
    paused_ = true;
  }
}

void ThreadPool::resume()
{
  {
    std::lock_guard< std::mutex > lock(mutex_);
    paused_ = false;
  }
  condition_.notify_all();
}

void ThreadPool::stop(bool cancel_pending)
{}

void ThreadPool::grow_to(size_t count)
{}

void ThreadPool::shrink_to(size_t count)
{}

void ThreadPool::resize(size_t count)
{
  if (count == 0) {
    count = 1;
  }

  if (count > size()) {
    grow_to(count);
  } else {
    shrink_to(count);
  }
}

size_t ThreadPool::size() const
{
  std::lock_guard< std::mutex > lock(mutex_);
  return threads_.size();
}

void ThreadPool::lock()
{
  user_mutex_.lock();
}

bool ThreadPool::try_lock()
{
  return user_mutex_.try_lock();
}

void ThreadPool::unlock()
{
  user_mutex_.unlock();
}

ThreadPool::~ThreadPool() noexcept
{
  try {
    stop();
  } catch (...) {
  }
}
