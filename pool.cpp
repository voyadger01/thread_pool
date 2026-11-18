#include "pool.hpp"

const size_t ThreadPool::batch_size_ = 8;

ThreadPool::ThreadPool(size_t thread_count):
  stopped_(false),
  paused_(false),
  cancel_pending_(false),
  active_tasks_(0)
{
  if (thread_count == 0) {
    thread_count = 1;
  }

  task_queues_.resize(thread_count);
  worker_active_tasks_.resize(thread_count, 0);
  workers_stop_.resize(thread_count, false);
  threads_.reserve(thread_count);

  for (size_t i = 0; i < thread_count; ++i) {
    threads_.emplace_back(&ThreadPool::execute, this, i);
  }
}

void ThreadPool::wait_idle()
{
  std::unique_lock< std::mutex > lock(mutex_);
  idle_condition_.wait(lock, [this]() {
    return queued_tasks_locked() == 0 && active_tasks_ == 0;
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
{
  {
    std::lock_guard< std::mutex > lock(mutex_);
    check_not_worker_thread();

    if (stopped_ && threads_.empty()) {
      return;
    }

    stopped_ = true;
    paused_ = false;
    cancel_pending_ = cancel_pending;

    if (cancel_pending_) {
      for (auto &queue : task_queues_) {
        queue.clear();
      }
      notify_idle_locked();
    }
  }

  condition_.notify_all();

  for (size_t i = 0; i < threads_.size(); ++i) {
    if (threads_[i].joinable()) {
      threads_[i].join();
    }
  }

  {
    std::lock_guard< std::mutex > lock(mutex_);
    threads_.clear();
    task_queues_.clear();
    worker_active_tasks_.clear();
    workers_stop_.clear();
    notify_idle_locked();
  }
}

void ThreadPool::grow_to(size_t count)
{
  {
    std::lock_guard< std::mutex > lock(mutex_);
    if (stopped_) {
      throw std::runtime_error("Pool is stopped");
    }
    check_not_worker_thread();

    if (count <= threads_.size()) {
      return;
    }

    const size_t old_size = threads_.size();
    task_queues_.resize(count);
    worker_active_tasks_.resize(count, 0);
    workers_stop_.resize(count, false);
    threads_.reserve(count);

    for (size_t i = old_size; i < count; ++i) {
      threads_.emplace_back(&ThreadPool::execute, this, i);
    }
  }

  condition_.notify_all();
}

void ThreadPool::shrink_to(size_t count)
{
  if (count == 0) {
    count = 1;
  }

  size_t old_size = 0;
  {
    std::lock_guard< std::mutex > lock(mutex_);
    check_not_worker_thread();
    if (stopped_) {
      throw std::runtime_error("Pool is stopped");
    }

    old_size = threads_.size();
    if (count >= old_size) {
      return;
    }

    for (size_t i = count; i < old_size; ++i) {
      workers_stop_[i] = true;
    }

    for (size_t i = count; i < old_size; ++i) {
      while (!task_queues_[i].empty()) {
        const size_t target = select_worker_locked();
        task_queues_[target].emplace_back(std::move(task_queues_[i].front()));
        task_queues_[i].pop_front();
      }
    }
  }

  condition_.notify_all();

  for (size_t i = count; i < old_size; ++i) {
    if (threads_[i].joinable()) {
      threads_[i].join();
    }
  }

  {
    std::lock_guard< std::mutex > lock(mutex_);
    threads_.resize(count);
    task_queues_.resize(count);
    worker_active_tasks_.resize(count);
    workers_stop_.resize(count);
    notify_idle_locked();
  }
}

void ThreadPool::resize(size_t count)
{
  if (count == 0) {
    count = 1;
  }

  const size_t current_size = size();
  if (count > current_size) {
    grow_to(count);
  } else if (count < current_size) {
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

void ThreadPool::execute(size_t id)
{
  for (;;) {
    std::vector< Task > batch;
    {
      std::unique_lock< std::mutex > lock(mutex_);
      condition_.wait(lock, [this, id]() {
        return stopped_ || workers_stop_[id] || (!paused_ && queued_tasks_locked() != 0);
      });

      if (workers_stop_[id] || (stopped_ && cancel_pending_)) {
        return;
      }

      if (!paused_) {
        take_batch_locked(id, batch);
      }

      if (batch.empty()) {
        if (stopped_ && queued_tasks_locked() == 0) {
          return;
        }
        continue;
      }
      active_tasks_ += batch.size();
      worker_active_tasks_[id] += batch.size();
    }

    for (auto &task : batch) {
      try {
        task();
      } catch (...) {
      }

      {
        std::lock_guard< std::mutex > lock(mutex_);
        --active_tasks_;
        --worker_active_tasks_[id];
        notify_idle_locked();
      }
    }
  }
}

size_t ThreadPool::select_worker_locked() const
{
  size_t selected = threads_.size();
  size_t smallest_load = static_cast< size_t >(-1);

  for (size_t i = 0; i < threads_.size(); ++i) {
    if (workers_stop_[i]) {
      continue;
    }

    const size_t load = task_queues_[i].size() + worker_active_tasks_[i];
    if (load < smallest_load) {
      smallest_load = load;
      selected = i;
    }
  }

  if (selected == threads_.size()) {
    throw std::runtime_error("Thread pool has no available workers");
  }
  return selected;
}

size_t ThreadPool::queued_tasks_locked() const
{
  size_t count = 0;
  for (const auto &queue : task_queues_) {
    count += queue.size();
  }
  return count;
}

bool ThreadPool::take_batch_locked(size_t worker_id, std::vector< Task > &batch)
{
  size_t source_id = worker_id;
  bool steal = false;

  if (task_queues_[worker_id].empty()) {
    size_t victim = threads_.size();
    size_t largest_queue = 0;

    for (size_t offset = 1; offset < threads_.size(); ++offset) {
      const size_t candidate = (worker_id + offset) % threads_.size();

      if (workers_stop_[candidate]) {
        continue;
      }
      if (task_queues_[candidate].size() > largest_queue) {
        largest_queue = task_queues_[candidate].size();
        victim = candidate;
      }
    }

    if (victim == threads_.size()) {
      return false;
    }

    source_id = victim;
    steal = true;
  }

  std::deque< Task > &source = task_queues_[source_id];

  size_t amount = source.size();

  if (steal) {
    amount = (amount + 1) / 2;
  }

  if (amount > batch_size_) {
    amount = batch_size_;
  }

  batch.reserve(amount);

  for (size_t i = 0; i < amount; ++i) {
    if (steal) {
      batch.emplace_back(std::move(source.front()));
      source.pop_front();
    } else {
      batch.emplace_back(std::move(source.back()));
      source.pop_back();
    }
  }

  return !batch.empty();
}

void ThreadPool::notify_idle_locked()
{
  if (queued_tasks_locked() == 0 && active_tasks_ == 0) {
    idle_condition_.notify_all();
  }
}

void ThreadPool::check_not_worker_thread() const
{
  const std::thread::id current = std::this_thread::get_id();
  for (const auto &thread : threads_) {
    if (thread.joinable() && thread.get_id() == current) {
      throw std::logic_error("This operation cannot be called from a pool task");
    }
  }
}
