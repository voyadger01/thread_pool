#ifndef POOL_HPP
#define POOL_HPP

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

class ThreadPool
{
public:
  using Task = std::function< void(void) >;
  explicit ThreadPool(size_t thread_count);

  template< class F >
  auto submit(F &&func) -> std::future< decltype(func()) >
  {
    using Result = decltype(func());
    auto task = std::make_shared< std::packaged_task< Result() > >(std::forward< F >(func));
    std::future< Result > result = task->get_future();
    {
      std::lock_guard< std::mutex > lock(mutex_);
      if (stopped_) {
        throw std::runtime_error("Thread pool is stopped");
      }
      if (threads_.empty()) {
        throw std::runtime_error("Thread pool has no workers");
      }
      const size_t worker_id = select_worker_locked();
      task_queues_[worker_id].emplace_back([task]() {
        (*task)();
      });
    }

    condition_.notify_all();
    return result;
  }

  void wait_idle();
  void pause();
  void resume();
  void stop(bool cancel_pending = false);
  void grow_to(size_t count);
  void shrink_to(size_t count);
  void resize(size_t count);
  size_t size() const;

  void lock();
  bool try_lock();
  void unlock();

  ~ThreadPool() noexcept;

private:
  static const size_t batch_size_;

  void execute(size_t id);
  void check_not_worker_thread() const;
  size_t select_worker_locked() const;
  size_t queued_tasks_locked() const;
  bool take_batch_locked(size_t worker_id, std::vector< Task > &batch);
  void notify_idle_locked();

  std::vector< std::deque< Task > > task_queues_;
  std::vector< size_t > worker_active_tasks_;

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::condition_variable idle_condition_;
  bool stopped_;
  bool paused_;
  bool cancel_pending_;
  size_t active_tasks_;
  std::vector< std::thread > threads_;
  std::vector< bool > workers_stop_;
  std::mutex user_mutex_;
};

#endif
