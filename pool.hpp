#ifndef POOL_HPP
#define POOL_HPP

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
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
  auto submit(F func) -> std::future< decltype(func()) >
  {
    using Result = decltype(func());
    auto task = std::make_shared< std::packaged_task< Result > >(std::move(func()));
    std::future< Result > result = task->get_future();
    {
      std::lock_guard< std::mutex > lock(mutex_);
      if (stopped_) {
        throw std::runtime_error("Thread pool is stopped");
      }
      tasks_.push([task]() {
        (*task)();
      });
      condition_.notify_one();
      return result;
    }
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
  void execute(size_t id);

  std::queue< Task > tasks_;
  std::mutex mutex_;
  std::condition_variable condition_;
  bool stopped_;
  bool paused_;
  bool cancel_pending_;
  size_t active_tasks_;
  std::vector< std::thread > thrs_;
};

#endif
