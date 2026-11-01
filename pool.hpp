#ifndef POOL_HPP
#define POOL_HPP

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool
{
public:
  using Task = std::function< void(void) >;
  explicit ThreadPool(size_t thread_count);

  template < class F >
  auto submit(F func);

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
  std::mutex mtx_;
  std::condition_variable cv_;
  bool stop_ = false;
  std::vector< std::thread > thrs_;
};

#endif
