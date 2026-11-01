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
  using f_t = std::function< void(void) >;
  explicit ThreadPool(size_t thrs);
  ~ThreadPool() noexcept;
  void submit(f_t func);
private:
  void execute();
  
  std::queue< f_t > tasks_;
  std::mutex mtx_;
  std::condition_variable cv_;
  bool stop_ = false;
  std::vector< std::thread > thrs_;
};

#endif
