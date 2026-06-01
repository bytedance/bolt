#pragma once

#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace bytedance::bolt::memory::bm::compress {

template <typename Context>
class CompressionContextPool {
 public:
  class Ref {
   public:
    Ref() = default;

    Ref(std::unique_ptr<Context> context, CompressionContextPool* pool)
        : context_(std::move(context)), pool_(pool) {}

    ~Ref() {
      reset();
    }

    Ref(const Ref&) = delete;
    Ref& operator=(const Ref&) = delete;

    Ref(Ref&& other) noexcept
        : context_(std::move(other.context_)), pool_(other.pool_) {
      other.pool_ = nullptr;
    }

    Ref& operator=(Ref&& other) noexcept {
      if (this != &other) {
        reset();
        context_ = std::move(other.context_);
        pool_ = other.pool_;
        other.pool_ = nullptr;
      }
      return *this;
    }

    Context* get() const {
      return context_.get();
    }

   private:
    void reset() noexcept {
      if (context_ != nullptr && pool_ != nullptr) {
        pool_->Release(std::move(context_));
      }
    }

    std::unique_ptr<Context> context_;
    CompressionContextPool* pool_{nullptr};
  };

  Ref Acquire() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (idle_.empty()) {
      return Ref{std::make_unique<Context>(), this};
    }
    auto context = std::move(idle_.back());
    idle_.pop_back();
    return Ref{std::move(context), this};
  }

 private:
  void Release(std::unique_ptr<Context> context) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    idle_.push_back(std::move(context));
  }

  std::mutex mutex_;
  std::vector<std::unique_ptr<Context>> idle_;
};

} // namespace bytedance::bolt::memory::bm::compress
