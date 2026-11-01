// Copyright 2026 BigBrotr contributors.
// Licensed under the MIT license in the repository root.
// Component controls for the actual archive-installed listener, not running DBs.
#include "db/c_options_internal.h"
#include "rocksdb/io_status.h"
#include "rocksdb/listener.h"

#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <initializer_list>
#include <memory>
#include <new>
#include <string_view>
#include <unistd.h>

#if !defined(NDEBUG) || NDEBUG != 1
#error "Match the selected Cargo release archive: NDEBUG=1 is required"
#endif
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
#error "Do not change Status layout relative to the selected native archive"
#endif
#ifdef ROCKSDB_LITE
#error "The selected native archive is not ROCKSDB_LITE"
#endif
#ifndef __cpp_exceptions
#error "The selected native listener uses C++ exception containment"
#endif
#define GUARD_STRING_LITERAL(x) #x
#define GUARD_STRING(x) GUARD_STRING_LITERAL(x)
static_assert(__cplusplus == 202002L, "Use the selected C++20 profile");
static_assert(std::string_view(GUARD_STRING(ROCKSDB_NAMESPACE)) == "rocksdb",
              "Use the selected default RocksDB namespace");

namespace {
using ROCKSDB_NAMESPACE::BackgroundErrorReason;
using ROCKSDB_NAMESPACE::EventListener;
using ROCKSDB_NAMESPACE::IOStatus;
using ROCKSDB_NAMESPACE::Status;
constexpr char kMessage[] = "guard callback allocation payload";
constexpr int kFailure = 64;
constexpr int kExpectedTermination = 86;
constexpr int kWrongTermination = 87;

[[noreturn]] void fail(const char* expression, int line) noexcept {
  std::fprintf(stderr, "CHECK failed at line %d: %s\n", line, expression);
  std::_Exit(kFailure);
}
#define CHECK(expression) \
  ((expression) ? static_cast<void>(0) : fail(#expression, __LINE__))

// Trivial TLS bookkeeping never allocates. Only the test executable replaces
// ordinary array allocation; the production library has no injection switch.
struct AllocationGate {
  bool active = false;
  bool fail_once = false;
  bool injected = false;
  bool wrong_size = false;
  bool unarmed_after_injection = false;
  std::size_t expected = 0;
  std::size_t observed = 0;
  std::size_t matched = 0;
};
constinit thread_local AllocationGate allocation{};
constinit thread_local rocksdb_bigbrotr_background_error_guard_t*
    termination_guard = nullptr;

void arm(std::size_t expected, bool fail_once) {
  CHECK(!allocation.active);
  allocation = AllocationGate{};
  allocation.expected = expected;
  allocation.fail_once = fail_once;
  allocation.active = true;
}
AllocationGate disarm() noexcept {
  allocation.active = false;
  return allocation;
}

void* allocate_array(std::size_t size) {
  if (!allocation.active && allocation.injected) {
    allocation.unarmed_after_injection = true;
  }
  if (allocation.active) {
    ++allocation.observed;
    if (size == allocation.expected) {
      ++allocation.matched;
      if (allocation.fail_once) {
        allocation.injected = true;
        allocation.active = false;
        throw std::bad_alloc();
      }
    } else {
      allocation.wrong_size = true;
      if (allocation.fail_once) {
        allocation.active = false;
        throw std::bad_alloc();
      }
    }
  }
  // Preserve ordinary new[] zero-size, alignment and new-handler semantics.
  // Over-aligned array overloads remain the standard library's matching pair.
  for (;;) {
    if (void* result = std::malloc(size == 0 ? 1 : size)) {
      return result;
    }
    std::new_handler handler = std::get_new_handler();
    if (handler == nullptr) {
      throw std::bad_alloc();
    }
    handler();
  }
}

void raw_write(const char* data, std::size_t size) noexcept {
  while (size != 0) {
    const ssize_t written = ::write(STDOUT_FILENO, data, size);
    if (written < 0 && errno == EINTR) {
      continue;
    }
    if (written <= 0) {
      std::_Exit(kWrongTermination);
    }
    data += written;
    size -= static_cast<std::size_t>(written);
  }
}

void expected_termination() noexcept {
  if (allocation.active || !allocation.fail_once || !allocation.injected ||
      allocation.wrong_size || allocation.unarmed_after_injection ||
      allocation.observed != 1 || allocation.matched != 1 ||
      termination_guard == nullptr ||
      !rocksdb_bigbrotr_background_error_guard_failed(termination_guard)) {
    std::_Exit(kWrongTermination);
  }
  constexpr char message[] = "PHASE allocation-termination injected-and-latched\n";
  raw_write(message, sizeof(message) - 1);
  std::_Exit(kExpectedTermination);
}

using Options =
    std::unique_ptr<rocksdb_options_t, decltype(&rocksdb_options_destroy)>;
using Guard = std::unique_ptr<rocksdb_bigbrotr_background_error_guard_t,
                              decltype(&rocksdb_bigbrotr_background_error_guard_destroy)>;
struct Installed {
  Options options{rocksdb_options_create(), &rocksdb_options_destroy};
  Guard guard{nullptr, &rocksdb_bigbrotr_background_error_guard_destroy};
  std::shared_ptr<EventListener> listener;
  Installed() {
    CHECK(options != nullptr);
    guard.reset(rocksdb_bigbrotr_options_install_background_error_guard(options.get()));
    CHECK(guard != nullptr);
    CHECK(options->rep.max_bgerror_resume_count == 0);
    CHECK(options->rep.listeners.size() == 1);
    listener = options->rep.listeners.front();
    CHECK(listener != nullptr);
    CHECK(!rocksdb_bigbrotr_background_error_guard_failed(guard.get()));
  }
  bool failed() const {
    return rocksdb_bigbrotr_background_error_guard_failed(guard.get()) != 0;
  }
};

void expect_status(const Status& status, Status::Code code, Status::SubCode subcode,
                   Status::Severity severity, const char* message) {
  CHECK(status.code() == code);
  CHECK(status.subcode() == subcode);
  CHECK(status.severity() == severity);
  if (message == nullptr) {
    CHECK(status.getState() == nullptr);
  } else {
    CHECK(status.getState() != nullptr);
    CHECK(std::strcmp(status.getState(), message) == 0);
  }
}

void no_space_severity(Status::Severity input, Status::Severity expected) {
  Installed owner;
  Status status = Status::NoSpace(kMessage);
  status = Status(status, input);  // Prepare the input before any allocation gate.
  owner.listener->OnBackgroundError(BackgroundErrorReason::kCompaction, &status);
  expect_status(status, Status::kIOError, Status::kNoSpace, expected, kMessage);
  CHECK(owner.failed());
}
void no_error() { no_space_severity(Status::kNoError, Status::kFatalError); }
void soft() { no_space_severity(Status::kSoftError, Status::kFatalError); }
void hard() { no_space_severity(Status::kHardError, Status::kFatalError); }
void fatal() { no_space_severity(Status::kFatalError, Status::kFatalError); }
void unrecoverable() {
  no_space_severity(Status::kUnrecoverableError, Status::kUnrecoverableError);
}

void okay() {
  Installed owner;
  Status status = Status::OK();
  owner.listener->OnBackgroundError(BackgroundErrorReason::kCompaction, &status);
  expect_status(status, Status::kOk, Status::kNone, Status::kNoError, nullptr);
  CHECK(!owner.failed());
}
void different_error() {
  Installed owner;
  Status status = Status::Corruption("independent corruption message");
  owner.listener->OnBackgroundError(BackgroundErrorReason::kFlush, &status);
  expect_status(status, Status::kCorruption, Status::kNone, Status::kFatalError,
                "independent corruption message");
  CHECK(owner.failed());
}
void io_flags() {
  for (bool retryable : {false, true}) {
    for (bool data_loss : {false, true}) {
      for (IOStatus::IOErrorScope scope : {IOStatus::kIOErrorScopeFileSystem,
                                         IOStatus::kIOErrorScopeFile,
                                         IOStatus::kIOErrorScopeRange}) {
        Installed owner;
        IOStatus io = IOStatus::NoSpace("independent IO flags");
        io.SetRetryable(retryable);
        io.SetDataLoss(data_loss);
        io.SetScope(scope);
        // Ordinary public derived-to-base reference, no layout cast.
        Status& base = io;
        base = Status(base, Status::kHardError);
        owner.listener->OnBackgroundError(BackgroundErrorReason::kFlush, &base);
        expect_status(base, Status::kIOError, Status::kNoSpace, Status::kFatalError,
                      "independent IO flags");
        CHECK(io.GetRetryable() == retryable);
        CHECK(io.GetDataLoss() == data_loss);
        CHECK(io.GetScope() == scope);
        CHECK(owner.failed());
      }
    }
  }
}
void recovery_veto() {
  for (bool initial : {true, false}) {
    Installed owner;
    bool recovery = initial;
    owner.listener->OnErrorRecoveryBegin(BackgroundErrorReason::kCompaction,
                                       Status::NoSpace("recovery input"),
                                       &recovery);
    CHECK(!recovery);
    CHECK(owner.failed());
    Status status = Status::NoSpace();
    owner.listener->OnBackgroundError(BackgroundErrorReason::kFlush, &status);
    recovery = true;
    owner.listener->OnErrorRecoveryBegin(BackgroundErrorReason::kFlush,
                                       Status::NoSpace(), &recovery);
    CHECK(!recovery);
    CHECK(owner.failed());
  }
}
void sticky_lifetime() {
  Installed owner;
  Options copied{rocksdb_options_create_copy(owner.options.get()),
                 &rocksdb_options_destroy};
  CHECK(copied != nullptr && copied->rep.listeners.size() == 1);
  CHECK(copied->rep.listeners.front() == owner.listener);
  owner.options.reset();
  copied.reset();
  CHECK(!owner.failed());
  Status status = Status::OK();
  owner.listener->OnBackgroundError(BackgroundErrorReason::kCompaction, &status);
  CHECK(!owner.failed());
  status = Status::NoSpace("after options destruction");
  owner.listener->OnBackgroundError(BackgroundErrorReason::kCompaction, &status);
  expect_status(status, Status::kIOError, Status::kNoSpace, Status::kFatalError,
                "after options destruction");
  CHECK(owner.failed());
  status = Status::OK();
  owner.listener->OnBackgroundError(BackgroundErrorReason::kCompaction, &status);
  CHECK(owner.failed());
  status = Status::Corruption("later error");
  owner.listener->OnBackgroundError(BackgroundErrorReason::kFlush, &status);
  CHECK(owner.failed());
  bool recovery = true;
  owner.listener->OnErrorRecoveryBegin(BackgroundErrorReason::kFlush,
                                       Status::NoSpace(), &recovery);
  CHECK(!recovery && owner.failed());
  owner.listener.reset();
  CHECK(owner.failed());  // Query handle independently retains the sticky state.
}
void guard_drop_first() {
  Installed owner;
  owner.guard.reset();
  owner.options.reset();
  Status status = Status::NoSpace("after guard destruction");
  owner.listener->OnBackgroundError(BackgroundErrorReason::kCompaction, &status);
  expect_status(status, Status::kIOError, Status::kNoSpace, Status::kFatalError,
                "after guard destruction");
  // No query through the destroyed guard. The actual listener owns its state.
}
void allocation_positive() {
  Installed owner;
  Status status = Status::NoSpace(kMessage);
  status = Status(status, Status::kSoftError);
  arm(sizeof(kMessage), false);
  owner.listener->OnBackgroundError(BackgroundErrorReason::kCompaction, &status);
  const AllocationGate observed = disarm();
  CHECK(observed.observed == 1 && observed.matched == 1 && !observed.wrong_size);
  CHECK(!observed.injected);
  expect_status(status, Status::kIOError, Status::kNoSpace, Status::kFatalError, kMessage);
  CHECK(owner.failed());
}
void allocation_no_copy() {
  for (Status::Severity severity : {Status::kNoError, Status::kFatalError,
                                   Status::kUnrecoverableError}) {
    Installed owner;
    Status status = severity == Status::kNoError ? Status::NoSpace()
                                                : Status::NoSpace(kMessage);
    status = Status(status, severity);
    arm(0, false);  // No array allocation of any size is acceptable here.
    owner.listener->OnBackgroundError(BackgroundErrorReason::kCompaction, &status);
    const AllocationGate observed = disarm();
    CHECK(observed.observed == 0 && !observed.wrong_size && !observed.injected);
    expect_status(status, Status::kIOError, Status::kNoSpace,
                  severity == Status::kNoError ? Status::kFatalError : severity,
                  severity == Status::kNoError ? nullptr : kMessage);
    CHECK(owner.failed());
  }
}
void allocation_termination() {
  Installed owner;
  Status status = Status::NoSpace(kMessage);
  status = Status(status, Status::kHardError);
  termination_guard = owner.guard.get();
  std::set_terminate(expected_termination);
  constexpr char phase[] = "PHASE allocation-termination prearmed\n";
  raw_write(phase, sizeof(phase) - 1);  // Reporting is prepared before arming.
  arm(sizeof(kMessage), true);
  try {
    owner.listener->OnBackgroundError(BackgroundErrorReason::kCompaction, &status);
  } catch (...) {
    disarm();
    fail("exception escaped callback", __LINE__);
  }
  disarm();
  fail("callback returned after injected allocation failure", __LINE__);
}
struct Case {
  const char* name;
  void (*run)();
  int expected_exit;
};
constexpr Case kCases[] = {
    {"nospace-no-error", no_error, 0}, {"nospace-soft", soft, 0},
    {"nospace-hard", hard, 0}, {"nospace-fatal", fatal, 0},
    {"nospace-unrecoverable", unrecoverable, 0}, {"okay", okay, 0},
    {"different-error", different_error, 0}, {"io-flags", io_flags, 0},
    {"recovery-veto", recovery_veto, 0}, {"sticky-lifetime", sticky_lifetime, 0},
    {"guard-drop-first", guard_drop_first, 0},
    {"allocation-positive", allocation_positive, 0},
    {"allocation-no-copy", allocation_no_copy, 0},
    {"allocation-termination", allocation_termination, kExpectedTermination},
};
}  // namespace

void* operator new[](std::size_t size) { return allocate_array(size); }
void operator delete[](void* pointer) noexcept { std::free(pointer); }
void operator delete[](void* pointer, std::size_t) noexcept { std::free(pointer); }
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
  try {
    return allocate_array(size);
  } catch (...) {
    return nullptr;
  }
}
void operator delete[](void* pointer, const std::nothrow_t&) noexcept {
  std::free(pointer);
}

int main(int argc, char** argv) {
  try {
    if (argc == 2 && std::strcmp(argv[1], "--identity") == 0) {
      std::puts("GUARD_CALLBACK_PROFILE\tcplusplus=202002\tNDEBUG=1"
                "\tstatus_checked=0\tnamespace=rocksdb");
      return 0;
    }
    if (argc == 2 && std::strcmp(argv[1], "--list") == 0) {
      for (const Case& item : kCases) {
        std::printf("%s\t%d\n", item.name, item.expected_exit);
      }
      return 0;
    }
    if (argc == 3 && std::strcmp(argv[1], "--case") == 0) {
      for (const Case& item : kCases) {
        if (std::strcmp(argv[2], item.name) == 0) {
          item.run();
          CHECK(item.expected_exit == 0);
          std::printf("PASS %s\n", item.name);
          return 0;
        }
      }
    }
    fail("expected --identity, --list or --case NAME", __LINE__);
  } catch (...) {
    disarm();
    fail("unexpected exception outside allocation termination control", __LINE__);
  }
}
