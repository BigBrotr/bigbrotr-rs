// Copyright 2026 BigBrotr contributors.
// Licensed under the MIT license in the repository root.
// Linux tmpfs/default-POSIX component experiment; no production fault switches.
#include "db/c_options_internal.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"
#include "rocksdb/write_batch.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <utility>

#if !defined(__linux__) || !defined(NDEBUG) || NDEBUG != 1
#error "Use the selected Linux release archive with NDEBUG=1"
#endif
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
#error "Do not change Status layout relative to the selected native archive"
#endif
#ifdef ROCKSDB_LITE
#error "The selected native archive is not ROCKSDB_LITE"
#endif
#ifndef __cpp_exceptions
#error "Match the selected exception-enabled native archive"
#endif
#define POSIX_GUARD_STRING_LITERAL(x) #x
#define POSIX_GUARD_STRING(x) POSIX_GUARD_STRING_LITERAL(x)
static_assert(__cplusplus == 202002L);
static_assert(std::string_view(POSIX_GUARD_STRING(ROCKSDB_NAMESPACE)) == "rocksdb");

namespace {
using namespace ROCKSDB_NAMESPACE;
using Clock = std::chrono::steady_clock;
constexpr uint64_t kMiB = 1024 * 1024;
constexpr uint64_t kMountBytes = 128 * kMiB;
constexpr uint64_t kMinFree = 64 * kMiB;
constexpr uint64_t kMaxInputBytes = 16 * kMiB;
constexpr size_t kWriteBuffer = 256 * 1024;
constexpr size_t kKeys = 1024;
constexpr size_t kValueBytes = 1024;
constexpr size_t kRecords = 1024;
constexpr size_t kFiles = 8;
constexpr char kFatalResume[] =
    "DB resume requested but failed due to Fatal/Unrecoverable error";
constexpr const char* kCases[] = {
    "full_guard", "retry0_only", "promotion_only", "recovery_veto_only"};

void require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

// All callback observations use preallocated slots. In particular the two error
// callbacks take no mutex, perform no I/O and never wait for the controller.
struct StatusRecord {
  bool present = false;
  int code = 0, subcode = 0, severity = 0;
  bool null_message = true;
  char message[1024]{};
};
struct Record {
  std::atomic<bool> ready{false};
  char kind[32]{};
  uint64_t tid = 0;
  int phase = 0, job = -1, reason = -1, operation = -1;
  bool flag_before = false, flag_after = false;
  bool latch_before = false, latch_after = false;
  bool retryable = false, data_loss = false, manual = false, remote = false;
  int scope = -1, base_level = -1, output_level = -1;
  int flush_scheduled = -1, flush_running = -1;
  int compaction_scheduled = -1, compaction_running = -1;
  uint64_t bytes = 0, extra = 0, input_records = 0, output_records = 0;
  uint64_t input_bytes = 0, output_bytes = 0, trivial_files = 0;
  uint64_t inputs[kFiles]{}, outputs[kFiles]{};
  size_t input_count = 0, output_count = 0;
  char path[384]{}, text[2048]{};
  StatusRecord before, after;
};

class Journal {
 public:
  std::atomic<int> phase{0};
  std::atomic<bool> invalid{false};
  std::condition_variable changed;
  std::mutex wait_mutex;

  template <size_t N>
  void copy(char (&target)[N], const char* source) noexcept {
    if (source == nullptr) return;
    const size_t length = std::strlen(source);
    if (length >= N) invalid.store(true, std::memory_order_release);
    std::memcpy(target, source, std::min(length, N - 1));
  }
  void status(StatusRecord& target, const Status& source) noexcept {
    target.present = true;
    target.code = source.code();
    target.subcode = source.subcode();
    target.severity = source.severity();
    target.null_message = source.getState() == nullptr;
    copy(target.message, source.getState());
  }
  Record* begin(const char* kind) noexcept {
    const size_t index = count_.fetch_add(1, std::memory_order_relaxed);
    if (index >= kRecords) {
      invalid.store(true, std::memory_order_release);
      changed.notify_all();
      return nullptr;
    }
    Record& record = records_[index];
    copy(record.kind, kind);
    record.tid = static_cast<uint64_t>(::syscall(SYS_gettid));
    record.phase = phase.load(std::memory_order_acquire);
    return &record;
  }
  void publish(Record* record) noexcept {
    if (record) record->ready.store(true, std::memory_order_release);
    changed.notify_all();
  }
  template <class Predicate>
  const Record* find(Predicate predicate) const noexcept {
    const size_t count = std::min(count_.load(std::memory_order_acquire), kRecords);
    for (size_t i = 0; i < count; ++i) {
      const Record& record = records_[i];
      if (record.ready.load(std::memory_order_acquire) && predicate(record))
        return &record;
    }
    return nullptr;
  }
  size_t count_kind(const char* kind) const noexcept {
    size_t count = 0;
    find([&](const Record& record) {
      if (std::strcmp(record.kind, kind) == 0) ++count;
      return false;
    });
    return count;
  }
  static void string(const char* value) {
    std::putchar('"');
    for (const unsigned char* p = reinterpret_cast<const unsigned char*>(value);
         *p != 0; ++p) {
      if (*p == '"' || *p == '\\') std::printf("\\%c", *p);
      else if (*p < 32 || *p >= 127) std::printf("\\u%04x", *p);
      else std::putchar(*p);
    }
    std::putchar('"');
  }
  static void dump_status(const StatusRecord& status) {
    if (!status.present) { std::printf("null"); return; }
    std::printf("{\"code\":%d,\"subcode\":%d,\"severity\":%d,\"message\":",
                status.code, status.subcode, status.severity);
    if (status.null_message) std::printf("null");
    else string(status.message);
    std::putchar('}');
  }
  void drain() {
    const size_t count = std::min(count_.load(std::memory_order_acquire), kRecords);
    while (drained_ < count && records_[drained_].ready.load(std::memory_order_acquire)) {
      const Record& r = records_[drained_];
      std::printf("{\"seq\":%zu,\"kind\":", drained_++);
      string(r.kind);
      std::printf(" ,\"tid\":%llu,\"phase\":%d,\"job\":%d,\"reason\":%d,\"operation\":%d,"
                  "\"flag_before\":%d,\"flag_after\":%d,\"latch_before\":%d,\"latch_after\":%d,"
                  "\"retryable\":%d,\"data_loss\":%d,\"scope\":%d,\"manual\":%d,\"remote\":%d,"
                  "\"base_level\":%d,\"output_level\":%d,\"bytes\":%llu,\"extra\":%llu,"
                  "\"input_records\":%llu,\"output_records\":%llu,\"input_bytes\":%llu,"
                  "\"output_bytes\":%llu,\"trivial_files\":%llu,\"path\":",
                  (unsigned long long)r.tid, r.phase, r.job, r.reason, r.operation,
                  r.flag_before, r.flag_after, r.latch_before, r.latch_after,
                  r.retryable, r.data_loss, r.scope, r.manual, r.remote,
                  r.base_level, r.output_level, (unsigned long long)r.bytes,
                  (unsigned long long)r.extra, (unsigned long long)r.input_records,
                  (unsigned long long)r.output_records, (unsigned long long)r.input_bytes,
                  (unsigned long long)r.output_bytes, (unsigned long long)r.trivial_files);
      string(r.path);
      std::printf(",\"text\":"); string(r.text);
      std::printf(",\"before\":"); dump_status(r.before);
      std::printf(",\"after\":"); dump_status(r.after);
      std::printf(",\"flush_scheduled\":%d,\"flush_running\":%d,"
                  "\"compaction_scheduled\":%d,\"compaction_running\":%d",
                  r.flush_scheduled, r.flush_running, r.compaction_scheduled, r.compaction_running);
      std::printf(",\"inputs\":[");
      for (size_t i = 0; i < r.input_count; ++i)
        std::printf("%s%llu", i ? "," : "", (unsigned long long)r.inputs[i]);
      std::printf("],\"outputs\":[");
      for (size_t i = 0; i < r.output_count; ++i)
        std::printf("%s%llu", i ? "," : "", (unsigned long long)r.outputs[i]);
      std::printf("]}\n");
    }
    require(std::fflush(stdout) == 0, "outside-mount evidence write failed");
  }
  template <class Predicate>
  void wait(Predicate predicate, Clock::time_point deadline) {
    for (;;) {
      drain();
      require(!invalid.load(std::memory_order_acquire), "observation overflow/truncation");
      if (predicate()) return;
      require(Clock::now() < deadline, "positive witness deadline expired");
      // This short bounded wait only waits for a positive event. Silence is
      // never an oracle. The periodic predicate check also avoids lost notify.
      std::unique_lock<std::mutex> lock(wait_mutex);
      changed.wait_until(lock, std::min(deadline, Clock::now() + std::chrono::milliseconds(10)));
    }
  }
 private:
  std::array<Record, kRecords> records_{};
  std::atomic<size_t> count_{0};
  size_t drained_ = 0;
};

class BoundedLogger final : public Logger {
 public:
  explicit BoundedLogger(Journal& journal) : journal_(journal) {}
  std::atomic<uint64_t> omitted{0};
  void Logv(const char* format, va_list args) noexcept override {
    // The selected native witness has no format arguments. Inspecting the
    // format first avoids formatting/capturing the large informational dump.
    const bool fatal = std::strstr(format, kFatalResume) != nullptr;
    const bool space_wait = std::strstr(format, "Cannot clear soft error") != nullptr;
    const bool selected = fatal || space_wait || std::strstr(format, "ErrorHandler:") != nullptr ||
        std::strstr(format, "Background IO error") != nullptr ||
        std::strstr(format, "Compaction error:") != nullptr ||
        std::strstr(format, "Waiting after background compaction error:") != nullptr;
    if (!selected) { omitted.fetch_add(1, std::memory_order_relaxed); return; }
    Record* r = journal_.begin(fatal ? "fatal_resume" : space_wait ? "sfm_space_wait" : "native_log");
    if (r) {
      const int length = std::vsnprintf(r->text, sizeof(r->text), format, args);
      if (length < 0 || static_cast<size_t>(length) >= sizeof(r->text))
        journal_.invalid.store(true, std::memory_order_release);
    }
    journal_.publish(r);
  }
 private:
  Journal& journal_;
};

class Observer final : public EventListener {
 public:
  Observer(Journal& journal, std::shared_ptr<EventListener> installed,
           rocksdb_bigbrotr_background_error_guard_t* guard, std::string mode,
           Clock::time_point deadline)
      : journal_(journal), installed_(std::move(installed)), guard_(guard),
        mode_(std::move(mode)), deadline_(deadline) {}
  std::atomic<bool> select_fault{false};
  std::atomic<bool> gated{false};
  std::atomic<int> fault_job{-1};
  std::array<std::string, kFiles> fault_paths;
  size_t fault_path_count = 0;
  std::mutex gate_mutex;
  std::condition_variable gate_changed;
  bool released = false;

  bool failed() const noexcept {
    return rocksdb_bigbrotr_background_error_guard_failed(guard_) != 0;
  }
  void release() {
    std::lock_guard<std::mutex> lock(gate_mutex);
    released = true;
    gate_changed.notify_all();
  }
  void OnBackgroundError(BackgroundErrorReason reason, Status* status) noexcept override {
    Record* r = journal_.begin("background_error");
    if (r) {
      r->reason = static_cast<int>(reason);
      r->latch_before = failed();
      journal_.status(r->before, *status);
    }
    if (mode_ == "full_guard" || mode_ == "promotion_only")
      installed_->OnBackgroundError(reason, status);
    if (r) {
      journal_.status(r->after, *status);
      r->latch_after = failed();
    }
    journal_.publish(r);
  }
  void OnErrorRecoveryBegin(BackgroundErrorReason reason, Status status,
                            bool* recovery) noexcept override {
    Record* r = journal_.begin("recovery_begin");
    if (r) {
      r->reason = static_cast<int>(reason);
      r->flag_before = *recovery;
      r->latch_before = failed();
      journal_.status(r->before, status);
    }
    if (mode_ == "full_guard" || mode_ == "recovery_veto_only")
      installed_->OnErrorRecoveryBegin(reason, status, recovery);
    if (r) {
      r->flag_after = *recovery;
      r->latch_after = failed();
      journal_.status(r->after, status);
    }
    journal_.publish(r);
  }
  bool ShouldBeNotifiedOnFileIO() noexcept override { return true; }
  void OnIOError(const IOErrorInfo& info) noexcept override {
    Record* r = journal_.begin("io_error");
    if (r) {
      journal_.status(r->before, info.io_status);
      r->operation = static_cast<int>(info.operation);
      r->retryable = info.io_status.GetRetryable();
      r->data_loss = info.io_status.GetDataLoss();
      r->scope = static_cast<int>(info.io_status.GetScope());
      r->bytes = info.length; r->extra = info.offset;
      journal_.copy(r->path, info.file_path.c_str());
    }
    journal_.publish(r);
  }
  void OnErrorRecoveryEnd(const BackgroundErrorRecoveryInfo& info) noexcept override {
    Record* r = journal_.begin("recovery_end");
    if (r) {
      journal_.status(r->before, info.old_bg_error);
      journal_.status(r->after, info.new_bg_error);
    }
    journal_.publish(r);
  }
  void OnFlushBegin(DB*, const FlushJobInfo& info) noexcept override { flush("flush_begin", info); }
  void OnFlushCompleted(DB*, const FlushJobInfo& info) noexcept override { flush("flush_end", info); }
  void OnCompactionBegin(DB*, const CompactionJobInfo& info) noexcept override {
    compaction("compaction_begin", info);
    if (!select_fault.exchange(false, std::memory_order_acq_rel)) return;
    // Input copying here is part of the explicitly permitted test-only gate,
    // never either error callback. No DB methods are called from this callback.
    if (info.input_files.size() > kFiles) {
      journal_.invalid.store(true, std::memory_order_release);
      return;
    }
    fault_path_count = info.input_files.size();
    for (size_t i = 0; i < fault_path_count; ++i) fault_paths[i] = info.input_files[i];
    fault_job.store(info.job_id, std::memory_order_release);
    Record* r = journal_.begin("fault_gate");
    if (r) r->job = info.job_id;
    journal_.publish(r);
    gated.store(true, std::memory_order_release);
    journal_.changed.notify_all();
    std::unique_lock<std::mutex> lock(gate_mutex);
    if (!gate_changed.wait_until(lock, deadline_, [&] { return released; })) {
      journal_.invalid.store(true, std::memory_order_release);
      journal_.publish(journal_.begin("gate_timeout"));
    }
  }
  void OnCompactionCompleted(DB*, const CompactionJobInfo& info) noexcept override {
    compaction("compaction_end", info);
  }
  void OnTableFileCreationStarted(const TableFileCreationBriefInfo& info) noexcept override {
    Record* r = journal_.begin("table_start");
    if (r) {
      r->job = info.job_id; r->reason = static_cast<int>(info.reason);
      journal_.copy(r->path, info.file_path.c_str());
    }
    journal_.publish(r);
  }
  void OnTableFileCreated(const TableFileCreationInfo& info) noexcept override {
    Record* r = journal_.begin("table_created");
    if (r) {
      r->job = info.job_id; r->reason = static_cast<int>(info.reason);
      r->bytes = info.file_size;
      journal_.copy(r->path, info.file_path.c_str());
      journal_.status(r->before, info.status);
    }
    journal_.publish(r);
  }
  void OnTableFileDeleted(const TableFileDeletionInfo& info) noexcept override {
    Record* r = journal_.begin("table_deleted");
    if (r) {
      r->job = info.job_id;
      journal_.copy(r->path, info.file_path.c_str());
      journal_.status(r->before, info.status);
    }
    journal_.publish(r);
  }
  void OnBackgroundJobPressureChanged(DB*, const BackgroundJobPressure& pressure) noexcept override {
    Record* r = journal_.begin("job_pressure");
    if (r) {
      r->flush_scheduled = pressure.flush_scheduled;
      r->flush_running = pressure.flush_running;
      r->compaction_scheduled = pressure.compaction_scheduled;
      r->compaction_running = pressure.compaction_running;
    }
    journal_.publish(r);
  }
 private:
  void flush(const char* kind, const FlushJobInfo& info) {
    Record* r = journal_.begin(kind);
    if (r) {
      r->job = info.job_id; r->reason = static_cast<int>(info.flush_reason);
      r->bytes = info.file_number;
      journal_.copy(r->path, info.file_path.c_str());
    }
    journal_.publish(r);
  }
  void compaction(const char* kind, const CompactionJobInfo& info) {
    Record* r = journal_.begin(kind);
    if (r) {
      r->job = info.job_id; r->reason = static_cast<int>(info.compaction_reason);
      r->base_level = info.base_input_level; r->output_level = info.output_level;
      r->manual = info.stats.is_manual_compaction;
      r->remote = info.stats.is_remote_compaction;
      r->input_records = info.stats.num_input_records;
      r->output_records = info.stats.num_output_records;
      r->input_bytes = info.stats.total_input_bytes;
      r->output_bytes = info.stats.total_output_bytes;
      r->trivial_files = info.stats.num_input_files_trivially_moved;
      journal_.status(r->before, info.status);
      if (info.input_file_infos.size() > kFiles || info.output_file_infos.size() > kFiles)
        journal_.invalid.store(true, std::memory_order_release);
      r->input_count = std::min(info.input_file_infos.size(), kFiles);
      r->output_count = std::min(info.output_file_infos.size(), kFiles);
      for (size_t i = 0; i < r->input_count; ++i) r->inputs[i] = info.input_file_infos[i].file_number;
      for (size_t i = 0; i < r->output_count; ++i) r->outputs[i] = info.output_file_infos[i].file_number;
    }
    journal_.publish(r);
  }
  Journal& journal_;
  const std::shared_ptr<EventListener> installed_;
  rocksdb_bigbrotr_background_error_guard_t* guard_;
  const std::string mode_;
  const Clock::time_point deadline_;
};

bool kind(const Record& record, const char* name) {
  return std::strcmp(record.kind, name) == 0;
}

uint64_t free_space(const std::string& root, Journal& journal, const char* phase) {
  struct statvfs info{};
  struct statfs fs{};
  require(::statvfs(root.c_str(), &info) == 0 && ::statfs(root.c_str(), &fs) == 0,
          "cannot inspect dedicated filesystem");
  require(fs.f_type == 0x01021994 && info.f_frsize == 4096 &&
          info.f_blocks * info.f_frsize == kMountBytes, "expected exact 128 MiB Linux tmpfs");
  Record* r = journal.begin(phase);
  if (r) {
    r->bytes = info.f_bavail * info.f_frsize;
    r->extra = info.f_blocks * info.f_frsize;
    r->input_bytes = info.f_bfree * info.f_frsize;
    r->output_bytes = info.f_frsize;
  }
  journal.publish(r);
  return info.f_bavail * info.f_frsize;
}

std::string key(size_t index) {
  char result[32];
  std::snprintf(result, sizeof(result), "key-%08zu", index);
  return result;
}
std::string value(unsigned generation, size_t index) {
  uint32_t state = (generation * 0x9e3779b9U) ^ static_cast<uint32_t>(index + 1);
  std::string result(kValueBytes, '\0');
  for (char& byte : result) {
    state ^= state << 13; state ^= state >> 17; state ^= state << 5;
    byte = static_cast<char>(state & 255);
  }
  return result;
}
void write_generation(DB& db, Journal& journal, unsigned generation) {
  WriteBatch batch;
  for (size_t i = 0; i < kKeys; ++i) batch.Put(key(i), value(generation, i));
  WriteOptions write;
  write.sync = true; write.disableWAL = false;
  Status status = db.Write(write, &batch);
  Record* r = journal.begin("accepted_generation");
  if (r) { r->bytes = generation; r->extra = kKeys; journal.status(r->before, status); }
  journal.publish(r);
  require(status.ok(), "preparation synced generation failed");
}
void kick(DB& db, Journal& journal, const char* text) {
  WriteOptions write;
  write.sync = true; write.disableWAL = false;
  Status status = db.Put(write, "kick", text);
  Record* r = journal.begin("accepted_kick");
  if (r) { journal.copy(r->text, text); journal.status(r->before, status); }
  journal.publish(r);
  require(status.ok(), "preparation synced kick failed");
}
void check_data(DB& db, Journal& journal, unsigned generation, const char* kick_value) {
  uint64_t digest = 14695981039346656037ULL;
  auto hash = [&](const std::string& bytes) {
    for (unsigned char byte : bytes) { digest ^= byte; digest *= 1099511628211ULL; }
  };
  std::unique_ptr<Iterator> iterator(db.NewIterator(ReadOptions()));
  size_t count = 0;
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    const std::string expected_key = count < kKeys ? key(count) : "kick";
    const std::string expected_value = count < kKeys ? value(generation, count) : kick_value;
    require(count <= kKeys && iterator->key().ToString() == expected_key &&
            iterator->value().ToString() == expected_value, "accepted scientific data differs");
    hash(expected_key); hash(expected_value); ++count;
  }
  require(iterator->status().ok() && count == kKeys + 1, "accepted data census differs");
  Record* r = journal.begin("data_checked");
  if (r) { r->bytes = count; r->extra = digest; r->input_bytes = generation; }
  journal.publish(r);
}

int experiment(const char* name, const std::string& root) {
  const auto deadline = Clock::now() + std::chrono::seconds(25);
  auto journal = std::make_unique<Journal>();
  auto logger = std::make_shared<BoundedLogger>(*journal);
  std::unique_ptr<rocksdb_options_t, decltype(&rocksdb_options_destroy)>
      options(rocksdb_options_create(), &rocksdb_options_destroy);
  std::unique_ptr<rocksdb_bigbrotr_background_error_guard_t,
                  decltype(&rocksdb_bigbrotr_background_error_guard_destroy)>
      guard(nullptr, &rocksdb_bigbrotr_background_error_guard_destroy);
  std::shared_ptr<Observer> observer;
  std::unique_ptr<DB> db;
  const std::string filler_path = root + "/owned-filler";
  int filler = -1;
  bool allocated = false;
  bool passed = false;
  try {
    require(options != nullptr && root.size() < 200, "invalid options or oversized fixture path");
    guard.reset(rocksdb_bigbrotr_options_install_background_error_guard(options.get()));
    require(guard && options->rep.listeners.size() == 1 &&
            options->rep.max_bgerror_resume_count == 0, "actual guard installation failed");
    const auto installed = options->rep.listeners.front();
    observer = std::make_shared<Observer>(*journal, installed, guard.get(), name, deadline);
    options->rep.listeners = {observer};
    Options& o = options->rep;
    o.create_if_missing = true; o.error_if_exists = true; o.paranoid_checks = true;
    o.use_fsync = true; o.info_log = logger;
    o.write_buffer_size = kWriteBuffer; o.max_write_buffer_number = 4;
    o.min_write_buffer_number_to_merge = 1;
    o.max_background_jobs = 2; o.max_background_compactions = 1; o.max_background_flushes = 1;
    o.max_subcompactions = 1; o.max_open_files = 64;
    o.max_total_wal_size = 16 * kMiB;
    o.compression = kNoCompression; o.bottommost_compression = kNoCompression;
    o.compaction_style = kCompactionStyleLevel; o.num_levels = 3;
    o.level_compaction_dynamic_level_bytes = false;
    o.level0_file_num_compaction_trigger = 2;
    o.level0_slowdown_writes_trigger = 20; o.level0_stop_writes_trigger = 24;
    o.target_file_size_base = 4 * kMiB; o.max_bytes_for_level_base = 64 * kMiB;
    o.ttl = 0; o.periodic_compaction_seconds = 0;
    require(o.env == Env::Default() && !o.sst_file_manager && o.db_paths.empty() &&
            !o.disable_auto_compactions && !o.allow_mmap_writes && !o.allow_mmap_reads &&
            !o.manual_wal_flush && !o.compaction_service && !o.avoid_unnecessary_blocking_io,
            "default native path changed");
    struct stat root_stat{}, filler_stat{};
    require(::stat(root.c_str(), &root_stat) == 0, "case root absent");
    filler = ::open(filler_path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    require(filler >= 0 && ::fstat(filler, &filler_stat) == 0 && S_ISREG(filler_stat.st_mode) &&
            filler_stat.st_nlink == 1 && filler_stat.st_dev == root_stat.st_dev &&
            filler_stat.st_uid == ::getuid(), "owned filler creation/identity failed");
    Record* identity = journal->begin("identity");
    if (identity) {
      std::snprintf(identity->text, sizeof(identity->text),
                    "installed=%p observer=%p env=%p uid=%u gid=%u dev=%llu inode=%llu",
                    static_cast<void*>(installed.get()), static_cast<void*>(observer.get()),
                    static_cast<void*>(o.env), filler_stat.st_uid,
                    filler_stat.st_gid, (unsigned long long)filler_stat.st_dev,
                    (unsigned long long)filler_stat.st_ino);
      identity->bytes = o.max_bgerror_resume_count;
      journal->copy(identity->path, filler_path.c_str());
    }
    journal->publish(identity);
    require(free_space(root, *journal, "mount_before") >= kMinFree, "insufficient frozen initial headroom");
    Status open_status = DB::Open(o, root + "/db", &db);
    Record* opened_record = journal->begin("opened");
    if (opened_record) journal->status(opened_record->before, open_status);
    journal->publish(opened_record);
    require(open_status.ok() && db, "fresh default-POSIX open failed");
    const DBOptions actual = db->GetDBOptions();
    require(actual.env == Env::Default() && actual.sst_file_manager &&
            actual.listeners.size() == 1 && actual.listeners.front() == observer &&
            actual.max_bgerror_resume_count == 0, "actual open options/observer differ");

    // Each oversized atomic batch occupies one memtable. Waiting for the first
    // automatic flush before the final tiny write prevents flush coalescing.
    // The kick is submitted before the second L0 file can admit compaction.
    auto prepare = [&](unsigned first, const char* kick_value, size_t first_flush) {
      write_generation(*db, *journal, first);
      write_generation(*db, *journal, first + 1);
      journal->wait([&] { return journal->count_kind("flush_end") >= first_flush; }, deadline);
      kick(*db, *journal, kick_value);
    };
    prepare(1, "healthy", 1);
    journal->wait([&] {
      return journal->find([](const Record& r) {
        return kind(r, "compaction_end") && r.before.code == Status::kOk &&
               r.input_records > 0 && r.output_records > 0 && r.output_bytes > 0 &&
               r.trivial_files == 0 && r.input_count >= 2;
      }) != nullptr;
    }, deadline);
    WaitForCompactOptions wait;
    wait.flush = false; wait.close_db = false;
    wait.timeout = std::chrono::duration_cast<std::chrono::microseconds>(deadline - Clock::now());
    require(wait.timeout.count() > 0 && db->WaitForCompact(wait).ok(), "healthy maintenance did not finish");
    require(journal->count_kind("flush_end") == 2 && journal->count_kind("background_error") == 0 &&
            !observer->failed(), "healthy automatic phase invalid");
    check_data(*db, *journal, 2, "healthy");
    journal->publish(journal->begin("healthy_complete"));
    journal->phase.store(1, std::memory_order_release);
    observer->select_fault.store(true, std::memory_order_release);
    prepare(3, "fault", 3);
    journal->wait([&] {
      return observer->gated.load(std::memory_order_acquire) &&
             journal->count_kind("flush_end") == 4 &&
             journal->find([](const Record& r) {
               return kind(r, "job_pressure") && r.phase == 1 && r.flush_scheduled == 0 &&
                      r.flush_running == 0 && r.compaction_scheduled == 1;
             }) != nullptr;
    }, deadline);
    // NotifyOnCompactionBegin releases the DB mutex. The separate flush worker
    // and the final foreground call can finish while this one worker is gated.
    // BackgroundCallFlush publishes pressure only after its synchronous obsolete
    // WAL cleanup, so that tail cannot free pages after the filler reaches ENOSPC.
    journal->publish(journal->begin("submission_stopped"));
    check_data(*db, *journal, 4, "fault");
    uint64_t input_bytes = 0;
    require(observer->fault_path_count >= 2, "selected job has insufficient overlapping inputs");
    for (size_t i = 0; i < observer->fault_path_count; ++i) {
      struct stat info{};
      require(observer->fault_paths[i].starts_with(root + "/db/") &&
              ::lstat(observer->fault_paths[i].c_str(), &info) == 0 && S_ISREG(info.st_mode) &&
              info.st_dev == root_stat.st_dev, "cannot measure admitted input SST");
      input_bytes += info.st_size;
    }
    require(input_bytes > 0 && input_bytes <= kMaxInputBytes, "admitted inputs exceed frozen fixture ceiling");
    Record* reservation = journal->begin("admitted_inputs");
    if (reservation) { reservation->job = observer->fault_job.load(); reservation->bytes = input_bytes;
                       reservation->extra = 2 * input_bytes + kWriteBuffer; }
    journal->publish(reservation);
    require(free_space(root, *journal, "mount_prefill") >= kMinFree, "insufficient prefill headroom");
    std::array<char, kMiB> pages{};
    uint64_t written = 0, short_writes = 0;
    for (;;) {
      require(Clock::now() < deadline && written <= kMountBytes, "filler bound expired");
      const ssize_t count = ::write(filler, pages.data(), pages.size());
      if (count < 0 && errno == EINTR) continue;
      if (count < 0) { require(errno == ENOSPC, "filler failed without ENOSPC"); break; }
      require(count > 0, "zero filler write");
      allocated = true;
      written += static_cast<uint64_t>(count);
      if (static_cast<size_t>(count) < pages.size()) ++short_writes;
    }
    require(::fstat(filler, &filler_stat) == 0, "cannot inspect allocated filler");
    Record* filled = journal->begin("filler_enospc");
    if (filled) { filled->bytes = written; filled->extra = filler_stat.st_blocks * 512;
                  filled->operation = ENOSPC; filled->input_bytes = short_writes; }
    journal->publish(filled);
    require(written > 0 && free_space(root, *journal, "mount_full") == 0, "filler did not exhaust real pages");
    journal->drain();
    observer->release();
    journal->wait([&] {
      return journal->count_kind("background_error") >= 1 &&
             journal->count_kind("recovery_begin") >= 1 && journal->count_kind("io_error") >= 1 &&
             (std::strcmp(name, "retry0_only") != 0 || journal->count_kind("sfm_space_wait") >= 1);
    }, deadline);
    // Required error records are published before freeing the only owned filler.
    require(::close(filler) == 0, "filler close failed"); filler = -1;
    require(::unlink(filler_path.c_str()) == 0, "owned filler unlink failed"); allocated = false;
    require(free_space(root, *journal, "mount_restored") >= kMinFree, "restored free headroom insufficient");
    if (std::strcmp(name, "retry0_only") == 0 || std::strcmp(name, "recovery_veto_only") == 0) {
      journal->wait([&] {
        const bool progress = journal->find([&](const Record& r) {
          return kind(r, "compaction_end") && r.job != observer->fault_job.load() && r.phase == 1 &&
                 r.before.code == Status::kOk && r.output_bytes > 0 && r.output_records > 0;
        }) != nullptr;
        return progress && (std::strcmp(name, "retry0_only") != 0 ||
               journal->find([](const Record& r) {
                 return kind(r, "recovery_end") && r.before.subcode == Status::kNoSpace &&
                        r.after.code == Status::kOk;
               }) != nullptr);
      }, deadline);
    } else if (std::strcmp(name, "promotion_only") == 0) {
      journal->wait([&] { return journal->count_kind("fatal_resume") >= 1; }, deadline);
    }
    // Error callbacks publish before ErrorHandler reacquires its DB mutex and
    // commits bg_error_. Let the native query observe that commit; a tiny probe
    // timeout could race it and yield TimedOut instead of the required status.
    wait.timeout = std::chrono::duration_cast<std::chrono::microseconds>(deadline - Clock::now());
    require(wait.timeout.count() > 0, "terminal query deadline expired");
    const Status terminal = db->WaitForCompact(wait);
    Record* terminal_record = journal->begin("terminal_query");
    if (terminal_record) { journal->status(terminal_record->before, terminal);
                          terminal_record->latch_after = observer->failed(); }
    journal->publish(terminal_record);
    check_data(*db, *journal, 4, "fault");
    passed = true;
  } catch (const std::exception& error) {
    Record* r = journal->begin("failure");
    if (r) journal->copy(r->text, error.what());
    journal->publish(r);
  }
  // Close/destruction is the producer/recovery join, unlike WaitForCompact.
  // Keep the query handle and forwarding listener alive until it has completed.
  if (filler >= 0) { ::close(filler); filler = -1; }
  if (allocated) {
    if (::unlink(filler_path.c_str()) != 0) passed = false;
  }
  if (observer) observer->release();
  journal->publish(journal->begin("close_begin"));
  journal->drain();
  if (db) {
    const Status closed = db->Close();
    Record* r = journal->begin("close_return");
    if (r) journal->status(r->before, closed);
    journal->publish(r);
    if (!closed.ok()) passed = false;
    db.reset();
  }
  Record* destroyed = journal->begin("destroyed");
  if (destroyed) destroyed->latch_after = guard && rocksdb_bigbrotr_background_error_guard_failed(guard.get());
  journal->publish(destroyed);
  Record* final = journal->begin("final");
  if (final) { final->flag_after = passed; final->latch_after = journal->invalid.load();
               final->bytes = logger->omitted.load(); }
  journal->publish(final);
  journal->drain();
  return passed && !journal->invalid.load() ? 0 : 64;
}
}  // namespace

int main(int argc, char** argv) {
  if (argc == 2 && std::strcmp(argv[1], "--identity") == 0) {
    std::puts("GUARD_REAL_POSIX_PROFILE\tcplusplus=202002\tNDEBUG=1\tstatus_checked=0\tnamespace=rocksdb"
              "\tmount_bytes=134217728\tmin_free_bytes=67108864\tmax_input_bytes=16777216"
              "\twrite_buffer_bytes=262144\tkeys=1024\tvalue_bytes=1024\ttrigger=2"
              "\trecords=1024\tfiles_per_job=8\twitness_seconds=25");
    return 0;
  }
  if (argc == 2 && std::strcmp(argv[1], "--list") == 0) {
    for (const char* name : kCases) std::printf("%s\t0\n", name);
    return 0;
  }
  if (argc == 5 && std::strcmp(argv[1], "--case") == 0 && std::strcmp(argv[3], "--root") == 0) {
    for (const char* name : kCases)
      if (std::strcmp(argv[2], name) == 0) return experiment(name, argv[4]);
  }
  std::fputs("expected --identity, --list or --case NAME --root FRESH_TMPFS_CASE_ROOT\n", stderr);
  return 64;
}
