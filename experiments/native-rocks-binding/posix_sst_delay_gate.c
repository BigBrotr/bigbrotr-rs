/* Experimental Linux observation/delay shim; never substitutes an I/O result.
 * The controller owns two inherited pipes. Only the first successful writable
 * O_CREAT|O_TRUNC /posix/db/[digits].sst open is delayed, before returning its
 * real fd. Every native call still reaches RTLD_NEXT with its original args.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#define GATE_MS 5000
#define MAX_RECORDS 64
static int (*next_open)(const char *, int, ...);
static int (*next_open64)(const char *, int, ...);
static int (*next_openat)(int, const char *, int, ...);
static int (*next_openat64)(int, const char *, int, ...);
static ssize_t (*next_write)(int, const void *, size_t);
static ssize_t (*next_pwrite)(int, const void *, size_t, off_t);
static ssize_t (*next_pwrite64)(int, const void *, size_t, off64_t);
static int journal_fd = -1, release_fd = -1;
static _Atomic int armed = 0, selected_fd = -1, invalid = 0;
static _Atomic unsigned sequence = 0;
static uint64_t selected_dev, selected_ino;
static int active = 0;

static void emit(const char *event, int fd, int flags, const struct stat *st,
                 const char *path, const char *operation, size_t count,
                 int64_t offset, int64_t returned, int observed_errno) {
  int saved = errno;
  unsigned seq = atomic_fetch_add(&sequence, 1) + 1;
  if (seq > MAX_RECORDS && strcmp(event, "end") != 0) {
    atomic_store(&invalid, 1);
    errno = saved;
    return;
  }
  char line[1024];
  int length = snprintf(line, sizeof(line),
      "{\"event\":\"%s\",\"seq\":%u,\"pid\":%ld,\"tid\":%ld,"
      "\"fd\":%d,\"flags\":%d,\"dev\":%" PRIu64 ",\"ino\":%" PRIu64 ","
      "\"size\":%" PRId64 ",\"blocks\":%" PRId64 ",\"path\":\"%s\","
      "\"operation\":\"%s\",\"count\":%zu,\"offset\":%" PRId64 ","
      "\"returned\":%" PRId64 ",\"errno\":%d,\"invalid\":%d}\n",
      event, seq, (long)syscall(SYS_getpid), (long)syscall(SYS_gettid), fd, flags,
      st ? (uint64_t)st->st_dev : 0, st ? (uint64_t)st->st_ino : 0,
      st ? (int64_t)st->st_size : 0, st ? (int64_t)st->st_blocks : 0,
      path ? path : "", operation ? operation : "", count, offset,
      returned, observed_errno, atomic_load(&invalid));
  if (length <= 0 || (size_t)length >= sizeof(line) ||
      syscall(SYS_write, journal_fd, line, (size_t)length) != length) {
    atomic_store(&invalid, 1);
  }
  errno = saved;
}

static int parse_fd(const char *name) {
  const char *raw = getenv(name);
  if (!raw || !*raw) return -1;
  char *end = NULL;
  long value = strtol(raw, &end, 10);
  return end && !*end && value > 2 && value < 1048576 ? (int)value : -1;
}

__attribute__((constructor)) static void initialize(void) {
  int saved = errno;
  next_open = (int (*)(const char *, int, ...))dlsym(RTLD_NEXT, "open");
  next_open64 = (int (*)(const char *, int, ...))dlsym(RTLD_NEXT, "open64");
  next_openat = (int (*)(int, const char *, int, ...))dlsym(RTLD_NEXT, "openat");
  next_openat64 = (int (*)(int, const char *, int, ...))dlsym(RTLD_NEXT, "openat64");
  next_write = (ssize_t (*)(int, const void *, size_t))dlsym(RTLD_NEXT, "write");
  next_pwrite = (ssize_t (*)(int, const void *, size_t, off_t))dlsym(RTLD_NEXT, "pwrite");
  next_pwrite64 = (ssize_t (*)(int, const void *, size_t, off64_t))dlsym(RTLD_NEXT, "pwrite64");
  journal_fd = parse_fd("BIGBROTR_SST_GATE_JOURNAL_FD");
  release_fd = parse_fd("BIGBROTR_SST_GATE_RELEASE_FD");
  if (!next_open || !next_open64 || !next_openat || !next_openat64 ||
      !next_write || !next_pwrite || !next_pwrite64) _exit(125);
  if (journal_fd >= 0 && release_fd >= 0 && journal_fd != release_fd) {
    struct stat a, b;
    int journal_flags = (int)syscall(SYS_fcntl, journal_fd, F_GETFL);
    int release_flags = (int)syscall(SYS_fcntl, release_fd, F_GETFL);
    if (syscall(SYS_fstat, journal_fd, &a) || syscall(SYS_fstat, release_fd, &b) ||
        !S_ISFIFO(a.st_mode) || !S_ISFIFO(b.st_mode) ||
        journal_flags < 0 || release_flags < 0 ||
        (journal_flags & O_ACCMODE) != O_WRONLY ||
        (release_flags & O_ACCMODE) != O_RDONLY ||
        !(journal_flags & O_NONBLOCK) || !(release_flags & O_NONBLOCK)) _exit(125);
    active = 1;
    emit("ready", -1, 0, NULL, NULL, NULL, 0, -1, 0, 0);
  }
  errno = saved;
}

__attribute__((destructor)) static void finish(void) {
  if (active) emit("end", atomic_load(&selected_fd), 0, NULL, NULL, NULL, 0, -1, 0, 0);
}

static int sst_path(int fd, char *path, size_t capacity) {
  char proc[64];
  int length = snprintf(proc, sizeof(proc), "/proc/self/fd/%d", fd);
  if (length <= 0 || (size_t)length >= sizeof(proc)) return 0;
  ssize_t count = syscall(SYS_readlink, proc, path, capacity - 1);
  if (count <= 0 || (size_t)count >= capacity - 1) return 0;
  path[count] = 0;
  const char *p = path;
  const char prefix[] = "/posix/db/";
  if (strncmp(p, prefix, sizeof(prefix) - 1)) return 0;
  p += sizeof(prefix) - 1;
  const char *digits = p;
  while (*p >= '0' && *p <= '9') ++p;
  return p != digits && strcmp(p, ".sst") == 0;
}

static int64_t monotonic_ms(void) {
  struct timespec time;
  if (syscall(SYS_clock_gettime, CLOCK_MONOTONIC, &time)) return -1;
  return (int64_t)time.tv_sec * 1000 + time.tv_nsec / 1000000;
}

static void after_open(int fd, int flags) {
  int saved = errno;
  if (!active || fd < 0 || (flags & (O_CREAT | O_TRUNC)) != (O_CREAT | O_TRUNC) ||
      ((flags & O_ACCMODE) != O_WRONLY && (flags & O_ACCMODE) != O_RDWR)) goto done;
  char path[256];
  struct stat st;
  if (!sst_path(fd, path, sizeof(path)) || syscall(SYS_fstat, fd, &st)) goto done;
  int expected = 0;
  if (!atomic_compare_exchange_strong(&armed, &expected, 1)) {
    atomic_store(&invalid, 1);
    emit("duplicate_gate", fd, flags, &st, path, NULL, 0, -1, fd, saved);
    goto done;
  }
  if (!S_ISREG(st.st_mode) || st.st_size != 0 || st.st_blocks != 0 || st.st_nlink != 1) {
    atomic_store(&invalid, 1);
    emit("invalid_gate", fd, flags, &st, path, NULL, 0, -1, fd, saved);
    goto done;
  }
  selected_dev = (uint64_t)st.st_dev;
  selected_ino = (uint64_t)st.st_ino;
  atomic_store_explicit(&selected_fd, fd, memory_order_release);
  emit("gate", fd, flags, &st, path, NULL, 0, -1, fd, saved);
  int64_t start = monotonic_ms();
  for (;;) {
    int64_t now = monotonic_ms();
    int remaining = (start >= 0 && now >= start) ? GATE_MS - (int)(now - start) : 0;
    struct pollfd pollfd = {.fd = release_fd, .events = POLLIN};
    int waited = remaining > 0 ? (int)syscall(SYS_poll, &pollfd, 1, remaining) : 0;
    if (waited < 0 && errno == EINTR) continue;
    char release = 0;
    if (waited > 0 && (pollfd.revents & POLLIN) &&
        syscall(SYS_read, release_fd, &release, 1) == 1 && release == 'R') {
      emit("released", fd, flags, &st, path, NULL, 0, -1, fd, saved);
    } else {
      atomic_store(&invalid, 1);
      emit("gate_timeout_or_protocol_error", fd, flags, &st, path, NULL, 0, -1, fd, saved);
    }
    break;
  }
done:
  errno = saved;
}

static int needs_mode(int flags) {
  return (flags & O_CREAT) || ((flags & O_TMPFILE) == O_TMPFILE);
}

#define OPEN_WRAPPER(name) \
int name(const char *path, int flags, ...) { \
  mode_t mode = 0; \
  if (needs_mode(flags)) { va_list args; va_start(args, flags); mode = va_arg(args, mode_t); va_end(args); } \
  int fd = next_##name ? next_##name(path, flags, mode) : (int)syscall(SYS_openat, AT_FDCWD, path, flags, mode); \
  after_open(fd, flags); return fd; \
}
OPEN_WRAPPER(open)
OPEN_WRAPPER(open64)
#define OPENAT_WRAPPER(name) \
int name(int dir, const char *path, int flags, ...) { \
  mode_t mode = 0; \
  if (needs_mode(flags)) { va_list args; va_start(args, flags); mode = va_arg(args, mode_t); va_end(args); } \
  int fd = next_##name ? next_##name(dir, path, flags, mode) : (int)syscall(SYS_openat, dir, path, flags, mode); \
  after_open(fd, flags); return fd; \
}
OPENAT_WRAPPER(openat)
OPENAT_WRAPPER(openat64)

static int selected(int fd, struct stat *st) {
  int saved = errno;
  int match = active && fd == atomic_load_explicit(&selected_fd, memory_order_acquire) &&
      syscall(SYS_fstat, fd, st) == 0 && (uint64_t)st->st_dev == selected_dev &&
      (uint64_t)st->st_ino == selected_ino;
  errno = saved;
  return match;
}

ssize_t write(int fd, const void *bytes, size_t count) {
  struct stat st;
  int match = selected(fd, &st);
  ssize_t result = next_write ? next_write(fd, bytes, count) : syscall(SYS_write, fd, bytes, count);
  int saved = errno;
  if (match) emit("io", fd, 0, &st, NULL, "write", count, -1, result, saved);
  errno = saved; return result;
}
#define PWRITE_WRAPPER(name, offset_type, syscall_name) \
ssize_t name(int fd, const void *bytes, size_t count, offset_type offset) { \
  struct stat st; int match = selected(fd, &st); \
  ssize_t result = next_##name ? next_##name(fd, bytes, count, offset) : syscall(syscall_name, fd, bytes, count, offset); \
  int saved = errno; \
  if (match) emit("io", fd, 0, &st, NULL, #name, count, offset, result, saved); \
  errno = saved; return result; \
}
PWRITE_WRAPPER(pwrite, off_t, SYS_pwrite64)
PWRITE_WRAPPER(pwrite64, off64_t, SYS_pwrite64)
