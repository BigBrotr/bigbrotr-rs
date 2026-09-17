/* Mac syscall observation; no product hooks or fault injection. */
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

static int observe_fsync(int fd) {
    struct stat child, current;
    /* The exact isolated test creates parent/db. Before native open it has no
       CURRENT. Neither RocksDB's db-directory fsync nor a WAL fsync matches. */
    int is_parent = fstatat(fd, "db", &child, AT_SYMLINK_NOFOLLOW) == 0 &&
        S_ISDIR(child.st_mode) &&
        fstatat(fd, "db/CURRENT", &current, AT_SYMLINK_NOFOLLOW) == -1 &&
        errno == ENOENT;
    int result = fsync(fd);
    int saved = errno;
    if (is_parent && result == 0) {
        static const char message[] = "PROBE parent directory fsync returned success before CURRENT exists\n";
        (void)write(STDERR_FILENO, message, sizeof(message) - 1);
    }
    errno = saved;
    return result;
}
__attribute__((used)) static struct { const void *replacement; const void *original; }
interpose_fsync __attribute__((section("__DATA,__interpose"))) = {
    (const void *)observe_fsync, (const void *)fsync
};
