/* Research only: one real openat, then ordinary process-wide exit. */
#define _GNU_SOURCE
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static int directory;
static void *reader(void *unused)
{
    (void)unused;
    int fd = openat(directory, "destination.json", O_RDONLY | O_NOFOLLOW | O_NONBLOCK);
    if (fd < 0) _exit(8);
    close(fd);
    puts("{\"event\":\"returned\"}");
    fflush(stdout);
    return NULL;
}
int main(int argc, char **argv)
{
    if (argc != 2) return 2;
    directory = open(argv[1], O_RDONLY | O_DIRECTORY);
    if (directory < 0) return 3;
    printf("{\"event\":\"ready\",\"pid\":%d}\n", getpid());
    fflush(stdout);
    pthread_t worker;
    if (pthread_create(&worker, NULL, reader, NULL)) return 5;
    char action = 0;
    if (read(STDIN_FILENO, &action, 1) != 1 || action != 'E') return 6;
    /* glibc _exit uses exit_group: a tracer must let every exit-stop finish. */
    _exit(4);
}
