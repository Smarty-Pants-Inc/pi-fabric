#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>

static void *hold(void *arg) {
  int fd = open((const char *)arg, O_RDONLY);
  if (fd < 0) _exit(2);
  for (;;) pause();
  return NULL;
}
int main(int argc, char **argv) {
  pthread_t thread;
  if (argc != 2 || pthread_create(&thread, NULL, hold, argv[1])) return 1;
  pthread_exit(NULL);
}
