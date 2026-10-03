// Linux watchdog fixture: publish a healthy lease but deliberately never waitpid.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>

static long long now_ms(void) {
  struct timeval t; gettimeofday(&t, NULL);
  return (long long)t.tv_sec * 1000 + t.tv_usec / 1000;
}
static void replace(const char *file, const char *text) {
  char temp[4096]; snprintf(temp, sizeof(temp), "%s.tmp", file);
  FILE *f = fopen(temp, "w"); if (!f) exit(2);
  fputs(text, f); fclose(f); if (rename(temp, file)) exit(2);
}
int main(void) {
  const char *root = getenv("WATCHDOG_ROOT"), *lease = getenv("WATCHDOG_LEASE");
  if (!root || !lease) return 2;
  char file[4096], text[4096], stat[4096], birth[64];
  FILE *f = fopen("/proc/self/stat", "r");
  if (!f || !fgets(stat, sizeof(stat), f)) return 2;
  fclose(f);
  char *field = strtok(strrchr(stat, ')') + 2, " ");
  for (int i = 0; i < 19; i++) field = strtok(NULL, " ");
  if (!field) return 2;
  snprintf(birth, sizeof(birth), "%s", field);
  snprintf(file, sizeof(file), "%s/starts", root);
  int count = 0; f = fopen(file, "r");
  if (f) { if (fscanf(f, "%d", &count) != 1) return 2; fclose(f); }
  count++;
  long long ready = now_ms();
  snprintf(file, sizeof(file), "%s/owner.json", root);
  snprintf(text, sizeof(text), "{\"format\":1,\"hostId\":\"resident:zombie\",\"pid\":%d,\"processStartTime\":\"%s\",\"token\":\"%d\",\"readyAt\":%lld}", getpid(), birth, getpid(), ready);
  replace(file, text);
  if (count == 1) {
    pid_t child = fork();
    if (child < 0) return 2;
    if (child == 0) _exit(0);
    snprintf(file, sizeof(file), "%s/zombie-pid", root);
    snprintf(text, sizeof(text), "%d", child); replace(file, text);
  }
  snprintf(file, sizeof(file), "%s/starts", root);
  snprintf(text, sizeof(text), "%d", count); replace(file, text);
  for (;;) {
    long long now = now_ms();
    snprintf(text, sizeof(text), "{\"format\":1,\"id\":\"resident:zombie\",\"rootId\":\"session:watchdog\",\"identityId\":\"resident:zombie\",\"updatedAt\":%lld,\"expiresAt\":%lld}", now, now + 60000);
    replace(lease, text); sleep(1);
  }
}
