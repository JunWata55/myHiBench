#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>

#define STDIN 0
#define STDOUT 1
#define DSIZE 1
#define ISIZE 1
#define MSIZE 1
#define LSIZE 1

#define MEASURE_INTERVAL 3
#define CRASH_INTERVAL 0
#define MEASURE_NUMBER 10
#define MEASURE_NUMBER2 10

char* dataSet[] = {"1000"};
char* intervals[] = {"2000"};
char* methods[] = {"1", "2"};
char* m_names[] = {"notIncremental", "Incremental"};
char* delay[] = {"1"};
char* numTaskManager = "1";
char* numLogic = "1";

void dataGen(int lnum, int dnum, int inum, int mnum);
void measureFlink(int rnum, int dnum, int interval, int method);
void getLatencyCSV(char *reportTopic, char *measureDataDir);
void startCluster();
void stopCluster();
int track(char *flinkjob);
void cancelFlink(char *flinkjob);
void metricsReader(char *kafkatopic, char *filename);
 
int main()
{
  printf("interval: %d[s], measure: %d, measure2: %d\n", MEASURE_INTERVAL, MEASURE_NUMBER, MEASURE_NUMBER2);

  for (int i = 0; i < LSIZE; i++) {
    for (int j = 0; j < DSIZE; j++) {
      for (int k = 0; k < ISIZE; k++) {
        for (int l = 0; l < MSIZE; l++) {
          dataGen(i, j, k, l);
        }
      }
    }
  }
  return 0;
}

void dataGen(int lnum, int dnum, int inum, int mnum) {
  printf("the function started with %s and the rate of 100/%s\n", dataSet[dnum], delay[lnum]);
  // child to parent
  int fds1[2];
  // parent to child
  int fds2[2];
  int pid, status;
  pipe(fds1);
  pipe(fds2);
  char cmd[] = "/home/junwata/HiBench/bin/workloads/streaming/wordcount/prepare/dataGen.sh";

  if ((pid = fork()) < 0) {
    perror("fork");
    exit(1);
  } else if (pid == 0) {
    // setup the pipe
    dup2(fds1[1], STDOUT);
    dup2(fds2[0], STDIN);

    close(fds1[0]);
    // close(fds1[1]);
    close(fds2[1]);

    printf("%s %s %s %s\n", cmd, dataSet[dnum], "1", delay[lnum]);
    
    execl(cmd, cmd, dataSet[dnum], "1", delay[lnum], (char *) NULL);
    exit(0);
  } else {
    printf("pid is %d\n", pid);
    close(fds1[1]);
    close(fds2[0]);
    close(fds2[1]);
    FILE *fp = fdopen(fds1[0], "r");
    char buffer[1000];

    while(1) {
      fgets(buffer, 1000, fp);
      printf("%s", buffer);
      if (strncmp("pool-1-thread-1 - starting generate data ...", buffer, 44) == 0)
          break;
    }

    startCluster();
    measureFlink(lnum, dnum, inum, mnum);
    stopCluster();

    printf("time has come\n");
    char kill_all[100];
    sprintf(kill_all, "/home/junwata/HiBench/doitUtils.sh %d", pid);
    system(kill_all);
    waitpid(pid, &status, 0);

    printf("pid finished %d\n", pid);
    close(fds1[0]);
  }
}

void measureFlink(int lnum, int dnum, int inum, int mnum) {
  printf("starting flink job with the interval of %s ms and the method of %s\n", intervals[inum], methods[mnum]);

  // child to parent
  int fds1[2];
  // parent to child
  int fds2[2];
  int pid, status;
  pipe(fds1);
  pipe(fds2);
  char cmd[] = "/home/junwata/HiBench/bin/workloads/streaming/wordcount/flink/run.sh";

  if ((pid = fork()) < 0) {
    perror("fork");
    exit(1);
  } else if (pid == 0) {
    // setup the pipe
    dup2(fds1[1], STDOUT);
    dup2(fds2[0], STDIN);

    close(fds1[0]);
    close(fds2[1]);
    
    execl(cmd, cmd, intervals[inum], methods[mnum], numLogic, (char *) NULL);
    exit(0);
  } else {
    printf("pid is %d\n", pid);
    close(fds1[1]);
    close(fds2[0]);
    close(fds2[1]);
    FILE *fp = fdopen(fds1[0], "r");
    char buffer[1000];
    char kafkatopic[100];
    char flinkjob[100];
    char filename[100];

    char cmd[1000];
    char dir[300];
    sprintf(dir, "my-data/little-slow%s-dataSet%s-interval%s-%s", delay[lnum], dataSet[dnum], intervals[inum], m_names[mnum]);
    sprintf(cmd, "mkdir -p %s", dir);
    system(cmd);

    sprintf(cmd, ": > /home/junwata/flink-1.15.0/log/flink-junwata-standalonesession-0-junichi-server.log");
    system(cmd);
    
    while(1) {
      fgets(buffer, 1000, fp);
      printf("%s", buffer);
      if (strncmp("metrics is being written to kafka topic ", buffer, 40) == 0) {
        strcpy(kafkatopic, &buffer[40]);
        break;
      }
    }

    while(1) {
      fgets(buffer, 1000, fp);
      printf("%s", buffer);
      if (strncmp("Job has been submitted with JobID ", buffer, 34) == 0) {
        strcpy(flinkjob, &buffer[34]);
        char *cp = strchr(flinkjob, '\n');
        if (cp != NULL)
          *cp = '\0';
        break;
      }
    }

    int pid2 = track(flinkjob);

    for (int i = 0; i < MEASURE_NUMBER; i++) {
      sleep(MEASURE_INTERVAL);
      // metricsReader(kafkatopic, filename);
    }

    stopCluster();

    sprintf(cmd, "/home/junwata/HiBench/mvLog.sh %s 1", dir);
    system(cmd);

    if (CRASH_INTERVAL > 0)
      sleep(CRASH_INTERVAL);
    startCluster();

    for (int i = 0; i < MEASURE_NUMBER2; i++) {
      sleep(MEASURE_INTERVAL);
      // metricsReader(kafkatopic, filename);
    }

    if (pid2 != 0) {
        sprintf(cmd, "/home/junwata/HiBench/doitUtils.sh %d", pid2);
        system(cmd);
        waitpid(pid2, &status, 0);
    }

    printf("time has come\n");

    sprintf(cmd, "mv report/%s %s/%s", filename, dir, filename);
    // system(cmd);

    sprintf(cmd, "/home/junwata/HiBench/doitUtils.sh %d", pid);
    system(cmd);
    waitpid(pid, &status, 0);

    cancelFlink(flinkjob);

    sprintf(cmd, "/home/junwata/HiBench/mvLog.sh %s 2", dir);
    system(cmd);

    sprintf(cmd, "cp /home/junwata/flink-1.15.0/log/flink-junwata-standalonesession-0-junichi-server.log %s/jobmanager.log", dir);
    system(cmd);

    sprintf(cmd, "cp /home/junwata/HiBench/record.txt %s/record.txt", dir);
    system(cmd);

    getLatencyCSV(kafkatopic, dir);

    printf("pid finished %d\n", pid);
    close(fds1[0]);
  }
}

void getLatencyCSV(char *reportTopic, char *measureDataDir) {
  int pid, status;
  char cmd[] = "/home/junwata/flink-1.15.0/bin/flink";
  char jarFile[] = "/home/junwata/flink-test/kafka/target/kafka-test.jar";

  if ((pid = fork()) < 0) {
    perror("fork");
    exit(1);
  } else if (pid == 0) {
    execl(cmd, cmd, "run", jarFile, "6", reportTopic, measureDataDir, (char *) NULL);
    exit(0);
  } else {
    sleep(10);
    // kill the flink job submission process
    sprintf(cmd, "/home/junwata/HiBench/doitUtils.sh %d", pid);
    system(cmd);
    waitpid(pid, &status, 0);
  }
}

void startCluster() {
  int pid, status;
  char cmd[] = "/home/junwata/start.sh";

  if ((pid = fork()) < 0) {
    perror("fork");
    exit(1);
  } else if (pid == 0) {    
    execl(cmd, cmd, numTaskManager, (char *) NULL);
    exit(0);
  } else {
    waitpid(pid, &status, 0);
  }
}

void stopCluster() {
  int pid, status;
  char cmd[] = "/home/junwata/stop.sh";

  if ((pid = fork()) < 0) {
    perror("fork");
    exit(1);
  } else if (pid == 0) {    
    execl(cmd, cmd, numTaskManager, (char *) NULL);
    exit(0);
  } else {
    waitpid(pid, &status, 0);
  }
}

int track(char *flinkjob) {
  int pid;
  char cmd[] = "/home/junwata/flink-1.15.0/track.sh";

  if ((pid = fork()) < 0) {
    perror("fork");
    return 0;
  } else if (pid == 0) {    
    execlp(cmd, cmd, flinkjob, (char *) NULL);
    return 0;
  } else {
    return pid;
  }
}

void cancelFlink(char *flinkjob) {
  int pid, status;
  char cmd[] = "/home/junwata/flink-1.15.0/bin/flink";

  if ((pid = fork()) < 0) {
    perror("fork");
    exit(1);
  } else if (pid == 0) {    
    execl(cmd, cmd, "cancel", flinkjob, (char *) NULL);
    exit(0);
  } else {
    waitpid(pid, &status, 0);
  }
}

void metricsReader(char *kafkatopic, char *filename) {
  // child to parent
  int fds1[2];
  // parent to child
  int fds2[2];
  int pid, status;
  pipe(fds1);
  pipe(fds2);
  char cmd[] = "/home/junwata/HiBench/bin/workloads/streaming/wordcount/common/metrics_reader.sh";

  if ((pid = fork()) < 0) {
    perror("fork");
    exit(1);
  } else if (pid == 0) {
    dup2(fds1[1], STDOUT);
    dup2(fds2[0], STDIN);

    close(fds1[0]);
    close(fds2[1]);

    execl(cmd, cmd, (char *) NULL);
    exit(0);
  } else {
    printf("pid is %d\n", pid);
    close(fds1[1]);
    close(fds2[0]);
    FILE *fp = fdopen(fds1[0], "r");
    char buffer[1000];

    while(1) {
      fgets(buffer, 1000, fp);
      printf("%s", buffer);
      if (strncmp("Please input the topic:", buffer, 23) == 0) {
        printf("the place\n");
        write(fds2[1], kafkatopic, strlen(kafkatopic));
        printf("%s", kafkatopic);
        break;
      }
    }

    while(1) {
      fgets(buffer, 1000, fp);
      printf("%s", buffer);
      if (strncmp("written out metrics to /home/junwata/HiBench/report/", buffer, 52) == 0) {
        strcpy(filename, &buffer[52]);
        char *cp = strchr(filename, '\n');
        if (cp != NULL)
          *cp = '\0';
        break;
      }
    }

    waitpid(pid, &status, 0);
    close(fds1[0]);
    close(fds2[1]);
  }
}
