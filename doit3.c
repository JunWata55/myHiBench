#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <dirent.h>

#define STDIN 0
#define STDOUT 1
#define SWSIZE 2 // the size of switch set
#define STSIZE 2 // the size of state set

#define MEASURE_INTERVAL 10
#define CRASH_INTERVAL 0
#define MEASURE_NUMBER 30
#define MEASURE_NUMBER2 10

char* switchSet[SWSIZE][2] = {{"0", "1"}, {"1", "0"}};
char* stateSet[STSIZE] = {"1000", "20000"};
char* m_names[2] = {"notIncremental", "Incremental"};

char* parallelism = "5";

void measureFlink(int switch_num, int state_num);
void executeFlink(int switch_num, int state_num, char *reportTopic, char *savepointDir, char *measureDataDir);
void executeFlinkWithSavepoint(int switch_num, int state_num, char *reportTopic, char *savepointDir, char *measureDataDir);
void getLatencyCSV(char *reportTopic, char *measureDataDir);
int track(char *flinkjob);
void cancelFlink(char *flinkjob);
void stopFlink(char *flinkjob, char *savepointDir);
void metricsReader(char *kafkatopic, char *filename);
 
int main()
{
  pid_t pid, pid2, pid3, pid4, pid5;
  int status;
  int ret;

  char buffer[1000];
  char kafkatopic[50];
  char flinkjob[50];

  printf("interval: %d[s], measure: %d, measure2: %d\n", MEASURE_INTERVAL, MEASURE_NUMBER, MEASURE_NUMBER2);

  for (int i = 0; i < SWSIZE; i++) {
    for (int j = 0; j < STSIZE; j++) {
      measureFlink(i, j);
    }
  }
  system("rm /home/junwata/flink-1.15.0/log/tmp*");
  return 0;
}

void measureFlink(int switch_num, int state_num) {
  char reportTopic[100];
  char savepointDir[300];
  char measureDataDir[300];

  // create the report topic name
  struct timeval now;
  gettimeofday(&now, NULL);
  time_t t = now.tv_sec * 1000000 + now.tv_usec;
  sprintf(reportTopic, "FLINK_%ld", t);

  // create the directory name for storing measured data
  char firstCheckpoint = switchSet[switch_num][0][0] - '0';
  char secondCheckpoint = switchSet[switch_num][1][0] - '0';
  sprintf(measureDataDir, "my-data/state%s-from-%s-to-%s",stateSet[state_num], m_names[firstCheckpoint], m_names[secondCheckpoint]);

  printf("the setup of the measurement:\n\tthe job will transit from %s to %s\n\twith the state size of %s\n\twritten into the kafka topic of %s\n\n",
          m_names[firstCheckpoint],
          m_names[secondCheckpoint],
          stateSet[state_num],
          reportTopic
        );
  
  // execute two flink jobs with a savepoint in between
  executeFlink(switch_num, state_num, reportTopic, savepointDir, measureDataDir);
  executeFlinkWithSavepoint(switch_num, state_num, reportTopic, savepointDir, measureDataDir);
  getLatencyCSV(reportTopic, measureDataDir);
}

// execute the first job
void executeFlink(int switch_num, int state_num, char *reportTopic, char *savepointDir, char *measureDataDir) {
  // child to parent
  int fds1[2];
  // parent to child
  int fds2[2];
  int pid, status;
  pipe(fds1);
  pipe(fds2);
  char cmd[] = "/home/junwata/flink-1.15.0/bin/flink";
  char jarFile[] = "/home/junwata/flink-test/kafka/target/kafka-test.jar";

  if ((pid = fork()) < 0) {
    perror("fork");
    exit(1);
  } else if (pid == 0) {
    // setup the pipe
    dup2(fds1[1], STDOUT);
    dup2(fds2[0], STDIN);

    close(fds1[0]);
    close(fds2[1]);
    
    printf("the cmd will be:\n\t%s run -p %s %s %s %s %s\n", cmd, parallelism, jarFile, switchSet[switch_num][0], stateSet[state_num], reportTopic);
    execl(cmd, cmd, "run", "-p", parallelism, jarFile, switchSet[switch_num][0], stateSet[state_num], reportTopic, (char *) NULL);
    exit(0);
  } else {
    close(fds1[1]);
    close(fds2[0]);
    close(fds2[1]);
    FILE *fp = fdopen(fds1[0], "r");

    char buffer[1000];
    char flinkjob[100];
    char cmd[1000];

    // make a new directory for the measured data
    sprintf(cmd, "mkdir -p %s", measureDataDir);
    system(cmd);

    // clean up the jobmanager log
    sprintf(cmd, ": > /home/junwata/flink-1.15.0/log/flink-junwata-standalonesession-0-junichi-server.log");
    system(cmd);

    // grep the flink job id
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

    // start the tracking process 1
    int pid2 = track(flinkjob);

    // period of execution
    for (int i = 0; i < MEASURE_NUMBER; i++) {
      sleep(MEASURE_INTERVAL);
    }
  
    // kill the tracking process 1
    if (pid2 != 0) {
      sprintf(cmd, "/home/junwata/HiBench/doitUtils.sh %d", pid2);
      system(cmd);
      waitpid(pid2, &status, 0);
    }

    // kill the flink job submission process
    sprintf(cmd, "/home/junwata/HiBench/doitUtils.sh %d", pid);
    system(cmd);
    waitpid(pid, &status, 0);

    // stop the flink job
    stopFlink(flinkjob, savepointDir);

    // store the monitor data in an appropriate directory
    sprintf(cmd, "cp /home/junwata/HiBench/record.txt %s/record-1.txt", measureDataDir);
    system(cmd);

    printf("first measurement end\n");
    close(fds1[0]);
  }
}

// execute the second job
void executeFlinkWithSavepoint(int switch_num, int state_num, char *reportTopic, char *savepointDir, char *measureDataDir) {
  // child to parent
  int fds1[2];
  // parent to child
  int fds2[2];
  int pid, status;
  pipe(fds1);
  pipe(fds2);
  char cmd[] = "/home/junwata/flink-1.15.0/bin/flink";
  char jarFile[] = "/home/junwata/flink-test/kafka/target/kafka-test.jar";

  if ((pid = fork()) < 0) {
    perror("fork");
    exit(1);
  } else if (pid == 0) {
    // setup the pipe
    dup2(fds1[1], STDOUT);
    dup2(fds2[0], STDIN);

    close(fds1[0]);
    close(fds2[1]);
    
    // printf("the cmd will be:\n\t%s run -p %s %s %s %s %s\n", cmd, parallelism, jarFile, switchSet[switch_num][1], stateSet[state_num], reportTopic);
    execl(cmd, cmd, "run", "-p", parallelism, "-s", savepointDir, jarFile, switchSet[switch_num][1], stateSet[state_num], reportTopic, (char *) NULL);
    exit(0);
  } else {
    close(fds1[1]);
    close(fds2[0]);
    close(fds2[1]);
    FILE *fp = fdopen(fds1[0], "r");

    char buffer[1000];
    char flinkjob[100];
    char cmd[1000];

    // grep the flink job id
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

    // start the tracking process 2
    int pid2 = track(flinkjob);

    // period of execution
    for (int i = 0; i < MEASURE_NUMBER2; i++) {
      sleep(MEASURE_INTERVAL);
    }

    // kill the tracking process 2
    if (pid2 != 0) {
        sprintf(cmd, "/home/junwata/HiBench/doitUtils.sh %d", pid2);
        system(cmd);
        waitpid(pid2, &status, 0);
    }

    // kill the flink job submission process
    sprintf(cmd, "/home/junwata/HiBench/doitUtils.sh %d", pid);
    system(cmd);
    waitpid(pid, &status, 0);

    // cancel the flink job
    cancelFlink(flinkjob);

    // store the jobmanager log in an appropriate directory
    sprintf(cmd, "cp /home/junwata/flink-1.15.0/log/flink-junwata-standalonesession-0-junichi-server.log %s/jobmanager.log", measureDataDir);
    system(cmd);

    // store the monitor data in an appropriate directory
    sprintf(cmd, "cp /home/junwata/HiBench/record.txt %s/record-2.txt", measureDataDir);
    system(cmd);

    // remove the savepoint directory
    system("rm -r /tmp/flink-savepoints-directory/*");

    printf("second measurement end\n");
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

void stopFlink(char *flinkjob, char *savepointDir) {
  // child to parent
  int fds1[2];
  // parent to child
  int fds2[2];
  int pid, status;
  pipe(fds1);
  pipe(fds2);
  char cmd[] = "/home/junwata/flink-1.15.0/bin/flink";

  if ((pid = fork()) < 0) {
    perror("fork");
    exit(1);
  } else if (pid == 0) {
    // setup the pipe
    dup2(fds1[1], STDOUT);
    dup2(fds2[0], STDIN);

    close(fds1[0]);
    close(fds2[1]);

    execl(cmd, cmd, "stop", "--drain", flinkjob, (char *) NULL);
    exit(0);
  } else {
    close(fds1[1]);
    close(fds2[0]);
    close(fds2[1]);
    FILE *fp = fdopen(fds1[0], "r");
    char buffer[1000];

    while(1) {
      fgets(buffer, 1000, fp);
      printf("%s", buffer);
      if (strncmp("Savepoint completed. Path: file:", buffer, 32) == 0) {
        strcpy(savepointDir, &buffer[32]);
        char *cp = strchr(savepointDir, '\n');
        if (cp != NULL)
          *cp = '\0';
        break;
      }
    }

    waitpid(pid, &status, 0);
    close(fds1[0]);
  }
}