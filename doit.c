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
#define DSIZE 3 // actually 5
#define ISIZE 3 // actually 4
#define MSIZE 2
#define RSIZE 2 // actually 5
// #define DSIZE 1
// #define ISIZE 1
// #define MSIZE 1

// char* dataSet[] = {"1000", "10000", "100000", "1000000", "2000000"};
// char* dataSet[] = {"1000", "10000", "100000", "1000000"};
char* dataSet[] = {"10000", "100000", "1000000"};
// char* intervals[] = {"10", "100", "1000", "10000"};
char* intervals[] = {"100", "1000", "10000"};
char* methods[] = {"0", "2"};
char* m_names[] = {"memory", "rocksdb"};

char* rate[] = {"4", "5"};

void dataGen(int rnum, int dnum, int inum, int mnum);
void measureFlink(int rnum, int dnum, int interval, int method);
void cancelFlink(char *flinkjob);
void metricsReader(char *kafkatopic, char *filename);
void mk_proc(char *cmd, char *args[]) {
  // child to parent
  int fds1[2];
  // parent to child
  int fds2[2];
  int pid, status;
  pipe(fds1);
  pipe(fds2);

  if ((pid = fork()) < 0) {
    perror("fork");
    exit(1);
  } else if (pid == 0) {
    dup2(fds1[1], STDOUT);
    dup2(fds2[0], STDIN);

    close(fds1[0]);
    close(fds2[1]);
    
    // execv(cmd, args);
    system(cmd);
    exit(0);
  } else {
    close(fds1[1]);
    close(fds2[0]);

    char buffer[100];

    write(fds2[1], "input\n", 6);
    read(fds1[0], buffer, 100);

    printf("%s", buffer);

    kill(pid, SIGKILL);
    waitpid(pid, &status, 0);
    printf("%d\n", WIFSIGNALED(status));
  }
}
 
int main()
{
  pid_t pid, pid2, pid3, pid4, pid5;
  int status;
  int ret;

  char buffer[1000];
  char kafkatopic[50];
  char flinkjob[50];

  // char* args[] = {NULL};
  // mk_proc("/home/junwata/HiBench/doit.sh", args);
  // return 0;

  // the point i changed
  // for (int i = 0; i < DSIZE; i++) {
  for (int i = 0; i < RSIZE; i++) {
    for (int j = 0; j < DSIZE; j++) {
      for (int k = 0; k < ISIZE; k++) {
        for (int l = 0; l < MSIZE; l++) {
          dataGen(i, j, k, l);
        }
      }
    }
  }
  // for (int i = 0; i < DSIZE; i++) {
  //   for (int j = 0; j < ISIZE; j++) {
  //     for (int k = 0; k < MSIZE; k++) {
  //       dataGen(i, j, k);
  //     }
  //   }
  // }
  return 0;
}

void dataGen(int rnum, int dnum, int inum, int mnum) {
  printf("the function started with %s and the rate of %s00\n", dataSet[dnum], rate[rnum]);
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

    printf("%s %s %s\n", cmd, dataSet[dnum], rate[rnum]);
    
    execl(cmd, cmd, dataSet[dnum], rate[rnum], (char *) NULL);
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

    // for (int i = 0; i < ISIZE; i++)
    //   for (int j = 1; j < MSIZE; j++)
    //     measureFlink(dnum, i, j);
    measureFlink(rnum, dnum, inum, mnum);

    printf("time has come\n");
    char kill_all[100];
    sprintf(kill_all, "/home/junwata/HiBench/doitUtils.sh %d", pid);
    system(kill_all);
    waitpid(pid, &status, 0);

    printf("pid finished %d\n", pid);
    close(fds1[0]);
  }
}

void measureFlink(int rnum, int dnum, int inum, int mnum) {
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

    // printf("%s %s\n", cmd, arg);
    
    execl(cmd, cmd, intervals[inum], methods[mnum], (char *) NULL);
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
    sprintf(dir, "my-data/throughput%s00-dataSet%s-interval%s-%s", rate[rnum], dataSet[dnum], intervals[inum], m_names[mnum]);
    sprintf(cmd, "mkdir -p %s", dir);
    system(cmd);

    sprintf(cmd, ": > /home/junwata/flink-1.15.0/log/flink-junwata-standalonesession-0-junichi-server.log");
    system(cmd);

    sprintf(cmd, "ps -p 1302623 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    sprintf(cmd, "ps -p 1303316 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd); 
    sprintf(cmd, "ps -p 1304091 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    sprintf(cmd, "ps -p 1304885 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 940825 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 941585 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 942335 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 943092 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 943818 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 944560 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 945341 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 946692 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 947465 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 948205 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 948969 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    
    sprintf(cmd, "echo >> %s/ps.txt", dir); system(cmd);

    while(1) {
      fgets(buffer, 1000, fp);
      printf("%s", buffer);
      if (strncmp("metrics is being written to kafka topic ", buffer, 40) == 0) {
        strcpy(kafkatopic, &buffer[40]);
        // char *cp = strchr(kafkatopic, '\n');
        // if (cp != NULL)
        //   *cp = '\0';
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

    for (int i = 0; i < 5; i++) {
      sleep(10);
      metricsReader(kafkatopic, filename);
    }

    printf("time has come\n");

    sprintf(cmd, "mv report/%s %s/%s", filename, dir, filename);
    system(cmd);

    sprintf(cmd, "ps -p 1302623 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    sprintf(cmd, "ps -p 1303316 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd); 
    sprintf(cmd, "ps -p 1304091 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    sprintf(cmd, "ps -p 1304885 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 940825 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 941585 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 942335 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 943092 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 943818 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 944560 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 945341 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 946692 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 947465 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 948205 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);
    // sprintf(cmd, "ps -p 948969 -o pid,size,rss,tsiz,trs,%%mem >> %s/ps.txt", dir); system(cmd);

    sprintf(cmd, "/home/junwata/HiBench/doitUtils.sh %d", pid);
    system(cmd);
    waitpid(pid, &status, 0);

    cancelFlink(flinkjob);

    sprintf(cmd, "cp /home/junwata/flink-1.15.0/log/flink-junwata-standalonesession-0-junichi-server.log %s/jobmanager.log", dir);
    system(cmd);

    printf("pid finished %d\n", pid);
    close(fds1[0]);
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
