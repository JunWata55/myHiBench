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
#define METHOD_SIZE 2
#define MEASUREMENT_SIZE 2
#define LOGIC 0
#define STATE 1

#define MEASURE_INTERVAL 10
#define MEASURE_NUMBER 10
#define MEASURE_NUMBER2 10

#define CRASH_NUMBER 2
#define CRASH_INTERVAL 100

char* measureSet[MEASUREMENT_SIZE][2] = {{"1", "1000"}, {"0", "20000"}};
char* method_names[METHOD_SIZE] = {"notIncremental", "Incremental"};

void measureFlink(int m_num);
void getLatencyCSV(char *reportTopic, char *measureDataDir);
void startCluster();
void stopCluster();
int track(char *flinkjob);
void cancelFlink(char *flinkjob);
 
int main()
{
    printf("interval: %d[s], measure: %d, crash num: %d\n", MEASURE_INTERVAL, MEASURE_NUMBER, CRASH_NUMBER);

    for (int i = 0; i < MEASUREMENT_SIZE; i++) {
        // startCluster();
        measureFlink(i);
        // stopCluster();
    }

    system("rm /home/junwata/flink-1.15.0/log/tmp*");
    return 0;
}

void measureFlink(int m_num) {
    // get the number of method name
    int method_num = measureSet[m_num][LOGIC][0] - '0';
    printf("starting flink job with the state size of %s and the method of %s\n", measureSet[m_num][STATE], method_names[method_num]);

    // child to parent
    int fds1[2];
    // parent to child
    int fds2[2];
    int pid, status;
    pipe(fds1);
    pipe(fds2);
    char cmd[] = "/home/junwata/flink-1.15.0/bin/flink";
    char jarFile[] = "/home/junwata/flink-test/kafka/target/kafka-test.jar";
    char reportTopic[100];
    // create the report topic name
    struct timeval now;
    gettimeofday(&now, NULL);
    time_t t = now.tv_sec * 1000000 + now.tv_usec;
    sprintf(reportTopic, "FLINK_%ld", t);

    if ((pid = fork()) < 0) {
        perror("fork");
        exit(1);
    } else if (pid == 0) {
        // setup the pipe
        dup2(fds1[1], STDOUT);
        dup2(fds2[0], STDIN);

        close(fds1[0]);
        close(fds2[1]);

        execl(cmd, cmd, "run", jarFile, "10", measureSet[m_num][LOGIC], measureSet[m_num][STATE], reportTopic, (char *) NULL);
        exit(0);
    } else {
        printf("pid is %d\n", pid);
        close(fds1[1]);
        close(fds2[0]);
        close(fds2[1]);
        FILE *fp = fdopen(fds1[0], "r");
        char buffer[1000];
        char flinkjob[100];
        char cmd[1000];
        char dir[300];
        sprintf(dir, "my-data/crash-stateSize%s-%s", measureSet[m_num][STATE], method_names[method_num]);
        sprintf(cmd, "mkdir -p %s", dir);
        system(cmd);

        sprintf(cmd, ": > /home/junwata/flink-1.15.0/log/flink-junwata-standalonesession-0-junichi-server.log");
        system(cmd);
    
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

        // period of execution
        for (int i = 0; i < MEASURE_NUMBER; i++) {
            sleep(MEASURE_INTERVAL);
        }
        sleep(5); // considering the savepoint latency

        // an environment with typical fault
        for (int i = 0; i < CRASH_NUMBER; i++) {
            sleep(CRASH_INTERVAL);
            stopCluster();
            startCluster();
        }

        // period of execution for the ending
        for (int i = 0; i < MEASURE_NUMBER2; i++) {
            sleep(MEASURE_INTERVAL);
        }

        // kill the tracking process
        if (pid2 != 0) {
            sprintf(cmd, "/home/junwata/HiBench/doitUtils.sh %d", pid2);
            system(cmd);
            waitpid(pid2, &status, 0);
        }

        sprintf(cmd, "/home/junwata/HiBench/doitUtils.sh %d", pid);
        system(cmd);
        waitpid(pid, &status, 0);

        cancelFlink(flinkjob);

        sprintf(cmd, "cp /home/junwata/flink-1.15.0/log/flink-junwata-standalonesession-0-junichi-server.log %s/jobmanager.log", dir);
        system(cmd);

        sprintf(cmd, "cp /home/junwata/HiBench/record.txt %s/record.txt", dir);
        system(cmd);

        getLatencyCSV(reportTopic, dir);

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
        execl(cmd, cmd, "1", (char *) NULL);
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
        execl(cmd, cmd, "1", (char *) NULL);
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