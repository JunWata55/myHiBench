#include <stdio.h>
#include <dirent.h>
#include <string.h>

int main(void)
{
    char dd[100];
    DIR *dir;
    struct dirent *dp;
    char dirpath[] = "/tmp/flink-savepoints-directory";

    dir = opendir(dirpath);
    if (dir == NULL) { return 1; }

    dp = readdir(dir);
    while (dp != NULL) {
        if (dp->d_name[0] != '.')
            break;
        dp = readdir(dir);
    }
    printf("%s\n", dp->d_name);
    strcpy(dd, dp->d_name);
    printf("%s\n", dd);

    if (dir != NULL) { closedir(dir); }

    return 0;
} 