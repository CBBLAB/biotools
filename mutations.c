#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "mpi.h"

#define EXTRA_BYTES (1024 * 16)

char mutations[] = { 'A', 'C', 'G', 'T' };

void usage()
{
    printf("usage: ./mutations --mutation-percent [value]\n");
    exit(1);
}

int main(int argc, char **argv)
{
    MPI_File fh;
    char *buf;
    int comm_rank, comm_size;
    uint64_t process_portion;
    MPI_Request req[2];
    MPI_Status status[2];
    int count, i, j, skip;
    MPI_Offset total_count;
    int mutation_percent = -1;
    char *input = NULL, *output = NULL;
    FILE *tmp_fp;
    size_t filesz;

    while (--argc && ++argv) {
        if (!strcmp(*argv, "--mutation-percent")) {
            --argc;
            ++argv;
            mutation_percent = atoi(*argv);
        }
        else if (!strcmp(*argv, "--input")) {
            --argc;
            ++argv;
            input = strdup(*argv);
        }
        else if (!strcmp(*argv, "--output")) {
            --argc;
            ++argv;
            output = strdup(*argv);
        }
        else {
            usage();
        }
    }

    if (mutation_percent < 0 || !input || !output) {
        usage();
    }

    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &comm_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

    tmp_fp = fopen(input, "r");
    fseek(tmp_fp, 0L, SEEK_END);
    filesz = ftell(tmp_fp);
    fclose(tmp_fp);

    process_portion = ((filesz / comm_size) + 1);
    buf = (char *) malloc(process_portion + 1 + EXTRA_BYTES);

    MPI_File_open(MPI_COMM_WORLD, input, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);

    MPI_File_read_at_all(fh, process_portion * comm_rank, buf, process_portion, MPI_CHAR,
                         MPI_STATUS_IGNORE);

    for (i = 0; buf[i] != '>'; i++);
    MPI_Isend(buf, i, MPI_CHAR, (comm_rank + comm_size - 1) % comm_size, 0, MPI_COMM_WORLD,
              &req[0]);
    MPI_Irecv(buf + process_portion, EXTRA_BYTES, MPI_CHAR, (comm_rank + 1) % comm_size, 0,
              MPI_COMM_WORLD, &req[1]);
    MPI_Waitall(2, req, status);

    MPI_Get_count(&status[1], MPI_CHAR, &count);
    buf += i;

    /* final count of how much I have */
    count += process_portion - i;

    srand(0);
    skip = 0;
    for (i = 0; i < count; i++) {
        if (buf[i] == '>')
            skip = 1;
        else if (buf[i] == '\n')
            skip = 0;

        if (!skip) {
            /* real data: mutate */
            if (rand() % 100 < mutation_percent) {

                for (j = 0; j < 4; j++)
                    if (mutations[j] == buf[i])
                        break;
                if (j == 4)
                    continue;

                /* find what character we should mutate to */
                j = ((j + (rand() % 3) + 1) % 4);
                buf[i] = mutations[j];
            }
        }
    }

    MPI_Exscan(&count, &total_count, 1, MPI_OFFSET, MPI_SUM, MPI_COMM_WORLD);
    if (comm_rank == 0)
        total_count = 0;

    MPI_File out;
    MPI_File_open(MPI_COMM_WORLD, output, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                  MPI_INFO_NULL, &out);
    MPI_File_write_at_all(out, total_count, buf, count, MPI_CHAR, MPI_STATUS_IGNORE);
    MPI_File_close(&out);

    MPI_File_close(&fh);

    MPI_Finalize();

    return 0;
}
