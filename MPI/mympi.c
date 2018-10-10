#include "mympi.h"

int mpiError(char *fxn, int code) {
    char msg[BUFFLEN];

    if (code != MPI_SUCCESS) {
        switch (code) {
            case MPI_ERR_OTHER:
                if (strstr(fxn, "Init") != NULL) {
                    strcpy(msg, "MPI may only be initialized once per program");
                } else if (strstr(fxn, "Final")) {
                    strcpy(msg, "Undefined error during finalization");
                }
                break;

            case MPI_ERR_BUFFER:
                strcpy(msg, "Invalid buffer pointer");
                break;

            case MPI_ERR_COUNT:
                strcpy(msg, "Invalid count argument");
                break;

            case MPI_ERR_TYPE:
                strcpy(msg, "Invalid datatype argument");
                break;

            case MPI_ERR_TAG:
                strcpy(msg, "Invalid tag argument");
                break;

            case MPI_ERR_RANK:
                strcpy(msg, "Invalid source or destination rank");
                break;

            case MPI_ERR_COMM:
                strcpy(msg, "Invalid communicator, check for null");
                break;

            case MPI_ERR_INTERN:
                strcpy(msg, "Unable to acquire memory");
                break;

            case MPI_ERR_ARG:
                strcpy(msg, "Invalid argument, check inputs");
                break;

            case MPI_ERR_PENDING:
                strcpy(msg, "Request remains pending");
                break;

            case MPI_ERR_REQUEST:
                strcpy(msg, "Invalid request, check for null");
                break;

            case MPI_ERR_IN_STATUS:
                strcpy(msg, "Check MPI_Status argument for actual error");
                break;

            default:        // Other...
                strcpy(msg, "Unknown error, google it");
                break;
        }

        if (strlen(msg) > 0) {
            fprintf(stderr, "MPI ERROR[%d] in %s: %s!\n", code, fxn, msg);
        }
    }

    return code;
}

int mpiSetup(MPI_Comm comm, int argc, char **argv, int *id, int *size) {
    int retval = MPI_Init(&argc, &argv);
    if (retval == MPI_SUCCESS) {
        retval = MPI_Comm_rank(comm, id);
        mpiError("MPI_Comm_rank", retval);

        retval = MPI_Comm_size(comm, size);
        mpiError("MPI_Comm_size", retval);
    } else {
        mpiError("MPI_Init", retval);
    }

    return retval;
}

int mpiSendDbl(const double *data, int size, int dest, int tag, MPI_Comm comm) {
    return mpiSend(data, size, MPI_DOUBLE, dest, tag, comm);
}

int mpiSendInt(const int *data, int size, int dest, int tag, MPI_Comm comm) {
    return mpiSend(data, size, MPI_INT, dest, tag, comm);
}

int mpiSendLong(const long *data, int size, int dest, int tag, MPI_Comm comm) {
    return mpiSend(data, size, MPI_LONG, dest, tag, comm);
}

int mpiSendChar(const char *data, int size, int dest, int tag, MPI_Comm comm) {
    return mpiSend(data, size, MPI_CHAR, dest, tag, comm);
}

int mpiSendStr(const char *data, int dest, int tag, MPI_Comm comm) {
    return mpiSend(data, strlen(data) + 1, MPI_CHAR, dest, tag, comm);
}

int mpiSend(const void *data, int size, MPI_Datatype type, int dest, int tag, MPI_Comm comm) {
    int retval = MPI_Send(data, size, type, dest, tag, comm);

    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Send", retval);
    }

    return retval;
}

int mpiIsend(const void *data, int size, MPI_Datatype type, int dest, int tag, MPI_Comm comm, MPI_Request *req) {
    int retval = MPI_Isend(data, size, type, dest, tag, comm, req);

    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Isend", retval);
    }

    return retval;
}

int mpiRecvDbl(double *data, int size, int src, int tag, MPI_Comm comm, MPI_Status *status) {
    return mpiRecv(data, size, MPI_DOUBLE, src, tag, comm, status);
}

int mpiRecvInt(int *data, int size, int src, int tag, MPI_Comm comm, MPI_Status *status) {
    return mpiRecv(data, size, MPI_INT, src, tag, comm, status);
}

int mpiRecvLong(long *data, int size, int src, int tag, MPI_Comm comm, MPI_Status *status) {
    return mpiRecv(data, size, MPI_LONG, src, tag, comm, status);
}

int mpiRecvStr(char *data, int size, int src, int tag, MPI_Comm comm, MPI_Status *status) {
    return mpiRecv(data, size, MPI_CHAR, src, tag, comm, status);
}

int mpiRecv(void *data, int size, MPI_Datatype type, int src, int tag, MPI_Comm comm, MPI_Status *status) {
    int retval = MPI_Recv(data, size, type, src, tag, comm, status);
    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Recv", retval);
    }
    return retval;
}

int mpiIrecv(void *data, int size, MPI_Datatype type, int src, int tag, MPI_Comm comm, MPI_Request *request) {
    int retval = MPI_Irecv(data, size, type, src, tag, comm, request);
    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Irecv", retval);
    }
    return retval;
}

int mpiReduce(const void *sendData, void *recvData, int size, MPI_Datatype type, MPI_Op op, int root, MPI_Comm comm) {
    int retval = MPI_Reduce(sendData, recvData, size, type, op, root, comm);
    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Reduce", retval);
    }
    return retval;
}

int mpiGather(const void *sendData, int sendSize, MPI_Datatype sendType, void *recvData, int recvSize, MPI_Datatype recvType, int root, MPI_Comm comm) {
    int retval = MPI_Gather(sendData, sendSize, sendType, recvData, recvSize, recvType, root, comm);
    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Gather", retval);
    }
    return retval;
}

int mpiScatter(const void *sendData, int sendSize, MPI_Datatype sendType, void *recvData, int recvSize, MPI_Datatype recvType, int root, MPI_Comm comm) {
    int retval = MPI_Scatter(sendData, sendSize, sendType, recvData, recvSize, recvType, root, comm);
    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Scatter", retval);
    }
    return retval;
}

int mpiAllReduce(void* sendData, void* recvData, int size, MPI_Datatype type, MPI_Op op, MPI_Comm comm) {
    int retval = MPI_Allreduce(sendData, recvData, size, type, op, comm);
    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Allreduce", retval);
    }
    return retval;
}

int mpiAllGather(const void *sendData, int sendSize, MPI_Datatype sendType, void *recvData, int recvSize, MPI_Datatype recvType, MPI_Comm comm) {
    int retval = MPI_Allgather(sendData, sendSize, sendType, recvData, recvSize, recvType, comm);
    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Allgather", retval);
    }
    return retval;
}

int mpiAllToAll(const void *sendData, const int *sendSizes, const int *sendDisp, MPI_Datatype sendType, void *recvData, const int *recvSizes, const int *recvDisp, MPI_Datatype recvType, MPI_Comm comm) {
    int retval;

    if (sendDisp == NULL || recvDisp == NULL) {
        retval = MPI_Alltoall(sendData, sendSizes[0], sendType, recvData, recvSizes[0], recvType, comm);
    } else {
        retval = MPI_Alltoallv(sendData, sendSizes, sendDisp, sendType, recvData, recvSizes, recvDisp, recvType, comm);
    }

    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Alltoall", retval);
    }

    return retval;
}

double mpiTimer() {
    return MPI_Wtime();
}

int mpiCount(MPI_Status *status, MPI_Datatype type) {
    int count = 0;
    int retval = MPI_SUCCESS;

    retval = MPI_Get_count(status, type, &count);

    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Get_count", retval);
    }

    return count;
}

int mpiSource(MPI_Status *status) {
    return status->MPI_SOURCE;
}

int mpiProbe(int source, int tag, MPI_Comm comm, MPI_Status *status) {
    int retval = MPI_Probe(source, tag, comm, status);

    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Test", retval);
    }

    return retval;
}

int mpiTest(MPI_Request *request, MPI_Status *status) {
    int flag = 0;
    int retval = MPI_Test(request, &flag, status);

    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Test", retval);
        flag = -1;
    }

    return flag;
}

int mpiTestAll(int size, MPI_Request *requests) {
    int flag = 0;
    int retval = MPI_SUCCESS;

    MPI_Status *statuses = (MPI_Status *) calloc(size, sizeof(MPI_Status));

    retval = MPI_Testall(size, requests, &flag, statuses);

    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Testall", retval);
        flag = -1;
    }

    free(statuses);

    return flag;
}

int mpiWait(MPI_Request *request, MPI_Status *status) {
    int retval = MPI_Wait(request, status);

    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Wait", retval);
    }

    return retval;
}

int mpiWaitAll(int size, MPI_Request *requests) {
    int retval = MPI_SUCCESS;

    MPI_Status *statuses = (MPI_Status *) calloc(size, sizeof(MPI_Status));

    retval = MPI_Waitall(size, requests, statuses);

    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Waitall", retval);
    }

    free(statuses);

    return retval;
}

int mpiFinish() {
    int retval = MPI_Finalize();

    if (retval != MPI_SUCCESS) {
        mpiError("MPI_Finalize", retval);
    }

    return retval;
}
