#ifndef _MYMPI_H_
#define _MYMPI_H_

#include <math.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "mytypes.h"

#define BUFFLEN 1024
#define ROOT 0

int mpiError(char *fxn, int code);
int mpiSetup(MPI_Comm comm, int argc, char **argv, int *id, int *size);
int mpiSendDbl(const double *data, int size, int dest, int tag, MPI_Comm comm);
int mpiSendInt(const int *data, int size, int dest, int tag, MPI_Comm comm);
int mpiSendLong(const long *data, int size, int dest, int tag, MPI_Comm comm);
int mpiSendChar(const char *data, int size, int dest, int tag, MPI_Comm comm);
int mpiSendStr(const char *data, int dest, int tag, MPI_Comm comm);
int mpiSend(const void *data, int size, MPI_Datatype type, int dest, int tag, MPI_Comm comm);
int mpiIsend(const void *data, int size, MPI_Datatype type, int dest, int tag, MPI_Comm comm, MPI_Request *req);
int mpiProbe(int source, int tag, MPI_Comm comm, MPI_Status *status);
int mpiRecvDbl(double *data, int size, int src, int tag, MPI_Comm comm, MPI_Status *status);
int mpiRecvInt(int *data, int size, int src, int tag, MPI_Comm comm, MPI_Status *status);
int mpiRecvLong(long *data, int size, int src, int tag, MPI_Comm comm, MPI_Status *status);
int mpiRecvStr(char *data, int size, int src, int tag, MPI_Comm comm, MPI_Status *status);
int mpiRecv(void *data, int size, MPI_Datatype type, int src, int tag, MPI_Comm comm, MPI_Status *status);
int mpiIrecv(void *data, int size, MPI_Datatype type, int src, int tag, MPI_Comm comm, MPI_Request *request);
int mpiReduce(const void *sendData, void *recvData, int size, MPI_Datatype type, MPI_Op op, int root, MPI_Comm comm);
int mpiGather(const void *sendData, int sendSize, MPI_Datatype sendType, void *recvData, int recvSize, MPI_Datatype recvType, int root, MPI_Comm comm);
int mpiScatter(const void *sendData, int sendSize, MPI_Datatype sendType, void *recvData, int recvSize, MPI_Datatype recvType, int root, MPI_Comm comm);
int mpiAllReduce(void* sendData, void* recvData, int size, MPI_Datatype type, MPI_Op op, MPI_Comm comm);
int mpiAllGather(const void *sendData, int sendSize, MPI_Datatype sendType, void *recvData, int recvSize, MPI_Datatype recvType, MPI_Comm comm);
int mpiAllToAll(const void *sendData, const int *sendSizes, const int *sendDisp, MPI_Datatype sendType, void *recvData, const int *recvSizes, const int *recvDisp, MPI_Datatype recvType, MPI_Comm comm);
int mpiFinish();
double mpiTimer();
int mpiCount(MPI_Status *status, MPI_Datatype type);
int mpiSource(MPI_Status *status);
int mpiTest(MPI_Request *req, MPI_Status *status);
int mpiTestAll(int size, MPI_Request *requests);
int mpiWait(MPI_Request *request, MPI_Status *status);
int mpiWaitAll(int size, MPI_Request *requests);

#endif  // _MYMPI_H_
