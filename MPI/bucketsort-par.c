#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <time.h>
#include <omp.h>
#include <prand.h>
#include "mympi.h"

#define  DEBUG       1
#define  OVER_FLOW  11
#define  BOX_TAG     1
#define  SENTINEL   -1

// Available sorting algos for bucketsort...
#define  ISORT       1
#define  QSORT       2

/* Box struct */
typedef struct _Box {
    int *elems;
    int capacity;
    int size;
    int index;
} Box;

/* Usage function */
void usage(char *program);

/* Array functions */
void genArray(int *array, int nElems, int seed, int pid);
void printArray(int *array, int nElems, FILE *fp);

/* Sort functions */
void parBucketSortP2P(int *array, int *pElems, int nBuckets, int pid, int pSize);
void parBucketSortA2A(int *array, int *pElems, int nBuckets, int pid, int pSize);
void seqBucketSort(int *array, int nElems, int nBuckets, int sortAlgo);
void insertionSort(int *array, int nElems);
void qsortPar(int *array, int size);
void qsortSub(int *array, int size, int threaded);
void qsortSwap(int *left, int *right);
int isSorted(int *array, int nElems);
int compareTo(const void *x, const void *y);

/* Box functions... */
void initBox(Box *box, int index, int capacity, int size, int doFill);
int boxInsert(Box *box, int elem, int pid);
int boxResize(Box *box);
void printBox(Box *box, FILE *fp);
void mergeBoxes(Box **boxes, int nBoxes, int *array, int *nElems);

/* Bucket functions */
void bucketInsert(int elem, int index, int **buckets, int *capacities, int *sizes);
void resizeBucket(int index, int **buckets, int *capacities);

/* Random functions */
LLONG prandSetup(UINT pid, UINT seed, ULONG nPerProc);
int unrankRand(long long int stride);

/* Global variables for OMP qsortPar... */
int _nWorkers = 0;
int _nActive = 0;
omp_lock_t _lock;

int main(int argc, char **argv) {
    int nElems = 0;
    int nBuckets = 0;
    int seed = 0;
    int *array;
    int pid;
    int pSize;
    int pElems;

    double tRun = 0.0;

    if (argc > 1) {
        /* Start up MPI */
        mpiSetup(MPI_COMM_WORLD, argc, argv, &pid, &pSize);

        nElems = atoi(argv[1]);

        if (argc > 2) {
            nBuckets = atoi(argv[2]);
        } else {
            nBuckets = (int) ceil(sqrt((double) nElems));
        }

        if (argc > 3) {
            seed = atoi(argv[3]);
        } else {
            seed = time(NULL);
        }

        /* Divide remaining numbers into remainder processes */
        pElems = (int) ceil((double) nElems / (double) pSize);

        array = (int *) calloc(nElems, sizeof(int));
        genArray(array, pElems, seed, pid);

        if (pid == 0) {
            tRun = mpiTimer();
        }

        parBucketSortP2P(array, &pElems, nBuckets, pid, pSize);
        //parBucketSortA2A(array, &pElems, nBuckets, pid, pSize);

        if (pid == 0) {
            tRun = mpiTimer() - tRun;
            printf("bucketsort: n = %d  m = %d buckets seed = %d time = %lf seconds\n", nElems, nBuckets, seed, tRun);
        }

        if (!isSorted(array, pElems)) {
            fprintf(stderr, "P%d ERROR: Array is not sorted!\n", pid);
        }

        free(array);

        /* Wrap up MPI */
        mpiFinish();

    } else {
        usage(argv[0]);
    }

    return 0;
}

void usage(char *program) {
    printf("usage: %s [items] <buckets> <seed>\n", program);
}

void genArray(int *array, int nElems, int seed, int pid) {
    int i;

    prandSetup(pid, seed, nElems);  // Setup PRNG...

    for (i = 0; i < nElems; i++) {
        array[i] = random();
    }
}

void printArray(int *array, int nElems, FILE *fp) {
    int i;
    int nPerLine = 10;

    fprintf(fp, "{");
    for (i = 0; i < nElems; i++) {
        fprintf(fp, " %d,", array[i]);
        if (i > 0 && i % nPerLine == 0) {
            fprintf(fp, "\n");
        }
    }

    fprintf(fp, " }\n");
    fflush(fp);
}

void parBucketSortA2A(int *array, int *pElems, int nBuckets, int pid, int pSize) {
    int i, j;
    int initCap;
    int elem;
    int index;
    int range;
    int size;
    int src;
    int nReqs;
    int intSize;
    int sendSum;
    int recvSum;
    int pSizeSq;

    int *initVals = NULL;
    int *allSizes = NULL;
    int *sendData = NULL;
    int *sendSizes = NULL;
    int *sendDisps = NULL;
    int *recvSizes = NULL;
    int *recvData = NULL;
    int *recvDisps = NULL;

    Box **boxes = NULL;
    Box *box = NULL;

    MPI_Status status;
    MPI_Comm comm = MPI_COMM_WORLD;
    MPI_Request *reqs;

#ifdef DEBUG
    char buff[32];
    FILE *flog = NULL;

    // Create some file pointers...
    sprintf(buff, "p%d.log", pid);
    flog = fopen(buff, "w");
    if (flog == NULL) {
        flog = stderr;
    }
#endif

    range = RAND_MAX / pSize;
    //initCap = *pElems;
    initCap = *pElems / pSize;
    initCap += (initCap * OVER_FLOW) / 100; // 11% extra for overflow
    intSize = sizeof(int);

    boxes = (Box **) calloc(pSize, sizeof(Box *));
    for (i = 0; i < pSize; i++) {
        boxes[i] = (Box *) calloc(1, sizeof(Box));
        boxes[i]->elems = (int *) calloc(initCap, intSize);
        initBox(boxes[i], i, initCap, 0, TRUE);

#ifdef DEBUG
        fprintf(flog, "P%d: Allocated box %d of size %d, capacity %d\n", pid, i, boxes[i]->size, boxes[i]->capacity);
        fflush(flog);
#endif
    }

    for (i = 0; i < *pElems; i++) {
        elem = array[i];
        index = elem / range;
        boxInsert(boxes[index], elem, pid);
    }

    box = boxes[pid];

#ifdef DEBUG
    fprintf(flog, "P%d: Keeping box %d of size %d.\n", pid, pid, boxes[pid]->size);
    //printBox(boxes[pid], flog);
    fflush(flog);
#endif

    // Allocate arrays for all-to-all...
    sendSizes = (int *) calloc(pSize, intSize);
    recvSizes = (int *) calloc(pSize, intSize);
    recvDisps = (int *) calloc(pSize, intSize);
    sendDisps = (int *) calloc(pSize, intSize);

    pSizeSq = pSize * pSize;
    allSizes = (int *) calloc(pSizeSq, intSize);

    // Setup send data...
    sendSum = 0;
    sendDisps[0] = 0;

    for (i = 0; i < pSize; i++) {
        sendSizes[i] = boxes[i]->size;
        sendSum += sendSizes[i];
        if (i > 0) {
            j = i - 1;
            sendDisps[i] = sendDisps[j] + sendSizes[j];
        }
    }

    // Here we gather all box sizes from other PIDs.
#ifdef DEBUG
    fprintf(flog, "P%d: Sending box sizes to mpiAllGather\n", pid);
    printArray(sendSizes, pSize, flog);
    fflush(flog);
#endif

    mpiAllGather(sendSizes, pSize, MPI_INT, allSizes, pSize, MPI_INT, comm);

    // Select the sizes this process will actually receive...
    j = 0;
    for (i = pid; i < pSizeSq; i+= pSize) {
        recvSizes[j] = allSizes[i];
        j += 1;
    }

#ifdef DEBUG
    fprintf(flog, "P%d: Collected recvSizes from %d processes\n", pid, pSize);
    printArray(recvSizes, pSize, flog);
    fflush(flog);
#endif

    // Build up the send  and recv data...
    recvSum = 0;
    recvDisps[0] = 0;

    for (i = 0; i < pSize; i++) {
        recvSum += recvSizes[i];
        if (i > 0) {
            j = i - 1;
            recvDisps[i] = recvDisps[j] + recvSizes[j];
        }
    }

#ifdef DEBUG
    fprintf(flog, "P%d: recvSum = %d, recvDisps: ", pid, recvSum);
    printArray(recvDisps, pSize, flog);
    fprintf(flog, "P%d: sendSum = %d, sendDisps: ", pid, sendSum);
    printArray(sendDisps, pSize, flog);
    fflush(flog);
#endif

    // Allocate recv and send data
    sendData = (int *) calloc(sendSum, intSize);
    recvData = (int *) calloc(recvSum, intSize);

    // And finally, prepare the send data...
    memset(recvData, 0, sendSum * intSize);
    for (i = 0; i < pSize; i++) {
        memcpy(&sendData[sendDisps[i]], boxes[i]->elems, sendSizes[i] * intSize);
    }

#ifdef DEBUG
    fprintf(flog, "P%d: pre recvData: ", pid);
    printArray(recvData, recvSum, flog);
    fprintf(flog, "P%d: pre sendData: ", pid);
    printArray(sendData, sendSum, flog);
    fflush(flog);
#endif

    // All to all goes here...
    mpiAllToAll(sendData, sendSizes, sendDisps, MPI_INT, recvData, recvSizes, recvDisps, MPI_INT, comm);

#ifdef DEBUG
    fprintf(flog, "P%d: post recvData: ", pid);
    printArray(recvData, recvSum, flog);
    fprintf(flog, "P%d: post sendData: ", pid);
    printArray(sendData, sendSum, flog);
    fflush(flog);
#endif

    // Copy received data into the output array...
    memcpy(array, recvData, recvSum * intSize);
    *pElems = recvSum;

    // No need to combine the buckets, already done by all-to-all...
    seqBucketSort(array, *pElems, nBuckets, QSORT);

    // Gather initial data points of each node to ensure data are sorted across all.
    if (pid == ROOT) {
        initVals = (int *) calloc(pSize, intSize);
    }

#ifdef DEBUG
    fprintf(flog, "P%d: Sending initial value %d to mpiGather\n", pid, array[0]);
    printArray(array, *pElems, flog);
    fflush(flog);
#endif

    mpiGather(array, 1, MPI_INT, initVals, 1, MPI_INT, ROOT, comm);

    if (pid == ROOT) {
#ifdef DEBUG
        fprintf(flog, "P%d: Gathered initial values from all %d processes\n", pid, pSize);
        fflush(flog);
#endif
        if (!isSorted(initVals, pSize)) {
            fprintf(stderr, "P%d ERROR: All nodes are not sorted!... ", pid);
            printArray(initVals, pSize, stderr);
        }

        free(initVals);
    }

    free(sendData);
    free(sendDisps);
    free(sendSizes);
    free(recvData);
    free(recvDisps);
    free(recvSizes);

    for (i = 0; i < pSize; i++) {
#ifdef DEBUG
        fprintf(flog, "P%d: Freeing box %d of size %d, capacity %d\n", pid, i, boxes[i]->size, boxes[i]->capacity);
        fflush(flog);
#endif
        free(boxes[i]->elems);
        free(boxes[i]);
    }

    free(boxes);

#ifdef DEBUG
    fprintf(flog, "P%d: array of size %d has been sorted.\n", pid, *pElems);
    //printArray(array, *pElems, flog);

    if (flog != stderr) {
        fclose(flog);
    }
#endif
}

void parBucketSortP2P(int *array, int *pElems, int nBuckets, int pid, int pSize) {
    int i, j;
    int initCap;
    int elem;
    int index;
    int range;
    int size;
    int src;
    int nReqs;

    int *inits = NULL;

    Box **boxes = NULL;
    Box *box = NULL;

    MPI_Status status;
    MPI_Comm comm = MPI_COMM_WORLD;
    MPI_Request *reqs;

#ifdef DEBUG
    char buff[32];
    FILE *flog = NULL;

    // Create some file pointers...
    sprintf(buff, "p%d.log", pid);
    flog = fopen(buff, "w");
    if (flog == NULL) {
        flog = stderr;
    }
#endif

    range = RAND_MAX / pSize;
    initCap = *pElems;
    //initCap = *pElems / pSize;
    //initCap += (initCap * OVER_FLOW) / 100; // 11% extra for overflow

    boxes = (Box **) calloc(pSize, sizeof(Box *));
    for (i = 0; i < pSize; i++) {
        boxes[i] = (Box *) calloc(1, sizeof(Box));
        boxes[i]->elems = (int *) calloc(initCap, sizeof(int));
        initBox(boxes[i], i, initCap, 0, TRUE);

#ifdef DEBUG
        fprintf(flog, "P%d: Allocated box %d of size %d, capacity %d\n", pid, i, boxes[i]->size, boxes[i]->capacity);
        fflush(flog);
#endif
    }

    for (i = 0; i < *pElems; i++) {
        elem = array[i];
        index = elem / range;
        boxInsert(boxes[index], elem, pid);
    }

#ifdef DEBUG
    fprintf(flog, "P%d: Keeping box %d of size %d.\n", pid, pid, boxes[pid]->size);
    //printBox(boxes[pid], flog);
    fflush(flog);
#endif

    reqs = (MPI_Request *) calloc(pSize - 1, sizeof(MPI_Request));
    nReqs = 0;

    // Boxes are built... now we send them...
    for (j = 0; j < pSize; j++) {
        if (j != pid) {
#ifdef DEBUG
            fprintf(flog, "P%d: Sending box %d of size %d to P%d\n", pid, j, boxes[j]->size, j);
            //printBox(boxes[j], flog);
            fflush(flog);
#endif
            // Send a box async...
            mpiIsend(boxes[j]->elems, boxes[j]->size, MPI_INT, j, BOX_TAG, comm, &reqs[nReqs]);
            nReqs += 1;
        }
    }

    //initCap *= 2;       // Ensure ample space for receiving data...

    // Now receive them in sync (to avoid deadlock)...
    for (j = 0; j < nReqs; j++) {
        // Allocate a new box to receive into...
        box = (Box *) calloc(1, sizeof(Box));
        box->elems = (int *) calloc(initCap, sizeof(int));

        mpiRecv(box->elems, initCap, MPI_INT, MPI_ANY_SOURCE, BOX_TAG, comm, &status);
        src = mpiSource(&status);
        size = mpiCount(&status, MPI_INT);
        initBox(box, src, initCap, size, FALSE);

#ifdef DEBUG
        fprintf(flog, "P%d: Recv'd box %d of size %d from P%d\n", pid, src, size, src);
        //printBox(box, flog);
        fflush(flog);
#endif

        // Replace our bucket at this index...
        //free(boxes[src]->elems);
        free(boxes[src]);
        boxes[src] = box;
    }

    // Combine the buckets...
    mergeBoxes(boxes, pSize, array, pElems);
    seqBucketSort(array, *pElems, nBuckets, QSORT);
    //sequentialBucketsort(array, *pElems, nBuckets);

    // Gather initial data points of each node to ensure data are sorted across all.
    if (pid == ROOT) {
        inits = (int *) calloc(pSize, sizeof(int));
    }

#ifdef DEBUG
    fprintf(flog, "P%d: Sending initial value %d to mpiGather\n", pid, array[0]);
    //printArray(array, *pElems, flog);
    fflush(flog);
#endif

    mpiGather(array, 1, MPI_INT, inits, 1, MPI_INT, ROOT, comm);

    if (pid == ROOT) {
#ifdef DEBUG
        fprintf(flog, "P%d: Gathered initial values from all %d processes\n", pid, pSize);
        fflush(flog);
#endif
        if (!isSorted(inits, pSize)) {
            fprintf(stderr, "P%d ERROR: All nodes are not sorted!... ", pid);
            printArray(inits, pSize, stderr);
        }

        free(inits);
    }

    for (i = 0; i < pSize; i++) {
#ifdef DEBUG
        fprintf(flog, "P%d: Freeing box %d of size %d, capacity %d\n", pid, i, boxes[i]->size, boxes[i]->capacity);
        fflush(flog);
#endif
        free(boxes[i]->elems);
        free(boxes[i]);
    }

    free(boxes);

#ifdef DEBUG
    fprintf(flog, "P%d: array of size %d has been sorted.\n", pid, *pElems);
    //printArray(array, *pElems, flog);

    if (flog != stderr) {
        fclose(flog);
    }
#endif
}

void mergeBoxes(Box **boxes, int nBoxes, int *array, int *nElems) {
    int i, j, k;
    int maxElems = 0;

#pragma omp parallel for shared(boxes,nBoxes,maxElems) private(i)
    for (i = 0; i < nBoxes; i++) {
        maxElems += boxes[i]->size;
    }

    if (maxElems != *nElems) {
        *nElems = maxElems;
        memset(array, SENTINEL, *nElems * sizeof(int));
    }

    k = 0;
// Appears to have a race condition, so commenting omp pragma...
//#pragma omp parallel for shared(array,boxes,nBoxes,maxElems,k) private(i,j)
    for (i = 0; i < nBoxes; i++) {
        for (j = 0; j < boxes[i]->size; j++) {
            array[k] = boxes[i]->elems[j];
            k += 1;
        }
    }
}

void initBox(Box *box, int index, int capacity, int size, int doFill) {
    if (doFill) {
        memset(box->elems, SENTINEL, sizeof(int) * capacity);
    }

    box->capacity = capacity;
    box->size = size;
    box->index = index;
}

int boxInsert(Box *box, int elem, int pid) {
    if (box->size == box->capacity) {
        boxResize(box);
    }

    box->elems[box->size] = elem;
    box->size += 1;

    return box->size;
}

int boxResize(Box *box) {
    // Double the capacity...
    int size = sizeof(int);
    int newCap = 2 * box->capacity;
    int *newElems = (int *) calloc(newCap, size);

    // Copy contents to new box and free the old one...
    memcpy(newElems, box->elems, box->capacity * size);
    free(box->elems);

    // Insert new box...
    box->elems = newElems;
    box->capacity = newCap;

    return newCap;
}

void printBox(Box *box, FILE *fp) {
    int i;

    fprintf(fp, "size = %d, capacity = %d, elems = [", box->size, box->capacity);
    for (i = 0; i < box->size; i++) {
        fprintf(fp, " %d,", box->elems[i]);
    }

    fprintf(fp, "]\n");
    fflush(fp);
}

void seqBucketSort(int *array, int nElems, int nBuckets, int sortAlgo) {
    int **buckets;
    int *sizes;
    int *capacities;
    int capacity;
    int index;
    int maxIndex;
    int range;
    int size;
    int i, j, k;

    // Capacity per bucket...
    capacity = nElems / nBuckets;
    capacity += (capacity * OVER_FLOW) / 100; // 11% extra for overflow

    maxIndex = nBuckets - 1;
    range = RAND_MAX / nBuckets;
    size = sizeof(int);

    capacities = (int *) calloc(nBuckets, size);
    sizes = (int *) calloc(nBuckets, size);
    buckets = (int **) calloc(nBuckets, sizeof(int*));

    // Initialize buckets...
    #pragma omp parallel for default(shared) private(i)
    for (i = 0; i < nBuckets; i++) {
        buckets[i] = (int *) calloc(capacity, size);
        capacities[i] = capacity;
        sizes[i] = 0;
    }

    // Fill buckets...
    for (i = 0; i < nElems; i++) {
        index = array[i] / range;
        if (index > maxIndex) {
            index = maxIndex;
        }

        bucketInsert(array[i], index, buckets, capacities, sizes);
    }

    #pragma omp parallel for default(shared) private(i)
    for (i = 0; i < nBuckets; i++) {
        if (sizes[i] > 0) {
            if (sortAlgo == ISORT) {
                // Insertion sort each bucket individually...
                insertionSort(buckets[i], sizes[i]);
            } else {    // QSORT
                // Quicksort each bucket individually...
                //qsort(buckets[i], sizes[i], sizeof(int), compareTo);
                qsortPar(buckets[i], sizes[i]);
            }
        }
    }

    // Merge buckets...
    k = 0;
//#pragma omp parallel for default(shared) private(i)
    for (i = 0; i < nBuckets; i++) {
        for (j = 0; j < sizes[i]; j++) {
            array[k] = buckets[i][j];
            k++;
        }
    }

    // Free buckets...
    for (i = 0; i < nBuckets; i++) {
        free(buckets[i]);
    }

    free(buckets);
    free(sizes);
    free(capacities);
}

void bucketInsert(int elem, int index, int **buckets, int *capacities, int *sizes) {
    if (sizes[index] == capacities[index]) {
        resizeBucket(index,buckets, capacities);
    }

    buckets[index][sizes[index]] = elem;
    sizes[index] += 1;
}

void resizeBucket(int index, int **buckets, int *capacities) {
    int *newBuck;
    int newCap;

    // Double the capacity...
    newCap = 2 * capacities[index];
    newBuck = (int *) calloc(newCap, sizeof(int));

    // Copy contents to new bucket and free the old one...
    memcpy(newBuck, buckets[index], capacities[index] * sizeof(int));
    free(buckets[index]);

    // Insert new bucket...
    buckets[index] = newBuck;
    capacities[index] = newCap;
}

void insertionSort(int *array, int nElems) {
    int i = 0, k = 0;
    int item;

    #pragma omp parallel for default(shared) private(i)
    for (i = 0; i < nElems; i++) {
        item = array[i];

        k = i;
        while (k > 0 && item < array[k - 1]) {
            array[k] = array[k - 1];
            k -= 1;
        }

        array[k] = item;
    }
}

void qsortPar(int *array, int size) {
    #pragma omp parallel
    {
        _nWorkers = omp_get_num_threads();
    }

    _nActive = 1;
    omp_init_lock(&_lock);
    qsortSub(array, size, TRUE);
    omp_destroy_lock(&_lock);
}

void qsortSub(int *array, int size, int threaded) {
    int left = 0;
    int right = size;
    int pivot;
    int recurse = FALSE;

    if (size > 1) {
        pivot = array[left];
        while (left < right) {
            while (left < size && array[++left] < pivot);
            while (array[--right] > pivot);

            if (left < right) {
                qsortSwap(&array[left], &array[right]);
            }
        }

        qsortSwap(&array[left - 1], &array[0]);

        // Check for available workers...
        omp_set_lock(&_lock);
        recurse = (_nActive < _nWorkers);
        if (recurse) {
            _nActive += 2;      // One thread for left partition, one for right.
        }
        omp_unset_lock(&_lock);

        if (recurse) {
            #pragma omp parallel sections
            {
                #pragma omp section
                qsortSub(array, left - 1, TRUE);

                #pragma omp section
                qsortSub(array + left, size - left, TRUE);
            }
        } else {    // No threads left... run sequentially...
            qsortSub(array, left - 1, FALSE);
            qsortSub(array + left, size - left, FALSE);
        }
    }

    if (threaded) {     // Free the thread running this iteration...
        omp_set_lock(&_lock);
        _nActive -= 1;
        omp_unset_lock(&_lock);
    }
}

void qsortSwap(int *left, int *right) {
    int temp = *left;
    *left = *right;
    *right = temp;
}

int isSorted(int *array, int nElems) {
    int i;
    int m = nElems - 1;
    int sorted = 1;

    for (i = 0; i < m && sorted; i++) {
        if (sorted && array[i] > array[i + 1]) {
            sorted = 0;
        }
    }

    return sorted;
}

/*
 * compareTo function for using qsort
 * returns  -ve if *x < *y, 0 if *x == *y, +ve if *x > *y
 */
int compareTo(const void *x, const void *y) {
    return ((*(int *) x) - (*(int *) y));
}

LLONG prandSetup(UINT pid, UINT seed, ULONG nPerProc) {
    LLONG startPos = 0;

    srandom(seed);
    startPos = pid * nPerProc;
    unrankRand(startPos);       //Move forward to the correct starting position

    return startPos;
}

