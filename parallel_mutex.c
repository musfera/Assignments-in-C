#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <sys/time.h>

#define NUM_BUCKETS 5     // Buckets in hash table
#define NUM_KEYS 100000   // Number of keys inserted per thread
int num_threads = 1;      // Number of threads (configurable)
int keys[NUM_KEYS];
pthread_mutex_t lock[NUM_BUCKETS];

typedef struct _bucket_entry {
    int key;
    int val;
    struct _bucket_entry *next;
} bucket_entry;

/*
pthread_mutex_t lock; //declare
pthread_mutex_init(&lock, NULL); //initialize 
pthread_mutex_lock(&lock); //acquire 
pthread_mutex_unlock(&lock); //release

*/

bucket_entry *table[NUM_BUCKETS];


void panic(char *msg) {
    printf("%s\n", msg);
    exit(1);
}

double now() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Inserts a key-value pair into the table
//make an array to store all the locks, so when inserting into the buckets
//multiple locks  
void insert(int key, int val) {
    int i = key % NUM_BUCKETS;
    bucket_entry *e = (bucket_entry *) malloc(sizeof(bucket_entry));
    if (!e) panic("No memory to allocate bucket!");
pthread_mutex_lock(&lock[i]); //acquire a lock, so no one else can change the table 
    e->next = table[i];
    e->key = key;
    e->val = val;
    table[i] = e;
pthread_mutex_unlock(&lock[i]); //release after its done
}

// Retrieves an entry from the hash table by key
// Returns NULL if the key isn't found in the table

//locks are not necessary in the retrieve function 
bucket_entry * retrieve(int key) {
//pthread_mutex_lock(&lock); //acquire a lock, so no one else can change
    bucket_entry *b;
    for (b = table[key % NUM_BUCKETS]; b != NULL; b = b->next) {
        if (b->key == key) {
	//pthread_mutex_unlock(&lock); //release after its done
	return b;
	}
    }
//pthread_mutex_unlock(&lock); //release after its done
    return NULL;
}

void * put_phase(void *arg) {
    long tid = (long) arg;
    int key = 0;

    // If there are k threads, thread i inserts
    //      (i, i), (i+k, i), (i+k*2)
    for (key = tid ; key < NUM_KEYS; key += num_threads) {
        insert(keys[key], tid);
    }

    pthread_exit(NULL);
}

void * get_phase(void *arg) {

    long tid = (long) arg;
    int key = 0;
    long lost = 0;

    for (key = tid ; key < NUM_KEYS; key += num_threads) {
        if (retrieve(keys[key]) == NULL) lost++;
    }
    printf("[thread %ld] %ld keys lost!\n", tid, lost);

    pthread_exit((void *)lost);
}

int main(int argc, char **argv) {
    long i;
    pthread_t *threads;
    double start, end;
int incr;
for (incr = 0; incr < sizeof(lock); incr++){

pthread_mutex_init(&lock[incr], NULL); //initialize the lock

}
    if (argc != 2) {
        panic("usage: ./parallel_hashtable <num_threads>");
    }
    if ((num_threads = atoi(argv[1])) <= 0) {
        panic("must enter a valid number of threads to run");
    }

    srandom(time(NULL));
    for (i = 0; i < NUM_KEYS; i++)
        keys[i] = random();

    threads = (pthread_t *) malloc(sizeof(pthread_t)*num_threads);
    if (!threads) {
        panic("out of memory allocating thread handles");
    }


    // Insert keys in parallel
    start = now();
//pthread_mutex_init(&lock, NULL); //initialize 
    for (i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, put_phase, (void *)i);
    }
    
    // Barrier
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    end = now();
    
    printf("[main] Inserted %d keys in %f seconds\n", NUM_KEYS, end - start);
    
    // Reset the thread array
    memset(threads, 0, sizeof(pthread_t)*num_threads);

    // Retrieve keys in parallel
    start = now();
    for (i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, get_phase, (void *)i);
    }

    // Collect count of lost keys
    long total_lost = 0;
    long *lost_keys = (long *) malloc(sizeof(long) * num_threads);
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], (void **)&lost_keys[i]);
        total_lost += lost_keys[i];
    }
    end = now();

    printf("[main] Retrieved %ld/%d keys in %f seconds\n", NUM_KEYS - total_lost, NUM_KEYS, end - start);

    return 0;
}
/*
1. Using a mutex is much faster, for single as well as mutliple threads 
2. When 8 threads are run with the mutex, it finishes in 9.92 seconds, while the spinlock finishes in 69.046 seconds. The mutex is much faster 
3. When retrieving an item from the hash table, a lock is not required because nothing is being changed, the table is only being accessed with get_phase 
4. With parallel insertion, there is an array of locks for each bucket, to keep track of all parallel buckets 
The keys need to be protected by the mutexes so more than one process cant use the same key at the same time. 

*/
