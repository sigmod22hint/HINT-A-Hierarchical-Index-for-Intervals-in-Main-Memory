#ifndef _PERIODINDEX_H_
#define _PERIODINDEX_H_

#include "../def_global.h"
#include "../containers/relation.h"
using namespace std; 



class AdaptivePeriodIndex
{
private:
    unsigned int numBuckets;
    unsigned int numLevels;
    unsigned int sizeBucket;
    int granS;
    int *startCounters;
    int **endCounters;
    int *durations;
    int *histogram;

    int **bucketcounters;
    Relation **buckets;
    int min, max, granule;
    long int IntervalsPerBucket;




public:
    Relation *pR;

    int numIndexedRecords;
    size_t numPartitions;
    size_t numEmptyPartitions;
    float avgPartitionSize;
    size_t sizeInBytes;
    int numOriginals, numReplicas;

    AdaptivePeriodIndex(Relation &R, const unsigned int numLevels, const unsigned int numBuckets);
    AdaptivePeriodIndex(Relation &R, Relation &M, const unsigned int numLevels, const unsigned int numBuckets);// {cout<<"hello"<<endl;};
    
    inline void updateCounters(const Record &rec);
    void updateBuckets(const Record &rec);
    inline void updateHistogram(const Record &rec);  

    void getStats();

    size_t execute(StabbingQuery Q);
    size_t execute(RangeQuery Q);
};
#endif // _PERIODINDEX_H_
