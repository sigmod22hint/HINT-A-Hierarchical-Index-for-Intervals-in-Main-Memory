#ifndef _HIERARCHICALINDEX_H_
#define _HIERARCHICALINDEX_H_

#include "../def_global.h"
#include "../containers/relation.h"



// Framework
class HierarchicalIndex
{
protected:
    size_t numIndexedRecords;
    unsigned int numBits;
    unsigned int maxBits;
    unsigned int height;
    
    // Construction
    virtual inline void updateCounters(const Record &r) {};
    virtual inline void updatePartitions(const Record &r) {};

public:
    // Statistics
    size_t numPartitions;
    size_t numEmptyPartitions;
    float avgPartitionSize;
    size_t numOriginals, numReplicas;
    size_t numOriginalsIn, numOriginalsOut, numReplicasIn, numReplicasOut;

    // Construction
    HierarchicalIndex(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    virtual void print(const char c) {};
    virtual void getStats() {};
    virtual ~HierarchicalIndex() {};
    
    // Querying
    virtual size_t execute(StabbingQuery Q) {};
    virtual size_t execute(RangeQuery Q) {};

    virtual size_t executeTopDown(StabbingQuery Q) {};
    virtual size_t executeTopDown(RangeQuery Q) {};
    virtual size_t executeBottomUp(StabbingQuery Q) {};
    virtual size_t executeBottomUp(RangeQuery Q) {};
    
    // Updating
    virtual void insert(const Record &r) {};
    virtual void remove(const Record &r) {};
};
#endif // _HIERARCHICALINDEX_H_
