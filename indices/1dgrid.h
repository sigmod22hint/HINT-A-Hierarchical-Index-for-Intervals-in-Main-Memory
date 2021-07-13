#ifndef _1D_GRID_H_
#define _1D_GRID_H_

#include "../def_global.h"
#include "../containers/relation.h"



class OneDimensionalGrid
{
private:
    PartitionId numPartitions;
    PartitionId numPartitionsMinus1;
    Timestamp gstart;
    Timestamp gend;
    Timestamp partitionExtent;
    RecordId numIndexedRecords;

    Relation *pRecs;
    size_t *pRecs_sizes;

    // Construction
    inline void updateCounters(const Record &rec);
    inline void updatePartitions(const Record &rec);

public:
    // Statistics
    PartitionId numEmptyPartitions;
    float avgPartitionSize;
    size_t numReplicas;
    
    // Construction
    inline void updatePartitionsUpdates(const Record &rec)
    {
        auto sP = (rec.start == this->gend) ? this->numPartitionsMinus1 : rec.start/this->partitionExtent;
        auto eP = (rec.end   == this->gend) ? this->numPartitionsMinus1 : rec.end/this->partitionExtent;
    
        this->pRecs[sP].push_back(rec);
        while (sP != eP)
        {
            sP++;
            this->pRecs[sP].push_back(rec);
        }
    };

    OneDimensionalGrid(const Relation &R, const PartitionId numPartitions);
    OneDimensionalGrid(const Relation &R, const Relation &M, const PartitionId numPartitions);   // Only for updates
    void print(char c);
    void getStats();
    ~OneDimensionalGrid();

    // Querying
    size_t execute(StabbingQuery Q);
    size_t execute(RangeQuery Q);
};
#endif // _1D_GRID_H_
