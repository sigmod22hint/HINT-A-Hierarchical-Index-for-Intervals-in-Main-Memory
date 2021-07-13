#ifndef _HINT_H_
#define _HINT_H_

#include "../def_global.h"
#include "../containers/relation.h"
#include "../indices/hierarchicalindex.h"



// Base HINT, no optimizations activated
class HINT : public HierarchicalIndex
{
private:
    RelationId **pOrgs, **pReps;
    RecordId   **pOrgs_sizes;
    size_t     **pReps_sizes;

    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);

public:
    // Construction
    inline void updatePartitionsUpdates(const Record &r)
    {
        int level = 0;
        Timestamp a = r.start >> (this->maxBits-this->numBits);
        Timestamp b = r.end   >> (this->maxBits-this->numBits);
        Timestamp prevb;
        int firstfound = 0;
        
        
        while (level < this->height && a <= b)
        {
            if (a%2)
            { //last bit of a is 1
                if (firstfound)
                {
                    this->pReps[level][a].push_back(r.id);
                    //                this->pReps_sizes[level][a]++;
                }
                else
                {
                    this->pOrgs[level][a].push_back(r.id);
                    //                this->pOrgs_sizes[level][a]++;
                    firstfound = 1;
                }
                //a+=(int)(pow(2,level));
                a++;
            }
            if (!(b%2))
            { //last bit of b is 0
                prevb = b;
                b--;
                //b-=(int)(pow(2,level));
                if ((!firstfound) && b < a)
                {
                    this->pOrgs[level][prevb].push_back(r.id);
                    //                this->pOrgs_sizes[level][prevb]++;
                }
                else
                {
                    this->pReps[level][prevb].push_back(r.id);
                    //                this->pReps_sizes[level][prevb]++;
                }
            }
            a >>= 1; // a = a div 2
            b >>= 1; // b = b div 2
            level++;
        }
    };

    HINT(const Relation &R, const unsigned int maxBits);
    HINT(const Relation &R, const Relation &M, const unsigned int maxBits);   // For updates only
    void print(const char c);
    void getStats();
    ~HINT();
    
    // Querying
    size_t execute(StabbingQuery Q);
    size_t execute(RangeQuery Q);
};



// HINT with the skewness & sparsity optimization activated
class HINT_SS : public HierarchicalIndex
{
private:
    RelationId *pOrgs, *pReps;
    RecordId   **pOrgs_sizes;
    size_t     **pReps_sizes;
    RecordId   **pOrgs_offsets;
    size_t     **pReps_offsets;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> > *pOrgs_ioffsets, *pReps_ioffsets;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
    // Querying
    inline void scanPartition_Orgs(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanPartition_Reps(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanPartitions_Orgs(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result);
    
public:
    // Construction
    HINT_SS(const Relation &R, const unsigned int maxBits);
    void getStats();
    ~HINT_SS();
    
    // Querying
    size_t execute(StabbingQuery Q);
    size_t execute(RangeQuery Q);
};
#endif // _HINT_H_
