#ifndef _HINT_M_H_
#define _HINT_M_H_

#include "../def_global.h"
#include "../containers/relation.h"
#include "../indices/hierarchicalindex.h"



// Base HINT^m, no optimizations activated
class HINT_M : public HierarchicalIndex
{
private:
    Relation **pOrgs, **pReps;
    RecordId **pOrgs_sizes;
    size_t   **pReps_sizes;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
public:
    // Construction
    HINT_M(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    void print(const char c);
    void getStats();
    ~HINT_M();
    
    // Querying
    size_t executeTopDown(RangeQuery Q);
    size_t executeBottomUp(RangeQuery Q);
};


// HINT^m with subs+sort optimization activated
class HINT_M_SubsSort : public HierarchicalIndex
{
private:
    Relation **pOrgsIn;
    Relation **pOrgsOut;
    Relation **pRepsIn;
    Relation **pRepsOut;
    RecordId **pOrgsIn_sizes, **pOrgsOut_sizes;
    size_t   **pRepsIn_sizes, **pRepsOut_sizes;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
public:
    // Construction
    HINT_M_SubsSort(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    void getStats();
    ~HINT_M_SubsSort();
    
    // Querying
    size_t executeBottomUp(RangeQuery Q);
};



// HINT^m with subs+sopt optimizations activated
class HINT_M_SubsSopt : public HierarchicalIndex
{
private:
    Relation      **pOrgsIn;
    RelationStart **pOrgsOut;
    RelationEnd   **pRepsIn;
    RelationId    **pRepsOut;
    RecordId      **pOrgsIn_sizes, **pOrgsOut_sizes;
    size_t        **pRepsIn_sizes, **pRepsOut_sizes;
    
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
        
        
        //    r.print('r');
        while (level < this->height && a <= b)
        {
            if (a%2)
            { //last bit of a is 1
                if (firstfound)
                {
                    if (a == b)
                    {
                        this->pRepsIn[level][a].push_back(RecordEnd(r.id, r.end));
                    }
                    else
                    {
                        this->pRepsOut[level][a].push_back(r.id);
                    }
                }
                else
                {
                    if (a == b)
                    {
                        this->pOrgsIn[level][a].push_back(Record(r.id, r.start, r.end));
                    }
                    else
                    {
                        this->pOrgsOut[level][a].push_back(RecordStart(r.id, r.start));
                    }
                    firstfound = 1;
                }
                //a+=(int)(pow(2,level));
                a++;
            }
            if (!(b%2))
            { //last bit of b is 0
                prevb = b;
                //b-=(int)(pow(2,level));
                b--;
                if ((!firstfound) && b < a)
                {
                    this->pOrgsIn[level][prevb].push_back(Record(r.id, r.start, r.end));
                }
                else
                {
                    this->pRepsIn[level][prevb].push_back(RecordEnd(r.id, r.end));
                }
            }
            a >>= 1; // a = a div 2
            b >>= 1; // b = b div 2
            level++;
        }
    };
    
    HINT_M_SubsSopt(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    HINT_M_SubsSopt(const Relation &R, const Relation &M, const unsigned int numBits, const unsigned int maxBits);   // Only for updates
    void print(const char c);
    void getStats();
    ~HINT_M_SubsSopt();
    
    // Querying
    size_t executeBottomUp(RangeQuery Q);
};



// HINT^m with subs+sort+sopt optimizations activated
class HINT_M_SubsSortSopt : public HierarchicalIndex
{
private:
    Relation      **pOrgsIn;
    RelationStart **pOrgsOut;
    RelationEnd   **pRepsIn;
    RelationId    **pRepsOut;
    RecordId      **pOrgsIn_sizes, **pOrgsOut_sizes;
    size_t        **pRepsIn_sizes, **pRepsOut_sizes;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
public:
    // Construction
    HINT_M_SubsSortSopt(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    void print(const char c);
    void getStats();
    ~HINT_M_SubsSortSopt();
    
    // Querying
    size_t executeBottomUp(RangeQuery Q);
};



// HINT^m with subs+sort+sopt and skewness & sparsity optimizations activated
class HINT_M_SubsSortSopt_SS : public HierarchicalIndex
{
private:
    Relation      *pOrgsIn;
    RelationStart *pOrgsOut;
    RelationEnd   *pRepsIn;
    RelationId    *pRepsOut;
    RecordId      **pOrgsIn_sizes, **pOrgsOut_sizes;
    size_t        **pRepsIn_sizes, **pRepsOut_sizes;
    RecordId      **pOrgsIn_offsets, **pOrgsOut_offsets;
    size_t        **pRepsIn_offsets, **pRepsOut_offsets;
    vector<tuple<Timestamp, RelationIterator, PartitionId> >      *pOrgsIn_ioffsets;
    vector<tuple<Timestamp, RelationStartIterator, PartitionId> > *pOrgsOut_ioffsets;
    vector<tuple<Timestamp, RelationEndIterator, PartitionId> >   *pRepsIn_ioffsets;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> >    *pRepsOut_ioffsets;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
    // Querying
    inline void scanFirstPartition_OrgsIn(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn(unsigned int level, Timestamp a, Timestamp qstart, Record qdummyE, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsOut(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsIn(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsIn(unsigned int level, Timestamp a, RecordEnd qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsOut(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanPartitions_OrgsIn(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result);
    inline void scanPartitions_OrgsOut(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result);
    inline void scanLastPartition_OrgsIn(unsigned int level, Timestamp b, Record qdummyE, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_OrgsOut(unsigned int level, Timestamp b, RecordStart qdummySE, PartitionId &next_from, size_t &result);
    
public:
    // Construction
    HINT_M_SubsSortSopt_SS(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    ~HINT_M_SubsSortSopt_SS();
    
    // Querying
    size_t executeBottomUp(RangeQuery Q);
};



// HINT^m with subs+sort+sopt and cash misses optimizations activated
class HINT_M_SubsSortSopt_CM : public HierarchicalIndex
{
private:
    Relation      **pOrgsInTmp;
    RelationStart **pOrgsOutTmp;
    RelationEnd   **pRepsInTmp;
    RelationId    **pOrgsInIds;
    vector<pair<Timestamp, Timestamp> > **pOrgsInTimestamps;
    RelationId    **pOrgsOutIds;
    vector<Timestamp> **pOrgsOutTimestamp;
    RelationId    **pRepsInIds;
    vector<Timestamp> **pRepsInTimestamp;
    RelationId    **pRepsOut;
    RecordId      **pOrgsIn_sizes, **pOrgsOut_sizes;
    size_t        **pRepsIn_sizes, **pRepsOut_sizes;
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
public:
    // Construction
    HINT_M_SubsSortSopt_CM(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    ~HINT_M_SubsSortSopt_CM();
    
    // Querying
    size_t executeBottomUp(RangeQuery Q);
};



// HINT^m with all optimizations activated
class HINT_M_ALL : public HierarchicalIndex
{
private:
    Relation      *pOrgsInTmp;
    RelationStart *pOrgsOutTmp;
    RelationEnd   *pRepsInTmp;
    
    RelationId    *pOrgsInIds;
    vector<pair<Timestamp, Timestamp> > *pOrgsInTimestamps;
    RelationId    *pOrgsOutIds;
    vector<Timestamp> *pOrgsOutTimestamp;
    RelationId    *pRepsInIds;
    vector<Timestamp> *pRepsInTimestamp;
    RelationId    *pRepsOut;
    
    RecordId      **pOrgsIn_sizes, **pOrgsOut_sizes;
    size_t        **pRepsIn_sizes, **pRepsOut_sizes;
    RecordId      **pOrgsIn_offsets, **pOrgsOut_offsets;
    size_t        **pRepsIn_offsets, **pRepsOut_offsets;
    vector<tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> >   *pOrgsIn_ioffsets;
    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> > *pOrgsOut_ioffsets;
    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> > *pRepsIn_ioffsets;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> > *pRepsOut_ioffsets;
    
    
    // Construction
    inline void updateCounters(const Record &r);
    inline void updatePartitions(const Record &r);
    
    // Querying
    inline void scanFirstPartition_OrgsIn(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn(unsigned int level, Timestamp a, Timestamp qstart, pair<Timestamp, Timestamp> qdummyE, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsIn(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_OrgsOut(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsIn(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsIn(unsigned int level, Timestamp a, RecordEnd qdummyS, PartitionId &next_from, size_t &result);
    inline void scanFirstPartition_RepsOut(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result);
    inline void scanPartitions_OrgsIn(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result);
    inline void scanPartitions_OrgsOut(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result);
    inline void scanLastPartition_OrgsIn(unsigned int level, Timestamp b, pair<Timestamp, Timestamp> qdummyE, PartitionId &next_from, size_t &result);
    inline void scanLastPartition_OrgsOut(unsigned int level, Timestamp b, RecordStart qdummySE, PartitionId &next_from, size_t &result);
    
public:    
    // Construction
    HINT_M_ALL(const Relation &R, const unsigned int numBits, const unsigned int maxBits);
    ~HINT_M_ALL();
    
    // Querying
    size_t executeBottomUp(StabbingQuery Q);
    size_t executeBottomUp(RangeQuery Q);
};
#endif // _HINT_M_H_
