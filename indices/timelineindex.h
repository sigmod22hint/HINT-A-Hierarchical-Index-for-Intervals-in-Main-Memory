#ifndef _TIMELINEINDEX_H_
#define _TIMELINEINDEX_H_

#include "../def_global.h"
#include "../containers/relation.h"
#include <boost/dynamic_bitset.hpp>
#include "../containers/endpoint_index.h"
#include <unordered_set>
using namespace std; 



class CheckPoint
{
private:

public:

    int* validIntervalIds;
    int checkpointTimestamp;
    int checkpointSpot;
    CheckPoint();
};



class TimelineIndex
{
private:

public:
    RecordId numRecords;
    int* validIntervalCount;
    CheckPoint* VersionMap;
    unsigned int numCheckpoints;
    int min,max;
    EndPointIndex eventList;
    int checkpointFrequency;

    // Construction
    TimelineIndex(const Relation &R, const unsigned int numCheckpoints);
    void getStats();
    ~TimelineIndex();

    // Querying
    size_t execute(StabbingQuery Q);
    size_t execute(RangeQuery Q);
};
#endif // _PERIODINDEX_H_
