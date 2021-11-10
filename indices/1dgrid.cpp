#include "1dgrid.h"



inline void OneDimensionalGrid::updateCounters(const Record &rec)
{
    auto sP = (rec.start == this->gend) ? this->numPartitionsMinus1 : rec.start/this->partitionExtent;
    auto eP = (rec.end   == this->gend) ? this->numPartitionsMinus1 : rec.end/this->partitionExtent;
    
    this->pRecs_sizes[sP]++;
    while (sP != eP)
    {
        sP++;
        this->pRecs_sizes[sP]++;
    }
}


inline void OneDimensionalGrid::updatePartitions(const Record &rec)
{
    auto sP = (rec.start == this->gend) ? this->numPartitionsMinus1 : rec.start/this->partitionExtent;
    auto eP = (rec.end   == this->gend) ? this->numPartitionsMinus1 : rec.end/this->partitionExtent;
    
    this->pRecs[sP][this->pRecs_sizes[sP]] = rec;
    this->pRecs_sizes[sP]++;
    while (sP != eP)
    {
        sP++;
        this->pRecs[sP][this->pRecs_sizes[sP]] = rec;
        this->pRecs_sizes[sP]++;
    }
}


//inline void OneDimensionalGrid::updatePartitionsUpdates(const Record &rec)
//{
//    auto sP = (rec.start == this->gend) ? this->numPartitionsMinus1 : rec.start/this->partitionExtent;
//    auto eP = (rec.end   == this->gend) ? this->numPartitionsMinus1 : rec.end/this->partitionExtent;
//
//    this->pRecs[sP].push_back(rec);
////    this->pRecs_sizes[sP]++;
//    while (sP != eP)
//    {
//        sP++;
//        this->pRecs[sP].push_back(rec);
////        this->pRecs_sizes[sP]++;
//    }
//}


OneDimensionalGrid::OneDimensionalGrid(const Relation &R, const PartitionId numPartitions)
{
    // Initialize statistics.
    this->numIndexedRecords   = R.size();
    this->numPartitions       = numPartitions;
    this->numPartitionsMinus1 = this->numPartitions-1;
    this->numEmptyPartitions  = 0;
    this->avgPartitionSize    = 0;
    this->gstart              = R.gstart;
    this->gend                = R.gend;
    this->partitionExtent     = (Timestamp)ceil((double)(this->gend-this->gstart)/this->numPartitions);
    this->numReplicas = 0;

    // Step 1: one pass to count the contents inside each partition.
    this->pRecs_sizes = (size_t*)calloc(this->numPartitions, sizeof(size_t));
    
    for (const Record &r : R)
        this->updateCounters(r);
    
    // Step 2: allocate necessary memory.
    this->pRecs = new Relation[this->numPartitions];
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        this->pRecs[pId].gstart = this->gstart            + pId*this->partitionExtent;
        this->pRecs[pId].gend   = this->pRecs[pId].gstart + this->partitionExtent;
        this->pRecs[pId].resize(this->pRecs_sizes[pId]);
    }
    memset(this->pRecs_sizes, 0, this->numPartitions*sizeof(size_t));
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    
    // Free auxiliary memory.
    free(pRecs_sizes);
}


// For updates only
OneDimensionalGrid::OneDimensionalGrid(const Relation &R, const Relation &M, const PartitionId numPartitions)
{
    // Initialize statistics.
    this->numIndexedRecords   = R.size();
    this->numPartitions       = numPartitions;
    this->numPartitionsMinus1 = this->numPartitions-1;
    this->numEmptyPartitions  = 0;
    this->avgPartitionSize    = 0;
    this->gstart              = R.gstart;
    this->gend                = R.gend;
    this->partitionExtent     = (Timestamp)ceil((double)(this->gend-this->gstart)/this->numPartitions);
    this->numReplicas = 0;
    
    // Step 1: one pass to count the contents inside each partition.
    this->pRecs_sizes = (size_t*)calloc(this->numPartitions, sizeof(size_t));
    
    for (const Record &r : R)
        this->updateCounters(r);
    for (const Record &r : M)   // To allocate necessary memory for the updates
        this->updateCounters(r);
    
    // Step 2: allocate necessary memory.
    this->pRecs = new Relation[this->numPartitions];
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        this->pRecs[pId].gstart = this->gstart            + pId*this->partitionExtent;
        this->pRecs[pId].gend   = this->pRecs[pId].gstart + this->partitionExtent;
        this->pRecs[pId].reserve(this->pRecs_sizes[pId]);
    }
    memset(this->pRecs_sizes, 0, this->numPartitions*sizeof(size_t));
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->insert(r);
    
    // Free auxiliary memory.
    free(this->pRecs_sizes);
}


void OneDimensionalGrid::print(char c)
{
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        Relation &p = this->pRecs[pId];
        
        cout << "Partition " << pId << " [" << p.gstart << ".." << p.gend << "] (" << p.size() << "):";
        for (size_t i = 0; i < p.size(); i++)
            cout << " r" << p[i].id;
        cout << endl;
    }
}


OneDimensionalGrid::~OneDimensionalGrid()
{
    delete[] this->pRecs;
}


void OneDimensionalGrid::getStats()
{
    for (auto pId = 0; pId < this->numPartitions; pId++)
    {
        this->numReplicas += this->pRecs[pId].size();

        if (this->pRecs[pId].empty())
            this->numEmptyPartitions++;
    }
    
    this->avgPartitionSize = (float)(this->numReplicas)/this->numPartitions;
    this->numReplicas -= this->numIndexedRecords;
}


size_t OneDimensionalGrid::execute(StabbingQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto pId = (Q.point == this->gend)? this->numPartitionsMinus1: Q.point/this->partitionExtent;
    
    
    // Handle the first partition.
    iterStart = this->pRecs[pId].begin();
    iterEnd   = this->pRecs[pId].end();
    for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
    {
        if ((iter->start <= Q.point) && (Q.point <= iter->end))
            result ^= iter->id;
    }
    
    
    return result;
}


size_t OneDimensionalGrid::execute(RangeQuery Q)
{
    size_t result = 0;
    RelationIterator iterStart, iterEnd;
    auto s_pId = (Q.start == this->gend)? this->numPartitionsMinus1: Q.start/this->partitionExtent;
    auto e_pId = (Q.end   == this->gend)? this->numPartitionsMinus1: Q.end/this->partitionExtent;
    
    
    // Handle the first partition.
    iterStart = this->pRecs[s_pId].begin();
    iterEnd   = this->pRecs[s_pId].end();
    for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
    {
        if ((iter->start <= Q.end) && (Q.start <= iter->end))
            result ^= iter->id;
    }

    // Handle partitions completely contained inside the query range.
    for (auto pId = s_pId+1; pId < e_pId; pId++)
    {
        Relation &p = this->pRecs[pId];
        iterStart = p.begin();
        iterEnd = p.end();
        for (RelationIterator iter = p.begin(); iter != iterEnd; iter++)
        {
            // Perform de-duplication test.
//            if (max(Q.start, iter->start) >= p.gstart)
            if (iter->start >= p.gstart)
                result ^= iter->id;
        }
    }

    // Handle the last partition.
    if (e_pId != s_pId)
    {
        iterStart = this->pRecs[e_pId].begin();
        iterEnd = this->pRecs[e_pId].end();
        for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
        {
//            if ((max(Q.start, iter->start) >= this->pRecs[e_pId].gstart) && (iter->start <= Q.end && Q.start <= iter->end))
            if ((iter->start >= this->pRecs[e_pId].gstart) && (iter->start <= Q.end && Q.start <= iter->end))
                result ^= iter->id;
        }
    }


    return result;
}


void OneDimensionalGrid::insert(const Record &rec)
{
    auto sP = (rec.start == this->gend) ? this->numPartitionsMinus1 : rec.start/this->partitionExtent;
    auto eP = (rec.end   == this->gend) ? this->numPartitionsMinus1 : rec.end/this->partitionExtent;
    
    this->pRecs[sP].push_back(rec);
    while (sP != eP)
    {
        sP++;
        this->pRecs[sP].push_back(rec);
    }
}


void OneDimensionalGrid::remove(const Record &rec)
{
    auto s_pId = (rec.start == this->gend)? this->numPartitionsMinus1: rec.start/this->partitionExtent;
    auto e_pId = (rec.end   == this->gend)? this->numPartitionsMinus1: rec.end/this->partitionExtent;
    
    
    // Handle the first partition.
    for (auto iter = this->pRecs[s_pId].begin(); iter != this->pRecs[s_pId].end(); ++iter)
    {
        if (iter->id == rec.id){
            iter->id = 0;
            break;
        }
    }

    // Handle partitions completely contained inside the query range.
    for (auto pId = s_pId+1; pId < e_pId; pId++)
    {
        Relation &p = this->pRecs[pId];
        for (auto iter = p.begin(); iter != p.end(); iter++)
        {
            if (iter->id == rec.id){
                iter->id = 0;
                break;
            }
        }
    }

    // Handle the last partition.
    if (e_pId != s_pId)
    {
        for (auto iter = this->pRecs[e_pId].begin(); iter != this->pRecs[e_pId].end(); iter++)
        {
            if (iter->id == rec.id){
                iter->id = 0;
                break;
            }
        }
    }

}
