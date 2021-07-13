#include "hint_m.h"



inline bool CompareTimestampsStart2(const pair<Timestamp, Timestamp> &lhs, const pair<Timestamp, Timestamp> &rhs)
{
    return (lhs.first < rhs.first);
}

inline bool CompareStartEnd(const tuple<Timestamp, RelationIterator, PartitionId> &lhs, const tuple<Timestamp, RelationIterator, PartitionId> &rhs)
{
    return (get<0>(lhs) < get<0>(rhs));
}

inline bool CompareStartEnd2(const tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> &lhs, const tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> &rhs)
{
    return (get<0>(lhs) < get<0>(rhs));
}

inline bool CompareStart(const tuple<Timestamp, RelationStartIterator, PartitionId> &lhs, const tuple<Timestamp, RelationStartIterator, PartitionId> &rhs)
{
    return (get<0>(lhs) < get<0>(rhs));
}

inline bool CompareStart2(const tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> &lhs, const tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> &rhs)
{
    return (get<0>(lhs) < get<0>(rhs));
}

inline bool CompareEnd(const tuple<Timestamp, RelationEndIterator, PartitionId> &lhs, const tuple<Timestamp, RelationEndIterator, PartitionId> &rhs)
{
    return (get<0>(lhs) < get<0>(rhs));
}

inline bool CompareEnd2(const tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> &lhs, const tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> &rhs)
{
    return (get<0>(lhs) < get<0>(rhs));
}

inline bool CompareId(const tuple<Timestamp, RelationIdIterator, PartitionId> &lhs, const tuple<Timestamp, RelationIdIterator, PartitionId> &rhs)
{
    return (get<0>(lhs) < get<0>(rhs));
}



inline void HINT_M::updateCounters(const Record &r)
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
                //printf("added to level %d, bucket %d, class B\n",level,a);
                this->pReps_sizes[level][a]++;
            }
            else
            {
                //printf("added to level %d, bucket %d, class A\n",level,a);
                this->pOrgs_sizes[level][a]++;
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
            if ((!firstfound) && b<a)
            {
                //printf("added to level %d, bucket %d, class A\n",level,prevb);
                this->pOrgs_sizes[level][prevb]++;
            }
            else
            {
                //printf("added to level %d, bucket %d, class B\n",level,prevb);
                this->pReps_sizes[level][prevb]++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


inline void HINT_M::updatePartitions(const Record &r)
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
                this->pReps[level][a][this->pReps_sizes[level][a]] = r;
                this->pReps_sizes[level][a]++;
            }
            else
            {
                this->pOrgs[level][a][this->pOrgs_sizes[level][a]] = r;
                this->pOrgs_sizes[level][a]++;
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
                this->pOrgs[level][prevb][this->pOrgs_sizes[level][prevb]] = r;
                this->pOrgs_sizes[level][prevb]++;
            }
            else
            {
                this->pReps[level][prevb][this->pReps_sizes[level][prevb]] = r;
                this->pReps_sizes[level][prevb]++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M::HINT_M(const Relation &R, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
{
    // Step 1: one pass to count the contents inside each partition.
    this->pOrgs_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pReps_sizes = (size_t **)malloc(this->height*sizeof(size_t *));

    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgs_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pReps_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);

    
    // Step 2: allocate necessary memory.
    this->pOrgs = new Relation*[this->height];
    this->pReps = new Relation*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));

        this->pOrgs[l] = new Relation[cnt];
        this->pReps[l] = new Relation[cnt];

        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgs[l][j].resize(this->pOrgs_sizes[l][j]);
            this->pReps[l][j].resize(this->pReps_sizes[l][j]);
        }
    }
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        memset(this->pOrgs_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pReps_sizes[l], 0, cnt*sizeof(size_t));
    }


    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);


    // Free auxiliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(pOrgs_sizes[l]);
        free(pReps_sizes[l]);
    }
    free(pOrgs_sizes);
    free(pReps_sizes);
}


void HINT_M::print(char c)
{
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        printf("Level %d: %d partitions\n", l, cnt);
        for (auto j = 0; j < cnt; j++)
        {
            printf("Orgs %d (%d): ", j, this->pOrgs[l][j].size());
//            for (auto k = 0; k < this->bucketcountersA[i][j]; k++)
//                printf("%d ", this->pOrgs[i][j][k].id);
            printf("\n");
            printf("Reps %d (%d): ", j, this->pReps[l][j].size());
//            for (auto k = 0; k < this->bucketcountersB[i][j]; k++)
//                printf("%d ", this->pReps[i][j][k].id);
            printf("\n\n");
        }
    }
}


void HINT_M::getStats()
{
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);

        this->numPartitions += cnt;
        for (int j = 0; j < cnt; j++)
        {
            this->numReplicas += this->pReps[l][j].size();
            if ((this->pOrgs[l][j].empty()) && (this->pReps[l][j].empty()))
                this->numEmptyPartitions++;
        }
    }

    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicas)/(this->numPartitions-numEmptyPartitions);
}


HINT_M::~HINT_M()
{
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgs[l];
        delete[] this->pReps[l];
    }
    delete[] this->pOrgs;
    delete[] this->pReps;
}


size_t HINT_M::executeTopDown(RangeQuery Q)
{
    size_t result = 0;
    Relation::iterator iterStart, iterEnd;
    RelationIterator iter;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        // Handle the partition that contains a: consider both originals and replicas, comparisons needed
        iterStart = this->pOrgs[l][a].begin();
        iterEnd = this->pOrgs[l][a].end();
        for (iter = iterStart; iter != iterEnd; iter++)
        {
            if ((iter->start <= Q.end) && (Q.start <= iter->end))
                result ^= iter->id;
        }
        
        iterStart = this->pReps[l][a].begin();
        iterEnd = this->pReps[l][a].end();
        for (iter = iterStart; iter != iterEnd; iter++)
        {
            if ((iter->start <= Q.end) && (Q.start <= iter->end))
                result ^= iter->id;
        }
        
        if (a < b)
        {
            // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
            for (auto j = a+1; j < b; j++)
            {
                iterStart = this->pOrgs[l][j].begin();
                iterEnd = this->pOrgs[l][j].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
            }
            
            // Handle the partition that contains b: consider only originals, comparisons needed
            iterStart = this->pOrgs[l][b].begin();
            iterEnd = this->pOrgs[l][b].end();
            for (iter = iterStart; iter != iterEnd; iter++)
            {
                if (iter->start <= Q.end)
                    result ^= iter->id;
            }
        }
        
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root: consider only originals, comparisons needed
    iterStart = this->pOrgs[this->numBits][0].begin();
    iterEnd = this->pOrgs[this->numBits][0].end();
    for (iter = iterStart; iter != iterEnd; iter++)
    {
        if ((iter->start <= Q.end) && (Q.start <= iter->end))
            result ^= iter->id;
    }
    
    
    return result;
}


size_t HINT_M::executeBottomUp(RangeQuery Q)
{
    size_t result = 0;
    Relation::iterator iterStart;
    RelationIterator iter, iterEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            iterStart = this->pReps[l][a].begin();
            iterEnd = this->pReps[l][a].end();
            for (iter = iterStart; iter != iterEnd; iter++)
                result ^= iter->id;
            
            // Handle rest: consider only originals
            for (auto j = a; j <= b; j++)
            {
                iterStart = this->pOrgs[l][j].begin();
                iterEnd = this->pOrgs[l][j].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
            }
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                if (!foundzero && !foundone)
                {
                    iterStart = this->pOrgs[l][a].begin();
                    iterEnd = this->pOrgs[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if ((iter->start <= Q.end) && (Q.start <= iter->end))
                            result ^= iter->id;
                    }
                }
                else if (foundzero)
                {
                    iterStart = this->pOrgs[l][a].begin();
                    iterEnd = this->pOrgs[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (iter->start <= Q.end)
                            result ^= iter->id;
                    }
                }
                else if (foundone)
                {
                    iterStart = this->pOrgs[l][a].begin();
                    iterEnd = this->pOrgs[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (Q.start <= iter->end)
                            result ^= iter->id;
                    }
                }
            }
            else
            {
                // Lemma 1
                if (!foundzero)
                {
                    iterStart = this->pOrgs[l][a].begin();
                    iterEnd = this->pOrgs[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (Q.start <= iter->end)
                            result ^= iter->id;
                    }
                }
                else
                {
                    iterStart = this->pOrgs[l][a].begin();
                    iterEnd = this->pOrgs[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= iter->id;
                }
            }
            
            // Lemma 1, 3
            if (!foundzero)
            {
                //TODO with
                iterStart = this->pReps[l][a].begin();
                iterEnd = this->pReps[l][a].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                {
                    if (Q.start <= iter->end)
                        result ^= iter->id;
                }
            }
            else
            {
                iterStart = this->pReps[l][a].begin();
                iterEnd = this->pReps[l][a].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
            }
            
            if (a < b)
            {
                if (!foundone)
                {
                    // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                    for (auto j = a+1; j < b; j++)
                    {
                        iterStart = this->pOrgs[l][j].begin();
                        iterEnd = this->pOrgs[l][j].end();
                        for (iter = iterStart; iter != iterEnd; iter++)
                            result ^= iter->id;
                    }
                    
                    // Handle the partition that contains b: consider only originals, comparisons needed
                    iterStart = this->pOrgs[l][b].begin();
                    iterEnd = this->pOrgs[l][b].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (iter->start <= Q.end)
                            result ^= iter->id;
                    }
                }
                else
                {
                    for (auto j = a+1; j <= b; j++)
                    {
                        iterStart = this->pOrgs[l][j].begin();
                        iterEnd = this->pOrgs[l][j].end();
                        for (iter = iterStart; iter != iterEnd; iter++)
                            result ^= iter->id;
                    }
                }
            }
            
            if ((!foundone) && (b%2)) //last bit of b is 1
                foundone = 1;
            if ((!foundzero) && (!(a%2))) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterStart = this->pOrgs[this->numBits][0].begin();
        iterEnd = this->pOrgs[this->numBits][0].end();
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= iter->id;
    }
    else
    {
        // Comparisons needed
        iterStart = this->pOrgs[this->numBits][0].begin();
        iterEnd = this->pOrgs[this->numBits][0].end();
        for (iter = iterStart; iter != iterEnd; iter++)
        {
            if ((iter->start <= Q.end) && (Q.start <= iter->end))
                result ^= iter->id;
        }
    }
    
    
    return result;
}



inline void HINT_M_SubsSort::updateCounters(const Record &r)
{
    int level = 0;
    Timestamp a = r.start >> (this->maxBits-this->numBits);
    Timestamp b = r.end   >> (this->maxBits-this->numBits);
    Timestamp prevb;
    int firstfound = 0;
    auto count = 0;
    
    
    //    r.print('r');
    while (level < this->height && a <= b)
    {
        if (a%2)
        { //last bit of a is 1
            if (firstfound)
            {
//                printf("up: added to level %d, partition %d, class B, a = %d, b = %d\n", level, a, a, b);
                if (a == b)
                    this->pRepsIn_sizes[level][a]++;
                else
                    this->pRepsOut_sizes[level][a]++;
                count++;
            }
            else
            {
//                printf("up: added to level %d, partition %d, class A, a = %d, b = %d\n", level, a, a, b);
                if (a == b)
                    this->pOrgsIn_sizes[level][a]++;
                else
                    this->pOrgsOut_sizes[level][a]++;
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
//                printf("down: added to level %d, partition %d, class A, a = %d, b = %d\n", level, prevb, a, prevb);
//                this->pOrgsOut_sizes[level][prevb]++;
                this->pOrgsIn_sizes[level][prevb]++;
            }
            else
            {
//                printf("down: added to level %d, partition %d, class B, a = %d, b = %d\n", level, prevb, a, prevb);
//                if (a == prevb)
                this->pRepsIn_sizes[level][prevb]++;
//                else
//                    this->pRepsOut_sizes[level][prevb]++;
                count++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
    
    //    cout << r.id << "\t" << count << endl;
}


inline void HINT_M_SubsSort::updatePartitions(const Record &r)
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
                    this->pRepsIn[level][a][this->pRepsIn_sizes[level][a]] = r;
                    this->pRepsIn_sizes[level][a]++;
                }
                else
                {
                    this->pRepsOut[level][a][this->pRepsOut_sizes[level][a]] = r;
                    this->pRepsOut_sizes[level][a]++;
                }
            }
            else
            {
                if (a == b)
                {
                    this->pOrgsIn[level][a][this->pOrgsIn_sizes[level][a]] = r;
                    this->pOrgsIn_sizes[level][a]++;
                }
                else
                {
                    this->pOrgsOut[level][a][this->pOrgsOut_sizes[level][a]] = r;
                    this->pOrgsOut_sizes[level][a]++;
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
//                this->pOrgsOut[level][prevb][this->pOrgsOut[level][prevb].numRecords] = RecordStart(r.id, r.start);
//                this->pOrgsOut[level][prevb].numRecords++;
                this->pOrgsIn[level][prevb][this->pOrgsIn_sizes[level][prevb]] = r;
                this->pOrgsIn_sizes[level][prevb]++;
            }
            else
            {
//                if (a == prevb)
//                {
                this->pRepsIn[level][prevb][this->pRepsIn_sizes[level][prevb]] = r;
                this->pRepsIn_sizes[level][prevb]++;
//                }
//                else
//                {
//                    this->pRepsOut[level][prevb][this->pRepsOut[level][prevb].numRecords] = r.id;
//                    this->pRepsOut[level][prevb].numRecords++;
//                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M_SubsSort::HINT_M_SubsSort(const Relation &R, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
{
    // Initialize statistics
    this->numOriginalsIn = this->numOriginalsOut = this->numReplicasIn = this->numReplicasOut = 0;
    

    // Step 1: one pass to count the contents inside each partition.
    this->pOrgsIn_sizes  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsOut_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_sizes  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsOut_sizes = (size_t **)malloc(this->height*sizeof(size_t *));
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgsIn_sizes[l]  = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pOrgsOut_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pRepsIn_sizes[l]  = (size_t *)calloc(cnt, sizeof(size_t));
        this->pRepsOut_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);
    

    // Step 2: allocate necessary memory.
    this->pOrgsIn  = new Relation*[this->height];
    this->pOrgsOut = new Relation*[this->height];
    this->pRepsIn  = new Relation*[this->height];
    this->pRepsOut = new Relation*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsIn[l]  = new Relation[cnt];
        this->pOrgsOut[l] = new Relation[cnt];
        this->pRepsIn[l]  = new Relation[cnt];
        this->pRepsOut[l] = new Relation[cnt];
        
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsIn[l][j].resize(this->pOrgsIn_sizes[l][j]);
            this->pOrgsOut[l][j].resize(this->pOrgsOut_sizes[l][j]);
            this->pRepsIn[l][j].resize(this->pRepsIn_sizes[l][j]);
            this->pRepsOut[l][j].resize(this->pRepsOut_sizes[l][j]);
        }
    }
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        memset(this->pOrgsIn_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pOrgsOut_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pRepsIn_sizes[l], 0, cnt*sizeof(size_t));
        memset(this->pRepsOut_sizes[l], 0, cnt*sizeof(size_t));
    }
    

    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    

    // Step 4: sort partition contents.
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsIn[l][j].sortByStart();
            this->pOrgsOut[l][j].sortByStart();
            this->pRepsIn[l][j].sortByEnd();
        }
    }
    
    
    // Free auxiliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(pOrgsIn_sizes[l]);
        free(pOrgsOut_sizes[l]);
        free(pRepsIn_sizes[l]);
        free(pRepsOut_sizes[l]);
    }
    free(pOrgsIn_sizes);
    free(pOrgsOut_sizes);
    free(pRepsIn_sizes);
    free(pRepsOut_sizes);
}


HINT_M_SubsSort::~HINT_M_SubsSort()
{
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgsIn[l];
        delete[] this->pOrgsOut[l];
        delete[] this->pRepsIn[l];
        delete[] this->pRepsOut[l];
    }
    delete[] this->pOrgsIn;
    delete[] this->pOrgsOut;
    delete[] this->pRepsIn;
    delete[] this->pRepsOut;
}


void HINT_M_SubsSort::getStats()
{
    size_t sum = 0;
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        this->numPartitions += cnt;
        for (int j = 0; j < cnt; j++)
        {
            this->numOriginalsIn  += this->pOrgsIn[l][j].size();
            this->numOriginalsOut += this->pOrgsOut[l][j].size();
            this->numReplicasIn   += this->pRepsIn[l][j].size();
            this->numReplicasOut  += this->pRepsOut[l][j].size();
            if ((this->pOrgsIn[l][j].empty()) && (this->pOrgsOut[l][j].empty()) && (this->pRepsIn[l][j].empty()) && (this->pRepsOut[l][j].empty()))
                this->numEmptyPartitions++;
        }
    }
    
    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicasIn+this->numReplicasOut)/(this->numPartitions-numEmptyPartitions);
}


inline bool CompareByEnd2(const Record &lhs, const Record &rhs)
{
    if (lhs.end == rhs.end)
        return (lhs.id < rhs.id);
    else
        return (lhs.end < rhs.end);
}


size_t HINT_M_SubsSort::executeBottomUp(RangeQuery Q)
{
    size_t result = 0;
    Relation::iterator iterStart;
    RelationIterator iter, iterEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    Record qdummyE(0, Q.end+1, Q.end+1);
    Record qdummyS(0, Q.start, Q.start);
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            iterStart = this->pRepsIn[l][a].begin();
            iterEnd = this->pRepsIn[l][a].end();
            for (iter = iterStart; iter != iterEnd; iter++)
                result ^= iter->id;
            iterStart =this->pRepsOut[l][a].begin();
            iterEnd = this->pRepsOut[l][a].end();
            for (iter = iterStart; iter != iterEnd; iter++)
                result ^= iter->id;
            
            // Handle rest: consider only originals
            for (auto j = a; j <= b; j++)
            {
                iterStart = this->pOrgsIn[l][j].begin();
                iterEnd = this->pOrgsIn[l][j].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
                iterStart = this->pOrgsOut[l][j].begin();
                iterEnd = this->pOrgsOut[l][j].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
            }
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                if (!foundzero && !foundone)
                {
                    iterStart = this->pOrgsIn[l][a].begin();
                    iterEnd = lower_bound(iterStart, this->pOrgsIn[l][a].end(), qdummyE);
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (Q.start <= iter->end)
                            result ^= iter->id;
                    }
                    iterStart = this->pOrgsOut[l][a].begin();
                    iterEnd = lower_bound(iterStart, this->pOrgsOut[l][a].end(), qdummyE);
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= iter->id;
                }
                else if (foundzero)
                {
                    iterStart = this->pOrgsIn[l][a].begin();
                    iterEnd = lower_bound(iterStart, this->pOrgsIn[l][a].end(), qdummyE);
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= iter->id;
                    iterStart = this->pOrgsOut[l][a].begin();
                    iterEnd = lower_bound(iterStart, this->pOrgsOut[l][a].end(), qdummyE);
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= iter->id;
                }
                else if (foundone)
                {
                    iterStart = this->pOrgsIn[l][a].begin();
                    iterEnd = this->pOrgsIn[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (Q.start <= iter->end)
                            result ^= iter->id;
                    }
                    iterStart = this->pOrgsOut[l][a].begin();
                    iterEnd = this->pOrgsOut[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= iter->id;
                }
            }
            else
            {
                // Lemma 1
                if (!foundzero)
                {
                    iterStart = this->pOrgsIn[l][a].begin();
                    iterEnd = this->pOrgsIn[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (Q.start <= iter->end)
                            result ^= iter->id;
                    }
                }
                else
                {
                    iterStart = this->pOrgsIn[l][a].begin();
                    iterEnd = this->pOrgsIn[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= iter->id;
                }
                iterStart = this->pOrgsOut[l][a].begin();
                iterEnd = this->pOrgsOut[l][a].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
            }
            
            // Lemma 1, 3
            if (!foundzero)
            {
                iterEnd = this->pRepsIn[l][a].end();
                iterStart = lower_bound(this->pRepsIn[l][a].begin(), this->pRepsIn[l][a].end(), qdummyS, CompareByEnd2);
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
            }
            else
            {
                iterStart = this->pRepsIn[l][a].begin();
                iterEnd = this->pRepsIn[l][a].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
            }
            iterStart = this->pRepsOut[l][a].begin();
            iterEnd = this->pRepsOut[l][a].end();
            for (iter = iterStart; iter != iterEnd; iter++)
                result ^= iter->id;
            
            if (a < b)
            {
                if (!foundone)
                {
                    // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                    for (auto j = a+1; j < b; j++)
                    {
                        iterStart = this->pOrgsIn[l][j].begin();
                        iterEnd = this->pOrgsIn[l][j].end();
                        for (iter = iterStart; iter != iterEnd; iter++)
                            result ^= iter->id;
                        iterStart = this->pOrgsOut[l][j].begin();
                        iterEnd = this->pOrgsOut[l][j].end();
                        for (iter = iterStart; iter != iterEnd; iter++)
                            result ^= iter->id;
                    }
                    
                    // Handle the partition that contains b: consider only originals, comparisons needed
                    iterStart = this->pOrgsIn[l][b].begin();
                    iterEnd = lower_bound(iterStart, this->pOrgsIn[l][b].end(), qdummyE);
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= iter->id;
                    iterStart = this->pOrgsOut[l][b].begin();
                    iterEnd = lower_bound(iterStart, this->pOrgsOut[l][b].end(), qdummyE);
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= iter->id;
                }
                else
                {
                    for (auto j = a+1; j <= b; j++)
                    {
                        iterStart = this->pOrgsIn[l][j].begin();
                        iterEnd = this->pOrgsIn[l][j].end();
                        for (iter = iterStart; iter != iterEnd; iter++)
                            result ^= iter->id;
                        iterStart = this->pOrgsOut[l][j].begin();
                        iterEnd = this->pOrgsOut[l][j].end();
                        for (iter = iterStart; iter != iterEnd; iter++)
                            result ^= iter->id;
                    }
                }
            }
            
            if ((!foundone) && (b%2)) //last bit of b is 1
                foundone = 1;
            if ((!foundzero) && (!(a%2))) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterStart = this->pOrgsIn[this->numBits][0].begin();
        iterEnd = this->pOrgsIn[this->numBits][0].end();
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= iter->id;
    }
    else
    {
        // Comparisons needed
        iterStart = this->pOrgsIn[this->numBits][0].begin();
        iterEnd = lower_bound(iterStart, this->pOrgsIn[this->numBits][0].end(), qdummyE);
        for (iter = iterStart; iter != iterEnd; iter++)
        {
            if (Q.start <= iter->end)
                result ^= iter->id;
        }
    }
    
    
    return result;
}



inline void HINT_M_SubsSopt::updateCounters(const Record &r)
{
    int level = 0;
    Timestamp a = r.start >> (this->maxBits-this->numBits);
    Timestamp b = r.end   >> (this->maxBits-this->numBits);
    Timestamp prevb;
    int firstfound = 0;
    auto count = 0;
    
    
    //    r.print('r');
    while (level < this->height && a <= b)
    {
        if (a%2)
        { //last bit of a is 1
            if (firstfound)
            {
//                printf("up: added to level %d, partition %d, class B, a = %d, b = %d\n", level, a, a, b);
                if (a == b)
                    this->pRepsIn_sizes[level][a]++;
                else
                    this->pRepsOut_sizes[level][a]++;
                count++;
            }
            else
            {
//                printf("up: added to level %d, partition %d, class A, a = %d, b = %d\n", level, a, a, b);
                if (a == b)
                    this->pOrgsIn_sizes[level][a]++;
                else
                    this->pOrgsOut_sizes[level][a]++;
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
//                printf("down: added to level %d, partition %d, class A, a = %d, b = %d\n", level, prevb, a, prevb);
//                this->pOrgsOut_sizes[level][prevb]++;
                this->pOrgsIn_sizes[level][prevb]++;
            }
            else
            {
//                printf("down: added to level %d, partition %d, class B, a = %d, b = %d\n", level, prevb, a, prevb);
//                if (a == prevb)
                this->pRepsIn_sizes[level][prevb]++;
//                else
//                    this->pRepsOut_sizes[level][prevb]++;
                count++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
    
    //    cout << r.id << "\t" << count << endl;
}


inline void HINT_M_SubsSopt::updatePartitions(const Record &r)
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
                    this->pRepsIn[level][a][this->pRepsIn_sizes[level][a]] = RecordEnd(r.id, r.end);
                    this->pRepsIn_sizes[level][a]++;
                }
                else
                {
                    this->pRepsOut[level][a][this->pRepsOut_sizes[level][a]] = r.id;
                    this->pRepsOut_sizes[level][a]++;
                }
            }
            else
            {
                if (a == b)
                {
                    this->pOrgsIn[level][a][this->pOrgsIn_sizes[level][a]] = Record(r.id, r.start, r.end);
                    this->pOrgsIn_sizes[level][a]++;
                }
                else
                {
                    this->pOrgsOut[level][a][this->pOrgsOut_sizes[level][a]] = RecordStart(r.id, r.start);
                    this->pOrgsOut_sizes[level][a]++;
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
//                this->pOrgsOut[level][prevb][this->pOrgsOut[level][prevb].numRecords] = RecordStart(r.id, r.start);
//                this->pOrgsOut[level][prevb].numRecords++;
                this->pOrgsIn[level][prevb][this->pOrgsIn_sizes[level][prevb]] = Record(r.id, r.start, r.end);
                this->pOrgsIn_sizes[level][prevb]++;
            }
            else
            {
//                if (a == prevb)
//                {
                this->pRepsIn[level][prevb][this->pRepsIn_sizes[level][prevb]] = RecordEnd(r.id, r.end);
                this->pRepsIn_sizes[level][prevb]++;
//                }
//                else
//                {
//                    this->pRepsOut[level][prevb][this->pRepsOut[level][prevb].numRecords] = r.id;
//                    this->pRepsOut[level][prevb].numRecords++;
//                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


//inline void HINT_M_SubsSopt::updatePartitionsUpdates(const Record &r)
//{
//    int level = 0;
//    Timestamp a = r.start >> (this->maxBits-this->numBits);
//    Timestamp b = r.end   >> (this->maxBits-this->numBits);
//    Timestamp prevb;
//    int firstfound = 0;
//
//
//    //    r.print('r');
//    while (level < this->height && a <= b)
//    {
//        if (a%2)
//        { //last bit of a is 1
//            if (firstfound)
//            {
//                if (a == b)
//                {
//                    this->pRepsIn[level][a].push_back(RecordEnd(r.id, r.end));
////                    this->pRepsIn_sizes[level][a]++;
//                }
//                else
//                {
//                    this->pRepsOut[level][a].push_back(r.id);
////                    this->pRepsOut_sizes[level][a]++;
//                }
//            }
//            else
//            {
//                if (a == b)
//                {
//                    this->pOrgsIn[level][a].push_back(Record(r.id, r.start, r.end));
////                    this->pOrgsIn_sizes[level][a]++;
//                }
//                else
//                {
//                    this->pOrgsOut[level][a].push_back(RecordStart(r.id, r.start));
////                    this->pOrgsOut_sizes[level][a]++;
//                }
//                firstfound = 1;
//            }
//            //a+=(int)(pow(2,level));
//            a++;
//        }
//        if (!(b%2))
//        { //last bit of b is 0
//            prevb = b;
//            //b-=(int)(pow(2,level));
//            b--;
//            if ((!firstfound) && b < a)
//            {
//                //                this->pOrgsOut[level][prevb][this->pOrgsOut[level][prevb].numRecords] = RecordStart(r.id, r.start);
//                //                this->pOrgsOut[level][prevb].numRecords++;
//                this->pOrgsIn[level][prevb].push_back(Record(r.id, r.start, r.end));
////                this->pOrgsIn_sizes[level][prevb]++;
//            }
//            else
//            {
//                //                if (a == prevb)
//                //                {
//                this->pRepsIn[level][prevb].push_back(RecordEnd(r.id, r.end));
////                this->pRepsIn_sizes[level][prevb]++;
//                //                }
//                //                else
//                //                {
//                //                    this->pRepsOut[level][prevb][this->pRepsOut[level][prevb].numRecords] = r.id;
//                //                    this->pRepsOut[level][prevb].numRecords++;
//                //                }
//            }
//        }
//        a >>= 1; // a = a div 2
//        b >>= 1; // b = b div 2
//        level++;
//    }
//}


HINT_M_SubsSopt::HINT_M_SubsSopt(const Relation &R, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
{
    // Initialize statistics
    this->numOriginalsIn = this->numOriginalsOut = this->numReplicasIn = this->numReplicasOut = 0;
    
    
    // Step 1: one pass to count the contents inside each partition.
    this->pOrgsIn_sizes  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsOut_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_sizes  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsOut_sizes = (size_t **)malloc(this->height*sizeof(size_t *));
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgsIn_sizes[l]  = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pOrgsOut_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pRepsIn_sizes[l]  = (size_t *)calloc(cnt, sizeof(size_t));
        this->pRepsOut_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);
    
    
    // Step 2: allocate necessary memory.
    this->pOrgsIn  = new Relation*[this->height];
    this->pOrgsOut = new RelationStart*[this->height];
    this->pRepsIn  = new RelationEnd*[this->height];
    this->pRepsOut = new RelationId*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsIn[l]  = new Relation[cnt];
        this->pOrgsOut[l] = new RelationStart[cnt];
        this->pRepsIn[l]  = new RelationEnd[cnt];
        this->pRepsOut[l] = new RelationId[cnt];
        
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsIn[l][j].resize(this->pOrgsIn_sizes[l][j]);
            this->pOrgsOut[l][j].resize(this->pOrgsOut_sizes[l][j]);
            this->pRepsIn[l][j].resize(this->pRepsIn_sizes[l][j]);
            this->pRepsOut[l][j].resize(this->pRepsOut_sizes[l][j]);
        }
    }
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        memset(this->pOrgsIn_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pOrgsOut_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pRepsIn_sizes[l], 0, cnt*sizeof(size_t));
        memset(this->pRepsOut_sizes[l], 0, cnt*sizeof(size_t));
    }
    
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    
    
    // Free auxiliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(pOrgsIn_sizes[l]);
        free(pOrgsOut_sizes[l]);
        free(pRepsIn_sizes[l]);
        free(pRepsOut_sizes[l]);
    }
    free(pOrgsIn_sizes);
    free(pOrgsOut_sizes);
    free(pRepsIn_sizes);
    free(pRepsOut_sizes);
}


// For updates only
HINT_M_SubsSopt::HINT_M_SubsSopt(const Relation &R, const Relation &M, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
{
    // Initialize statistics
    this->numOriginalsIn = this->numOriginalsOut = this->numReplicasIn = this->numReplicasOut = 0;
    
    
    // Step 1: one pass to count the contents inside each partition.
    this->pOrgsIn_sizes  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsOut_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_sizes  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsOut_sizes = (size_t **)malloc(this->height*sizeof(size_t *));
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgsIn_sizes[l]  = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pOrgsOut_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pRepsIn_sizes[l]  = (size_t *)calloc(cnt, sizeof(size_t));
        this->pRepsOut_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);
    for (const Record &r : M)   // To allocate necessary memory for the updates
        this->updateCounters(r);
    
    
    // Step 2: allocate necessary memory.
    this->pOrgsIn  = new Relation*[this->height];
    this->pOrgsOut = new RelationStart*[this->height];
    this->pRepsIn  = new RelationEnd*[this->height];
    this->pRepsOut = new RelationId*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsIn[l]  = new Relation[cnt];
        this->pOrgsOut[l] = new RelationStart[cnt];
        this->pRepsIn[l]  = new RelationEnd[cnt];
        this->pRepsOut[l] = new RelationId[cnt];
        
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsIn[l][j].reserve(this->pOrgsIn_sizes[l][j]);
            this->pOrgsOut[l][j].reserve(this->pOrgsOut_sizes[l][j]);
            this->pRepsIn[l][j].reserve(this->pRepsIn_sizes[l][j]);
            this->pRepsOut[l][j].reserve(this->pRepsOut_sizes[l][j]);
        }
    }
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        memset(this->pOrgsIn_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pOrgsOut_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pRepsIn_sizes[l], 0, cnt*sizeof(size_t));
        memset(this->pRepsOut_sizes[l], 0, cnt*sizeof(size_t));
    }
    
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitionsUpdates(r);
    

    // Free auxiliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgsIn_sizes[l]);
        free(this->pOrgsOut_sizes[l]);
        free(this->pRepsIn_sizes[l]);
        free(this->pRepsOut_sizes[l]);
    }
    free(this->pOrgsIn_sizes);
    free(this->pOrgsOut_sizes);
    free(this->pRepsIn_sizes);
    free(this->pRepsOut_sizes);
}


void HINT_M_SubsSopt::print(char c)
{
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        printf("Level %d: %d partitions\n", l, cnt);
        for (auto j = 0; j < cnt; j++)
        {
            printf("OrgsIn %d (%d): ", j, this->pOrgsIn[l][j].size());
//            for (auto k = 0; k < this->bucketcountersA[i][j]; k++)
//                printf("%d ", this->pOrgs[i][j][k].id);
            printf("\n");
            printf("OrgsOut %d (%d): ", j, this->pOrgsOut[l][j].size());
            printf("\n");
            printf("RepsIn %d (%d): ", j, this->pRepsIn[l][j].size());
//            for (auto k = 0; k < this->bucketcountersB[i][j]; k++)
//                printf("%d ", this->pReps[i][j][k].id);
            printf("\n");
            printf("RepsOut %d (%d): ", j, this->pRepsOut[l][j].size());
            printf("\n\n");
        }
    }
}


void HINT_M_SubsSopt::getStats()
{
    size_t sum = 0;
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        this->numPartitions += cnt;
        for (int j = 0; j < cnt; j++)
        {
            this->numOriginalsIn  += this->pOrgsIn[l][j].size();
            this->numOriginalsOut += this->pOrgsOut[l][j].size();
            this->numReplicasIn   += this->pRepsIn[l][j].size();
            this->numReplicasOut  += this->pRepsOut[l][j].size();
            if ((this->pOrgsIn[l][j].empty()) && (this->pOrgsOut[l][j].empty()) && (this->pRepsIn[l][j].empty()) && (this->pRepsOut[l][j].empty()))
                this->numEmptyPartitions++;
        }
    }
    
    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicasIn+this->numReplicasOut)/(this->numPartitions-numEmptyPartitions);
}


HINT_M_SubsSopt::~HINT_M_SubsSopt()
{
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgsIn[l];
        delete[] this->pOrgsOut[l];
        delete[] this->pRepsIn[l];
        delete[] this->pRepsOut[l];
    }
    
    delete[] this->pOrgsIn;
    delete[] this->pOrgsOut;
    delete[] this->pRepsIn;
    delete[] this->pRepsOut;
}


size_t HINT_M_SubsSopt::executeBottomUp(RangeQuery Q)
{
    size_t result = 0;
    Relation::iterator iterStart;
    RelationIterator iter, iterEnd;
    RelationStart::iterator iterSStart, iterSEnd;
    RelationEnd::iterator iterEStart, iterEEnd;
    RelationIdIterator iterIStart, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    Record qdummyE(0, Q.end+1, Q.end+1);
    RecordStart qdummySE(0, Q.end+1);
    RecordEnd qdummyS(0, Q.start);
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            iterEStart = this->pRepsIn[l][a].begin();
            iterEEnd = this->pRepsIn[l][a].end();
            for (RelationEndIterator iter = iterEStart; iter != iterEEnd; iter++)
                result ^= iter->id;
            iterIStart =this->pRepsOut[l][a].begin();
            iterIEnd = this->pRepsOut[l][a].end();
            for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
                result ^= (*iter);
            
            // Handle rest: consider only originals
            for (auto j = a; j <= b; j++)
            {
                iterStart = this->pOrgsIn[l][j].begin();
                iterEnd = this->pOrgsIn[l][j].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
                iterSStart = this->pOrgsOut[l][j].begin();
                iterSEnd = this->pOrgsOut[l][j].end();
                for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                    result ^= iter->id;
            }
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                if (!foundzero && !foundone)
                {
                    iterStart = this->pOrgsIn[l][a].begin();
                    iterEnd = this->pOrgsIn[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if ((iter->start <= Q.end) && (Q.start <= iter->end))
                            result ^= iter->id;
                    }
                    iterSStart = this->pOrgsOut[l][a].begin();
                    iterSEnd = this->pOrgsOut[l][a].end();
                    for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                    {
                        if (iter->start <= Q.end)
                            result ^= iter->id;
                    }
                }
                else if (foundzero)
                {
                    iterStart = this->pOrgsIn[l][a].begin();
                    iterEnd = this->pOrgsIn[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (iter->start <= Q.end)
                            result ^= iter->id;
                    }
                    iterSStart = this->pOrgsOut[l][a].begin();
                    iterSEnd = this->pOrgsOut[l][a].end();
                    for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                    {
                        if (iter->start <= Q.end)
                            result ^= iter->id;
                    }
                }
                else if (foundone)
                {
                    iterStart = this->pOrgsIn[l][a].begin();
                    iterEnd = this->pOrgsIn[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (Q.start <= iter->end)
                            result ^= iter->id;
                    }
                    iterSStart = this->pOrgsOut[l][a].begin();
                    iterSEnd = this->pOrgsOut[l][a].end();
                    for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                        result ^= iter->id;
                }
            }
            else
            {
                // Lemma 1
                if (!foundzero)
                {
                    iterStart = this->pOrgsIn[l][a].begin();
                    iterEnd = this->pOrgsIn[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (Q.start <= iter->end)
                            result ^= iter->id;
                    }
                }
                else
                {
                    iterStart = this->pOrgsIn[l][a].begin();
                    iterEnd = this->pOrgsIn[l][a].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= iter->id;
                }
                iterSStart = this->pOrgsOut[l][a].begin();
                iterSEnd = this->pOrgsOut[l][a].end();
                for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                    result ^= iter->id;
            }
            
            // Lemma 1, 3
            if (!foundzero)
            {
                iterEStart = this->pRepsIn[l][a].begin();
                iterEEnd = this->pRepsIn[l][a].end();
                for (RelationEndIterator iter = iterEStart; iter != iterEEnd; iter++)
                {
                    if (Q.start <= iter->end)
                        result ^= iter->id;
                }
            }
            else
            {
                iterEStart = this->pRepsIn[l][a].begin();
                iterEEnd = this->pRepsIn[l][a].end();
                for (RelationEndIterator iter = iterEStart; iter != iterEEnd; iter++)
                    result ^= iter->id;
            }
            iterIStart = this->pRepsOut[l][a].begin();
            iterIEnd = this->pRepsOut[l][a].end();
            for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
                result ^= (*iter);
            
            if (a < b)
            {
                if (!foundone)
                {
                    // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                    for (auto j = a+1; j < b; j++)
                    {
                        iterStart = this->pOrgsIn[l][j].begin();
                        iterEnd = this->pOrgsIn[l][j].end();
                        for (iter = iterStart; iter != iterEnd; iter++)
                            result ^= iter->id;
                        iterSStart = this->pOrgsOut[l][j].begin();
                        iterSEnd = this->pOrgsOut[l][j].end();
                        for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                            result ^= iter->id;
                    }
                    
                    // Handle the partition that contains b: consider only originals, comparisons needed
                    iterStart = this->pOrgsIn[l][b].begin();
                    iterEnd = this->pOrgsIn[l][b].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                    {
                        if (iter->start <= Q.end)
                            result ^= iter->id;
                    }
                    iterSStart = this->pOrgsOut[l][b].begin();
                    iterSEnd = this->pOrgsOut[l][b].end();
                    for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                    {
                        if (iter->start <= Q.end)
                            result ^= iter->id;
                    }
                }
                else
                {
                    for (auto j = a+1; j <= b; j++)
                    {
                        iterStart = this->pOrgsIn[l][j].begin();
                        iterEnd = this->pOrgsIn[l][j].end();
                        for (iter = iterStart; iter != iterEnd; iter++)
                            result ^= iter->id;
                        iterSStart = this->pOrgsOut[l][j].begin();
                        iterSEnd = this->pOrgsOut[l][j].end();
                        for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                            result ^= iter->id;
                    }
                }
            }
            
            if ((!foundone) && (b%2)) //last bit of b is 1
                foundone = 1;
            if ((!foundzero) && (!(a%2))) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterStart = this->pOrgsIn[this->numBits][0].begin();
        iterEnd = this->pOrgsIn[this->numBits][0].end();
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= iter->id;
    }
    else
    {
        // Comparisons needed
        iterStart = this->pOrgsIn[this->numBits][0].begin();
        iterEnd = this->pOrgsIn[this->numBits][0].end();
        for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
        {
            if ((iter->start <= Q.end) && (Q.start <= iter->end))
                result ^= iter->id;
        }
    }
    
    
    return result;
}



inline void HINT_M_SubsSortSopt::updateCounters(const Record &r)
{
    int level = 0;
    Timestamp a = r.start >> (this->maxBits-this->numBits);
    Timestamp b = r.end   >> (this->maxBits-this->numBits);
    Timestamp prevb;
    int firstfound = 0;
    auto count = 0;
    
    
    //    r.print('r');
    while (level < this->height && a <= b)
    {
        if (a%2)
        { //last bit of a is 1
            if (firstfound)
            {
//                printf("up: added to level %d, partition %d, class B, a = %d, b = %d\n", level, a, a, b);
                if (a == b)
                    this->pRepsIn_sizes[level][a]++;
                else
                    this->pRepsOut_sizes[level][a]++;
                count++;
            }
            else
            {
//                printf("up: added to level %d, partition %d, class A, a = %d, b = %d\n", level, a, a, b);
                if (a == b)
                    this->pOrgsIn_sizes[level][a]++;
                else
                    this->pOrgsOut_sizes[level][a]++;
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
//                printf("down: added to level %d, partition %d, class A, a = %d, b = %d\n", level, prevb, a, prevb);
//                this->pOrgsOut_sizes[level][prevb]++;
                this->pOrgsIn_sizes[level][prevb]++;
            }
            else
            {
//                printf("down: added to level %d, partition %d, class B, a = %d, b = %d\n", level, prevb, a, prevb);
//                if (a == prevb)
                this->pRepsIn_sizes[level][prevb]++;
//                else
//                    this->pRepsOut_sizes[level][prevb]++;
                count++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
    
    //    cout << r.id << "\t" << count << endl;
}


inline void HINT_M_SubsSortSopt::updatePartitions(const Record &r)
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
                    this->pRepsIn[level][a][this->pRepsIn_sizes[level][a]] = RecordEnd(r.id, r.end);
                    this->pRepsIn_sizes[level][a]++;
                }
                else
                {
                    this->pRepsOut[level][a][this->pRepsOut_sizes[level][a]] = r.id;
                    this->pRepsOut_sizes[level][a]++;
                }
            }
            else
            {
                if (a == b)
                {
                    this->pOrgsIn[level][a][this->pOrgsIn_sizes[level][a]] = Record(r.id, r.start, r.end);
                    this->pOrgsIn_sizes[level][a]++;
                }
                else
                {
                    this->pOrgsOut[level][a][this->pOrgsOut_sizes[level][a]] = RecordStart(r.id, r.start);
                    this->pOrgsOut_sizes[level][a]++;
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
//                this->pOrgsOut[level][prevb][this->pOrgsOut[level][prevb].numRecords] = RecordStart(r.id, r.start);
//                this->pOrgsOut[level][prevb].numRecords++;
                this->pOrgsIn[level][prevb][this->pOrgsIn_sizes[level][prevb]] = Record(r.id, r.start, r.end);
                this->pOrgsIn_sizes[level][prevb]++;
            }
            else
            {
//                if (a == prevb)
//                {
                this->pRepsIn[level][prevb][this->pRepsIn_sizes[level][prevb]] = RecordEnd(r.id, r.end);
                this->pRepsIn_sizes[level][prevb]++;
//                }
//                else
//                {
//                    this->pRepsOut[level][prevb][this->pRepsOut[level][prevb].numRecords] = r.id;
//                    this->pRepsOut[level][prevb].numRecords++;
//                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M_SubsSortSopt::HINT_M_SubsSortSopt(const Relation &R, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
{
    // Initialize statistics
    this->numOriginalsIn = this->numOriginalsOut = this->numReplicasIn = this->numReplicasOut = 0;
    
    
    // Step 1: one pass to count the contents inside each partition.
    this->pOrgsIn_sizes  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsOut_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_sizes  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsOut_sizes = (size_t **)malloc(this->height*sizeof(size_t *));
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgsIn_sizes[l]  = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pOrgsOut_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pRepsIn_sizes[l]  = (size_t *)calloc(cnt, sizeof(size_t));
        this->pRepsOut_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);
    
    
    // Step 2: allocate necessary memory.
    this->pOrgsIn  = new Relation*[this->height];
    this->pOrgsOut = new RelationStart*[this->height];
    this->pRepsIn  = new RelationEnd*[this->height];
    this->pRepsOut = new RelationId*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsIn[l]  = new Relation[cnt];
        this->pOrgsOut[l] = new RelationStart[cnt];
        this->pRepsIn[l]  = new RelationEnd[cnt];
        this->pRepsOut[l] = new RelationId[cnt];
        
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsIn[l][j].resize(this->pOrgsIn_sizes[l][j]);
            this->pOrgsOut[l][j].resize(this->pOrgsOut_sizes[l][j]);
            this->pRepsIn[l][j].resize(this->pRepsIn_sizes[l][j]);
            this->pRepsOut[l][j].resize(this->pRepsOut_sizes[l][j]);
        }
    }
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        memset(this->pOrgsIn_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pOrgsOut_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pRepsIn_sizes[l], 0, cnt*sizeof(size_t));
        memset(this->pRepsOut_sizes[l], 0, cnt*sizeof(size_t));
    }
    
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    
    
    // Step 4: sort partition contents.
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsIn[l][j].sortByStart();
            this->pOrgsOut[l][j].sortByStart();
            this->pRepsIn[l][j].sortByEnd();
        }
    }

    
    // Free auxiliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(pOrgsIn_sizes[l]);
        free(pOrgsOut_sizes[l]);
        free(pRepsIn_sizes[l]);
        free(pRepsOut_sizes[l]);
    }
    free(pOrgsIn_sizes);
    free(pOrgsOut_sizes);
    free(pRepsIn_sizes);
    free(pRepsOut_sizes);
}


void HINT_M_SubsSortSopt::print(char c)
{
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        printf("Level %d: %d partitions\n", l, cnt);
        for (auto j = 0; j < cnt; j++)
        {
            printf("OrgsIn %d (%d): ", j, this->pOrgsIn[l][j].size());
            //            for (auto k = 0; k < this->bucketcountersA[i][j]; k++)
            //                printf("%d ", this->pOrgs[i][j][k].id);
            printf("\n");
            printf("OrgsOut %d (%d): ", j, this->pOrgsOut[l][j].size());
            printf("\n");
            printf("RepsIn %d (%d): ", j, this->pRepsIn[l][j].size());
            //            for (auto k = 0; k < this->bucketcountersB[i][j]; k++)
            //                printf("%d ", this->pReps[i][j][k].id);
            printf("\n");
            printf("RepsOut %d (%d): ", j, this->pRepsOut[l][j].size());
            printf("\n\n");
        }
    }
}


void HINT_M_SubsSortSopt::getStats()
{
    size_t sum = 0;
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        
        this->numPartitions += cnt;
        for (int j = 0; j < cnt; j++)
        {
            this->numOriginalsIn  += this->pOrgsIn[l][j].size();
            this->numOriginalsOut += this->pOrgsOut[l][j].size();
            this->numReplicasIn   += this->pRepsIn[l][j].size();
            this->numReplicasOut  += this->pRepsOut[l][j].size();
            if ((this->pOrgsIn[l][j].empty()) && (this->pOrgsOut[l][j].empty()) && (this->pRepsIn[l][j].empty()) && (this->pRepsOut[l][j].empty()))
                this->numEmptyPartitions++;
        }
    }
    
    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicasIn+this->numReplicasOut)/(this->numPartitions-numEmptyPartitions);
}


HINT_M_SubsSortSopt::~HINT_M_SubsSortSopt()
{
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgsIn[l];
        delete[] this->pOrgsOut[l];
        delete[] this->pRepsIn[l];
        delete[] this->pRepsOut[l];
    }
    
    delete[] this->pOrgsIn;
    delete[] this->pOrgsOut;
    delete[] this->pRepsIn;
    delete[] this->pRepsOut;
}


size_t HINT_M_SubsSortSopt::executeBottomUp(RangeQuery Q)
{
    size_t result = 0;
    Relation::iterator iterStart;
    RelationIterator iter, iterEnd;
    RelationStart::iterator iterSStart, iterSEnd;
    RelationEnd::iterator iterEStart, iterEEnd;
    RelationIdIterator iterIStart, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    Record qdummyE(0, Q.end+1, Q.end+1);
    RecordStart qdummySE(0, Q.end+1);
    RecordEnd qdummyS(0, Q.start);
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            iterEStart = this->pRepsIn[l][a].begin();
            iterEEnd = this->pRepsIn[l][a].end();
            for (RelationEndIterator iter = iterEStart; iter != iterEEnd; iter++)
                result ^= iter->id;
            iterIStart =this->pRepsOut[l][a].begin();
            iterIEnd = this->pRepsOut[l][a].end();
            for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
                result ^= (*iter);
            
            // Handle rest: consider only originals
            for (auto j = a; j <= b; j++)
            {
                iterStart = this->pOrgsIn[l][j].begin();
                iterEnd = this->pOrgsIn[l][j].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
                iterSStart = this->pOrgsOut[l][j].begin();
                iterSEnd = this->pOrgsOut[l][j].end();
                for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                    result ^= iter->id;
            }
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                iterStart = this->pOrgsIn[l][a].begin();
                iterEnd = lower_bound(iterStart, this->pOrgsIn[l][a].end(), qdummyE);
                for (iter = iterStart; iter != iterEnd; iter++)
                {
                    if (Q.start <= iter->end)
                        result ^= iter->id;
                }
                iterSStart = this->pOrgsOut[l][a].begin();
                iterSEnd = lower_bound(iterSStart, this->pOrgsOut[l][a].end(), qdummySE);
                for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                    result ^= iter->id;
            }
            else
            {
                // Lemma 1
                iterStart = this->pOrgsIn[l][a].begin();
                iterEnd = this->pOrgsIn[l][a].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                {
                    if (Q.start <= iter->end)
                        result ^= iter->id;
                }
                iterSStart = this->pOrgsOut[l][a].begin();
                iterSEnd = this->pOrgsOut[l][a].end();
                for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                    result ^= iter->id;
            }
            
            // Lemma 1, 3
            iterEEnd = this->pRepsIn[l][a].end();
            iterEStart = lower_bound(this->pRepsIn[l][a].begin(), iterEEnd, qdummyS);
            for (RelationEndIterator iter = iterEStart; iter != iterEEnd; iter++)
                result ^= iter->id;
            iterIStart = this->pRepsOut[l][a].begin();
            iterIEnd = this->pRepsOut[l][a].end();
            for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
                result ^= (*iter);
            
            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                for (auto j = a+1; j < b; j++)
                {
                    iterStart = this->pOrgsIn[l][j].begin();
                    iterEnd = this->pOrgsIn[l][j].end();
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= iter->id;
                    iterSStart = this->pOrgsOut[l][j].begin();
                    iterSEnd = this->pOrgsOut[l][j].end();
                    for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                        result ^= iter->id;
                }
                
                // Handle the partition that contains b: consider only originals, comparisons needed
                iterStart = this->pOrgsIn[l][b].begin();
                iterEnd = lower_bound(iterStart, this->pOrgsIn[l][b].end(), qdummyE);
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
                iterSStart = this->pOrgsOut[l][b].begin();
                iterSEnd = lower_bound(iterSStart, this->pOrgsOut[l][b].end(), qdummySE);
                for (RelationStartIterator iter = iterSStart; iter != iterSEnd; iter++)
                    result ^= iter->id;
            }
            
            if (b%2) //last bit of b is 1
                foundone = 1;
            if (!(a%2)) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterStart = this->pOrgsIn[this->numBits][0].begin();
        iterEnd = this->pOrgsIn[this->numBits][0].end();
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= iter->id;
    }
    else
    {
        // Comparisons needed
        iterStart = this->pOrgsIn[this->numBits][0].begin();
        iterEnd = lower_bound(iterStart, this->pOrgsIn[this->numBits][0].end(), qdummyE);
        for (RelationIterator iter = iterStart; iter != iterEnd; iter++)
        {
            if (Q.start <= iter->end)
                result ^= iter->id;
        }
    }
    
    
    return result;
}



inline void HINT_M_SubsSortSopt_SS::updateCounters(const Record &r)
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
                if (a == b)
                    this->pRepsIn_sizes[level][a]++;
                else
                    this->pRepsOut_sizes[level][a]++;
            }
            else
            {
                if (a == b)
                    this->pOrgsIn_sizes[level][a]++;
                else
                    this->pOrgsOut_sizes[level][a]++;
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
                this->pOrgsIn_sizes[level][prevb]++;
            }
            else
            {
                this->pRepsIn_sizes[level][prevb]++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


inline void HINT_M_SubsSortSopt_SS::updatePartitions(const Record &r)
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
                    this->pRepsIn[level][this->pRepsIn_offsets[level][a]++] = RecordEnd(r.id, r.end);
                }
                else
                {
                    this->pRepsOut[level][this->pRepsOut_offsets[level][a]++] = r.id;
                }
            }
            else
            {
                if (a == b)
                {
                    this->pOrgsIn[level][this->pOrgsIn_offsets[level][a]++] = Record(r.id, r.start, r.end);
                }
                else
                {
                    this->pOrgsOut[level][this->pOrgsOut_offsets[level][a]++] = RecordStart(r.id, r.start);
                }
                firstfound = 1;
            }
            a++;
        }
        if (!(b%2))
        { //last bit of b is 0
            prevb = b;
            b--;
            if ((!firstfound) && b < a)
            {
                this->pOrgsIn[level][this->pOrgsIn_offsets[level][prevb]++] = Record(r.id, r.start, r.end);
            }
            else
            {
//                if (a == prevb)
//                {
                this->pRepsIn[level][this->pRepsIn_offsets[level][prevb]++] = RecordEnd(r.id, r.end);
//                }
//                else
//                {
//                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M_SubsSortSopt_SS::HINT_M_SubsSortSopt_SS(const Relation &R, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
{
    tuple<Timestamp, RelationIterator, PartitionId> dummySE;
    tuple<Timestamp, RelationStartIterator, PartitionId> dummyS;
    tuple<Timestamp, RelationEndIterator, PartitionId> dummyE;
    tuple<Timestamp, RelationIdIterator, PartitionId> dummyI;
    vector<tuple<Timestamp, RelationIterator, PartitionId> >::iterator iterSEO, iterSEOStart, iterSEOEnd;
    vector<tuple<Timestamp, RelationStartIterator, PartitionId> >::iterator iterSO, iterSOStart, iterSOEnd;
    vector<tuple<Timestamp, RelationEndIterator, PartitionId> >::iterator iterEO, iterEOStart, iterEOEnd;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    PartitionId tmp = -1;
    
    
    // Initialize statistics
    this->numOriginalsIn = this->numOriginalsOut = this->numReplicasIn = this->numReplicasOut = 0;
    
    
    // Step 1: one pass to count the contents inside each partition.
    this->pOrgsIn_sizes  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsOut_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_sizes  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsOut_sizes = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pOrgsIn_offsets  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsOut_offsets = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_offsets  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsOut_offsets = (size_t **)malloc(this->height*sizeof(size_t *));
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgsIn_sizes[l]  = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pOrgsOut_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pRepsIn_sizes[l]  = (size_t *)calloc(cnt, sizeof(size_t));
        this->pRepsOut_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
        this->pOrgsIn_offsets[l]  = (RecordId *)calloc(cnt+1, sizeof(RecordId));
        this->pOrgsOut_offsets[l] = (RecordId *)calloc(cnt+1, sizeof(RecordId));
        this->pRepsIn_offsets[l]  = (size_t *)calloc(cnt+1, sizeof(size_t));
        this->pRepsOut_offsets[l] = (size_t *)calloc(cnt+1, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);
    
    
    // Step 2: allocate necessary memory.
    this->pOrgsIn  = new Relation[this->height];
    this->pOrgsOut = new RelationStart[this->height];
    this->pRepsIn  = new RelationEnd[this->height];
    this->pRepsOut = new RelationId[this->height];
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumOin = 0, sumOout = 0, sumRin = 0, sumRout = 0;
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgsIn_offsets[l][pId]  = sumOin;
            this->pOrgsOut_offsets[l][pId] = sumOout;
            this->pRepsIn_offsets[l][pId]  = sumRin;
            this->pRepsOut_offsets[l][pId] = sumRout;
            sumOin  += this->pOrgsIn_sizes[l][pId];
            sumOout += this->pOrgsOut_sizes[l][pId];
            sumRin  += this->pRepsIn_sizes[l][pId];
            sumRout += this->pRepsOut_sizes[l][pId];
        }
        this->pOrgsIn_offsets[l][cnt]  = sumOin;
        this->pOrgsOut_offsets[l][cnt] = sumOout;
        this->pRepsIn_offsets[l][cnt]  = sumRin;
        this->pRepsOut_offsets[l][cnt] = sumRout;
        
        this->pOrgsIn[l].resize(sumOin);
        this->pOrgsOut[l].resize(sumOout);
        this->pRepsIn[l].resize(sumRin);
        this->pRepsOut[l].resize(sumRout);
    }
    
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    
    
    // Step 4: create offset pointers
    this->pOrgsIn_ioffsets  = new vector<tuple<Timestamp, RelationIterator, PartitionId> >[this->height];
    this->pOrgsOut_ioffsets = new vector<tuple<Timestamp, RelationStartIterator, PartitionId> >[this->height];
    this->pRepsIn_ioffsets  = new vector<tuple<Timestamp, RelationEndIterator, PartitionId> >[this->height];
    this->pRepsOut_ioffsets = new vector<tuple<Timestamp, RelationIdIterator, PartitionId> >[this->height];
    for (int l = this->height-1; l > -1; l--)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumOin = 0, sumOout = 0, sumRin = 0, sumRout = 0;
        
        //        cout << "this->numBits-l = " << this->numBits-l << endl;
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            get<0>(dummySE) = pId >> 1;
            //            get<0>(dummySE) = ((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pOrgsIn_sizes[l][pId] > 0)
            {
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSEOStart = this->pOrgsIn_ioffsets[l+1].begin();
                    iterSEOEnd = this->pOrgsIn_ioffsets[l+1].end();
                    iterSEO = lower_bound(iterSEOStart, iterSEOEnd, dummySE, CompareStartEnd);
                    tmp = (iterSEO != iterSEOEnd)? (iterSEO-iterSEOStart): -1;
                }
                this->pOrgsIn_ioffsets[l].push_back(tuple<Timestamp, RelationIterator, PartitionId>(pId, this->pOrgsIn[l].begin()+sumOin, tmp));
            }
            
            get<0>(dummyS) = pId >> 1;
            //            get<0>(dummySE) = ((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pOrgsOut_sizes[l][pId] > 0)
            {
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSOStart = this->pOrgsOut_ioffsets[l+1].begin();
                    iterSOEnd = this->pOrgsOut_ioffsets[l+1].end();
                    iterSO = lower_bound(iterSOStart, iterSOEnd, dummyS, CompareStart);
                    tmp = (iterSO != iterSOEnd)? (iterSO-iterSOStart): -1;
                }
                this->pOrgsOut_ioffsets[l].push_back(tuple<Timestamp, RelationStartIterator, PartitionId>(pId, this->pOrgsOut[l].begin()+sumOout, tmp));
            }
            
            get<0>(dummyE) = pId >> 1;
            //            get<0>(dummySE) = ((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pRepsIn_sizes[l][pId] > 0)
            {
                tmp = -1;
                if (l < this->height-1)
                {
                    iterEOStart = this->pRepsIn_ioffsets[l+1].begin();
                    iterEOEnd = this->pRepsIn_ioffsets[l+1].end();
                    iterEO = lower_bound(iterEOStart, iterEOEnd, dummyE, CompareEnd);
                    tmp = (iterEO != iterEOEnd)? (iterEO-iterEOStart): -1;
                }
                this->pRepsIn_ioffsets[l].push_back(tuple<Timestamp, RelationEndIterator, PartitionId>(pId, this->pRepsIn[l].begin()+sumRin, tmp));
            }
            
            get<0>(dummyI) = pId >> 1;
            //            get<0>(dummySE) = ((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pRepsOut_sizes[l][pId] > 0)
            {
                tmp = -1;
                if (l < this->height-1)
                {
                    iterIOStart = this->pRepsOut_ioffsets[l+1].begin();
                    iterIOEnd = this->pRepsOut_ioffsets[l+1].end();
                    iterIO = lower_bound(iterIOStart, iterIOEnd, dummyI, CompareId);
                    tmp = (iterIO != iterIOEnd)? (iterIO-iterIOStart): -1;
                }
                this->pRepsOut_ioffsets[l].push_back(tuple<Timestamp, RelationIdIterator, PartitionId>(pId, this->pRepsOut[l].begin()+sumRout, tmp));
            }
            
            this->pOrgsIn_offsets[l][pId]  = sumOin;
            this->pOrgsOut_offsets[l][pId] = sumOout;
            this->pRepsIn_offsets[l][pId]  = sumRin;
            this->pRepsOut_offsets[l][pId] = sumRout;
            
            sumOin += this->pOrgsIn_sizes[l][pId];
            sumOout += this->pOrgsOut_sizes[l][pId];
            sumRin += this->pRepsIn_sizes[l][pId];
            sumRout += this->pRepsOut_sizes[l][pId];
        }
        this->pOrgsIn_offsets[l][cnt]  = sumOin;
        this->pOrgsOut_offsets[l][cnt] = sumOout;
        this->pRepsIn_offsets[l][cnt]  = sumRin;
        this->pRepsOut_offsets[l][cnt] = sumRout;
    }
    
    
    // Free auxliary memory
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgsIn_sizes[l]);
        free(this->pOrgsOut_sizes[l]);
        free(this->pRepsIn_sizes[l]);
        free(this->pRepsOut_sizes[l]);
    }
    free(this->pOrgsIn_sizes);
    free(this->pOrgsOut_sizes);
    free(this->pRepsIn_sizes);
    free(this->pRepsOut_sizes);
    
    
    // Step 4: sort partition contents.
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        for (auto pId = 0; pId < cnt; pId++)
        {
            sort(this->pOrgsIn[l].begin()+this->pOrgsIn_offsets[l][pId], this->pOrgsIn[l].begin()+this->pOrgsIn_offsets[l][pId+1]);
            sort(this->pOrgsOut[l].begin()+this->pOrgsOut_offsets[l][pId], this->pOrgsOut[l].begin()+this->pOrgsOut_offsets[l][pId+1]);
            sort(this->pRepsIn[l].begin()+this->pRepsIn_offsets[l][pId], this->pRepsIn[l].begin()+this->pRepsIn_offsets[l][pId+1]);
        }
    }
    
    
    // Free auxliary memory
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgsIn_offsets[l]);
        free(this->pOrgsOut_offsets[l]);
        free(this->pRepsIn_offsets[l]);
        free(this->pRepsOut_offsets[l]);
    }
    free(this->pOrgsIn_offsets);
    free(this->pOrgsOut_offsets);
    free(this->pRepsIn_offsets);
    free(this->pRepsOut_offsets);
}


HINT_M_SubsSortSopt_SS::~HINT_M_SubsSortSopt_SS()
{
    delete[] this->pOrgsIn_ioffsets;
    delete[] this->pOrgsOut_ioffsets;
    delete[] this->pRepsIn_ioffsets;
    delete[] this->pRepsOut_ioffsets;
    
    delete[] this->pOrgsIn;
    delete[] this->pOrgsOut;
    delete[] this->pRepsIn;
    delete[] this->pRepsOut;
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_OrgsIn(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareStartEnd);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pOrgsIn[level].end());
                
                next_from = get<2>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsIn_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsIn_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pOrgsIn_ioffsets[level][from]) == a))
            {
                iterStart = get<1>(this->pOrgsIn_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pOrgsIn_ioffsets[level][from+1]) : this->pOrgsIn[level].end());
                
                next_from = get<2>(this->pOrgsIn_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= iter->id;
    }
    else
        next_from = -1;
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_OrgsIn(unsigned int level, Timestamp a, Timestamp qstart, Record qdummyE, PartitionId &next_from, size_t &result)
{
    RelationIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareStartEnd);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pOrgsIn[level].end());
                
                next_from = get<2>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsIn_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsIn_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pOrgsIn_ioffsets[level][from]) == a))
            {
                iterStart = get<1>(this->pOrgsIn_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pOrgsIn_ioffsets[level][from+1]) : this->pOrgsIn[level].end());
                
                next_from = get<2>(this->pOrgsIn_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        RelationIterator pivot = lower_bound(iterStart, iterEnd, qdummyE);
        for (iter = iterStart; iter != pivot; iter++)
        {
            if (qstart <= iter->end)
                result ^= iter->id;
        }
    }
    else
        next_from = -1;
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_OrgsIn(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result)
{
    RelationIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareStartEnd);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pOrgsIn[level].end());
                
                next_from = get<2>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsIn_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsIn_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pOrgsIn_ioffsets[level][from]) == a))
            {
                iterStart = get<1>(this->pOrgsIn_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pOrgsIn_ioffsets[level][from+1]) : this->pOrgsIn[level].end());
                
                next_from = get<2>(this->pOrgsIn_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        for (iter = iterStart; iter != iterEnd; iter++)
        {
            if (qstart <= iter->end)
                result ^= iter->id;
        }
    }
    else
        next_from = -1;
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_OrgsOut(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationStartIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationStartIterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationStartIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsOut_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pOrgsOut_ioffsets[level].begin();
            iterIOEnd = this->pOrgsOut_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareStart);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pOrgsOut[level].end());
                
                next_from = get<2>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsOut_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsOut_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pOrgsOut_ioffsets[level][from]) == a))
            {
                iterStart = get<1>(this->pOrgsOut_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pOrgsOut_ioffsets[level][from+1]) : this->pOrgsOut[level].end());
                
                next_from = get<2>(this->pOrgsOut_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= iter->id;
    }
    else
        next_from = -1;
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_RepsIn(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationEndIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationEndIterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationEndIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pRepsIn_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pRepsIn_ioffsets[level].begin();
            iterIOEnd = this->pRepsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareEnd);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pRepsIn[level].end());
                
                next_from = get<2>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pRepsIn_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pRepsIn_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pRepsIn_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pRepsIn_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pRepsIn_ioffsets[level][from]) == a))
            {
                iterStart = get<1>(this->pRepsIn_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pRepsIn_ioffsets[level][from+1]) : this->pRepsIn[level].end());
                
                next_from = get<2>(this->pRepsIn_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= iter->id;
    }
    else
        next_from = -1;
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_RepsIn(unsigned int level, Timestamp a, RecordEnd qdummyS, PartitionId &next_from, size_t &result)
{
    RelationEndIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationEndIterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationEndIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pRepsIn_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pRepsIn_ioffsets[level].begin();
            iterIOEnd = this->pRepsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareEnd);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pRepsIn[level].end());
                
                next_from = get<2>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pRepsIn_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pRepsIn_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pRepsIn_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pRepsIn_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pRepsIn_ioffsets[level][from]) == a))
            {
                iterStart = get<1>(this->pRepsIn_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pRepsIn_ioffsets[level][from+1]) : this->pRepsIn[level].end());
                
                next_from = get<2>(this->pRepsIn_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        RelationEndIterator pivot = lower_bound(iterStart, iterEnd, qdummyS);
        for (iter = pivot; iter != iterEnd; iter++)
            result ^= iter->id;
    }
    else
        next_from = -1;
}


inline void HINT_M_SubsSortSopt_SS::scanFirstPartition_RepsOut(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIdIterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pRepsOut_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pRepsOut_ioffsets[level].begin();
            iterIOEnd = this->pRepsOut_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareId);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pRepsOut[level].end());
                
                next_from = get<2>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pRepsOut_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pRepsOut_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pRepsOut_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pRepsOut_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pRepsOut_ioffsets[level][from]) == a))
            {
                iterStart = get<1>(this->pRepsOut_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pRepsOut_ioffsets[level][from+1]) : this->pRepsOut[level].end());
                
                next_from = get<2>(this->pRepsOut_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= (*iter);
    }
    else
        next_from = -1;
}


inline void HINT_M_SubsSortSopt_SS::scanLastPartition_OrgsIn(unsigned int level, Timestamp b, Record qdummyE, PartitionId &next_from, size_t &result)
{
    RelationIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = b;
            iterIOStart = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareStartEnd);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == b))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pOrgsIn[level].end());
                
                next_from = get<2>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsIn_ioffsets[level][from]);
            if (tmp < b)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) < b) && (from < cnt))
                    from++;
            }
            else if (tmp > b)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) > b) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsIn_ioffsets[level][from]) != b) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pOrgsIn_ioffsets[level][from]) == b))
            {
                iterStart = get<1>(this->pOrgsIn_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pOrgsIn_ioffsets[level][from+1]) : this->pOrgsIn[level].end());
                
                next_from = get<2>(this->pOrgsIn_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        RelationIterator pivot = lower_bound(iterStart, iterEnd, qdummyE);
        for (iter = iterStart; iter != pivot; iter++)
            result ^= iter->id;
    }
    else
        next_from = -1;
}


inline void HINT_M_SubsSortSopt_SS::scanLastPartition_OrgsOut(unsigned int level, Timestamp b, RecordStart qdummySE, PartitionId &next_from, size_t &result)
{
    RelationStartIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationStartIterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationStartIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsOut_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = b;
            iterIOStart = this->pOrgsOut_ioffsets[level].begin();
            iterIOEnd = this->pOrgsOut_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareStart);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == b))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pOrgsOut[level].end());
                
                next_from = get<2>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsOut_ioffsets[level][from]);
            if (tmp < b)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][from]) < b) && (from < cnt))
                    from++;
            }
            else if (tmp > b)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][from]) > b) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsOut_ioffsets[level][from]) != b) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pOrgsOut_ioffsets[level][from]) == b))
            {
                iterStart = get<1>(this->pOrgsOut_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pOrgsOut_ioffsets[level][from+1]) : this->pOrgsOut[level].end());
                
                next_from = get<2>(this->pOrgsOut_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        RelationStartIterator pivot = lower_bound(iterStart, iterEnd, qdummySE);
        for (iter = iterStart; iter != pivot; iter++)
            result ^= iter->id;
    }
    else
        next_from = -1;
}


inline void HINT_M_SubsSortSopt_SS::scanPartitions_OrgsIn(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIterator, PartitionId> qdummyA, qdummyB;
    vector<tuple<Timestamp, RelationIterator, PartitionId> >::iterator iterIO, iterIO2, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from, to = next_to;
    
    
    if (cnt > 0)
    {
        from = next_from;
        to = next_to;
        
        // Adjusting pointers.
        if ((from == -1) || (to == -1))
        {
            get<0>(qdummyA) = a;
            iterIOStart = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummyA, CompareStartEnd);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) <= b))
            {
                next_from = get<2>(*iterIO);
                
                get<0>(qdummyB) = b;
                iterStart = get<1>(*iterIO);
                
                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyB, CompareStartEnd);
                //                iterIO2 = iterIO;
                //                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= b))
                //                    iterIO2++;
                
                iterEnd = ((iterIO2 != iterIOEnd) ? iterEnd = get<1>(*iterIO2): this->pOrgsIn[level].end());
                for (iter = iterStart; iter != iterEnd; iter++)
                    //                    cout << "\tr" << iter->id << endl;
                    result ^= iter->id;
                
                if (iterIO2 != iterIOEnd)
                    next_to = get<2>(*iterIO2);
                else
                    next_to = -1;
            }
            else
                next_from = -1;
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsIn_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsIn_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            tmp = get<0>(this->pOrgsIn_ioffsets[level][to]);
            if (tmp > b)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][to]) > b) && (to > -1))
                    to--;
                to++;
            }
            //                else if (tmp <= b)
            else if (tmp == b)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][to]) <= b) && (to < cnt))
                    to++;
            }
            
            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterStart = get<1>(this->pOrgsIn_ioffsets[level][from]);
                iterEnd   = (to != cnt)? get<1>(this->pOrgsIn_ioffsets[level][to]): this->pOrgsIn[level].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
                
                next_from = get<2>(this->pOrgsIn_ioffsets[level][from]);
                next_to   = (to != cnt) ? get<2>(this->pOrgsIn_ioffsets[level][to])  : -1;
            }
            else
                next_from = next_to = -1;
        }
    }
    else
    {
        next_from = -1;
        next_to = -1;
    }
}


inline void HINT_M_SubsSortSopt_SS::scanPartitions_OrgsOut(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationStartIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationStartIterator, PartitionId> qdummyA, qdummyB;
    vector<tuple<Timestamp, RelationStartIterator, PartitionId> >::iterator iterIO, iterIO2, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsOut_ioffsets[level].size();
    PartitionId from = next_from, to = next_to;
    
    
    if (cnt > 0)
    {
        // Adjusting pointers.
        if ((from == -1) || (to == -1))
        {
            get<0>(qdummyA) = a;
            iterIOStart = this->pOrgsOut_ioffsets[level].begin();
            iterIOEnd = this->pOrgsOut_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummyA, CompareStart);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) <= b))
            {
                next_from = get<2>(*iterIO);
                
                get<0>(qdummyB) = b;
                iterStart = get<1>(*iterIO);
                
                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyB, CompareStart);
                //                iterIO2 = iterIO;
                //                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= b))
                //                    iterIO2++;
                
                iterEnd = ((iterIO2 != iterIOEnd) ? iterEnd = get<1>(*iterIO2): this->pOrgsOut[level].end());
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
                
                if (iterIO2 != iterIOEnd)
                    next_to = get<2>(*iterIO2);
                else
                    next_to = -1;
            }
            else
                next_from = -1;
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsOut_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsOut_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            tmp = get<0>(this->pOrgsOut_ioffsets[level][to]);
            if (tmp > b)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][to]) > b) && (to > -1))
                    to--;
                to++;
            }
            //                else if (tmp <= b)
            else if (tmp == b)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][to]) <= b) && (to < cnt))
                    to++;
            }
            
            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterStart = get<1>(this->pOrgsOut_ioffsets[level][from]);
                iterEnd   = (to != cnt)? get<1>(this->pOrgsOut_ioffsets[level][to]): this->pOrgsOut[level].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= iter->id;
                
                next_from = get<2>(this->pOrgsOut_ioffsets[level][from]);
                next_to   = (to != cnt) ? get<2>(this->pOrgsOut_ioffsets[level][to])  : -1;
            }
            else
                next_from = next_to = -1;
        }
    }
    else
    {
        next_from = -1;
        next_to = -1;
    }
}


size_t HINT_M_SubsSortSopt_SS::executeBottomUp(RangeQuery Q)
{
    size_t result = 0;
    Relation::iterator iterStart;
    RelationIterator iter, iterEnd;
    RelationStart::iterator iterSStart, iterSEnd;
    RelationEnd::iterator iterEStart, iterEEnd;
    RelationIdIterator iterIStart, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    Record qdummyE(0, Q.end+1, Q.end+1);
    RecordStart qdummySE(0, Q.end+1);
    RecordEnd qdummyS(0, Q.start);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOoutA = -1, next_fromRinA = -1, next_fromRoutA = -1, next_fromOinB = -1, next_fromOoutB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOoutAB = -1, next_toOoutAB = -1;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            this->scanFirstPartition_RepsIn(l, a, next_fromRinA, result);
            this->scanFirstPartition_RepsOut(l, a, next_fromRoutA, result);
            
            this->scanPartitions_OrgsIn(l, a, b, next_fromOinAB, next_toOinAB, result);
            this->scanPartitions_OrgsOut(l, a, b, next_fromOoutAB, next_toOoutAB, result);
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                this->scanFirstPartition_OrgsIn(l, a, Q.start, qdummyE, next_fromOinA, result);
                this->scanLastPartition_OrgsOut(l, a, qdummySE, next_fromOoutA, result);
            }
            else
            {
                // Lemma 1
                this->scanFirstPartition_OrgsIn(l, a, Q.start, next_fromOinA, result);
                this->scanFirstPartition_OrgsOut(l, a, next_fromOoutA, result);
            }
            
            // Lemma 1, 3
            this->scanFirstPartition_RepsIn(l, a, qdummyS, next_fromRinA, result);
            this->scanFirstPartition_RepsOut(l, a, next_fromRoutA, result);
            
            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                this->scanPartitions_OrgsIn(l, a+1, b-1, next_fromOinAB, next_toOinAB, result);
                this->scanPartitions_OrgsOut(l, a+1, b-1, next_fromOoutAB, next_toOoutAB, result);
                
                // Handle the partition that contains b: consider only originals, comparisons needed
                this->scanLastPartition_OrgsIn(l, b, qdummyE, next_fromOinB, result);
                this->scanLastPartition_OrgsOut(l, b, qdummySE, next_fromOoutB, result);
            }
            
            if (b%2) //last bit of b is 1
                foundone = 1;
            if (!(a%2)) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterStart = this->pOrgsIn[this->numBits].begin();
        iterEnd = this->pOrgsIn[this->numBits].end();
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= iter->id;
    }
    else
    {
        // Comparisons needed
        iterStart = this->pOrgsIn[this->numBits].begin();
        iterEnd = lower_bound(iterStart, this->pOrgsIn[this->numBits].end(), qdummyE);
        for (iter = iterStart; iter != iterEnd; iter++)
        {
            if (Q.start <= iter->end)
                result ^= iter->id;
        }
    }
    
    
    return result;
}



// Opt
/*size_t HINT_M_SubsSortSopt_SS::executeBottomUp(RangeQuery Q)
{
    size_t result = 0;
    Relation::iterator iterStart;
    RelationIterator iter, iterEnd;
    RelationStart::iterator iterSStart, iterSEnd;
    RelationEnd::iterator iterEStart, iterEEnd;
    RelationIdIterator iterIStart, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    Record qdummyE(0, Q.end+1, Q.end+1);
    RecordStart qdummySE(0, Q.end+1);
    RecordEnd qdummyS(0, Q.start);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOoutA = -1, next_fromRinA = -1, next_fromRoutA = -1, next_fromOinB = -1, next_fromOoutB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOoutAB = -1, next_toOoutAB = -1;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            this->scanFirstPartition_RepsIn(l, a, next_fromRinA, result);
            this->scanFirstPartition_RepsOut(l, a, next_fromRoutA, result);
            
            this->scanPartitions_OrgsIn(l, a, b, next_fromOinAB, next_toOinAB, result);
            this->scanPartitions_OrgsOut(l, a, b, next_fromOoutAB, next_toOoutAB, result);
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                if (!foundzero && !foundone) // compFirst && compLast
                {
                    this->scanFirstPartition_OrgsIn(l, a, Q.start, qdummyE, next_fromOinA, result); // Q.start, Q.end
                    this->scanLastPartition_OrgsOut(l, a, qdummySE, next_fromOoutA, result); // Q.end
                }
                else if (foundzero) // compLast
                {
                    this->scanLastPartition_OrgsIn(l, b, qdummyE, next_fromOinA, result);    // Q.end
                    this->scanLastPartition_OrgsOut(l, b, qdummySE, next_fromOoutA, result); // Q.end
                }
                else if (foundone) // compFirst
                {
                    this->scanFirstPartition_OrgsIn(l, a, Q.start, next_fromOinA, result); // Q.start
                    this->scanFirstPartition_OrgsOut(l, a, next_fromOoutA, result);
                }
            }
            else
            {
                // Lemma 1
                if (!foundzero) // CompFirst
                {
                    this->scanFirstPartition_OrgsIn(l, a, Q.start, next_fromOinA, result);   // Q.start
                }
                else
                {
                    this->scanFirstPartition_OrgsIn(l, a, next_fromOinA, result);
                }
                this->scanFirstPartition_OrgsOut(l, a, next_fromOoutA, result);
            }
            
            // Lemma 1, 3
            if (!foundzero) // compFirst
            {
                this->scanFirstPartition_RepsIn(l, a, qdummyS, next_fromRinA, result); // Q.start
            }
            else
            {
                this->scanFirstPartition_RepsIn(l, a, next_fromRinA, result);
            }
            this->scanFirstPartition_RepsOut(l, a, next_fromRoutA, result);
            
            if (a < b)
            {
                if (!foundone) // CompLast
                {
                    // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                    this->scanPartitions_OrgsIn(l, a+1, b-1, next_fromOinAB, next_toOinAB, result);
                    this->scanPartitions_OrgsOut(l, a+1, b-1, next_fromOoutAB, next_toOoutAB, result);
                    
                    // Handle the partition that contains b: consider only originals, comparisons needed
                    this->scanLastPartition_OrgsIn(l, b, qdummyE, next_fromOinB, result);    // Q.end
                    this->scanLastPartition_OrgsOut(l, b, qdummySE, next_fromOoutB, result);    // Q.end
                }
                else
                {
                    this->scanPartitions_OrgsIn(l, a+1, b, next_fromOinAB, next_toOinAB, result);
                    this->scanPartitions_OrgsOut(l, a+1, b, next_fromOoutAB, next_toOoutAB, result);
                }
            }
            
            if ((!foundone) && (b%2)) //last bit of b is 1
                foundone = 1;
            if ((!foundzero) && (!(a%2))) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterStart = this->pOrgsIn[this->numBits].begin();
        iterEnd = this->pOrgsIn[this->numBits].end();
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= iter->id;
    }
    else
    {
        // Comparisons needed
        iterStart = this->pOrgsIn[this->numBits].begin();
        iterEnd = lower_bound(iterStart, this->pOrgsIn[this->numBits].end(), qdummyE);
        for (iter = iterStart; iter != iterEnd; iter++)
        {
            if (Q.start <= iter->end)
                result ^= iter->id;
        }
    }
    
    
    return result;
}*/



inline void HINT_M_SubsSortSopt_CM::updateCounters(const Record &r)
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
                //                printf("up: added to level %d, partition %d, class B, a = %d, b = %d\n", level, a, a, b);
                if (a == b)
                    this->pRepsIn_sizes[level][a]++;
                else
                    this->pRepsOut_sizes[level][a]++;
            }
            else
            {
                //                printf("up: added to level %d, partition %d, class A, a = %d, b = %d\n", level, a, a, b);
                if (a == b)
                    this->pOrgsIn_sizes[level][a]++;
                else
                    this->pOrgsOut_sizes[level][a]++;
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
                //                printf("down: added to level %d, partition %d, class A, a = %d, b = %d\n", level, prevb, a, prevb);
                //                this->pOrgsOut_sizes[level][prevb]++;
                this->pOrgsIn_sizes[level][prevb]++;
            }
            else
            {
                //                printf("down: added to level %d, partition %d, class B, a = %d, b = %d\n", level, prevb, a, prevb);
                //                if (a == prevb)
                this->pRepsIn_sizes[level][prevb]++;
                //                else
                //                    this->pRepsOut_sizes[level][prevb]++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


inline void HINT_M_SubsSortSopt_CM::updatePartitions(const Record &r)
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
                    this->pRepsInTmp[level][a][this->pRepsIn_sizes[level][a]] = RecordEnd(r.id, r.end);
                    this->pRepsIn_sizes[level][a]++;
                }
                else
                {
                    this->pRepsOut[level][a][this->pRepsOut_sizes[level][a]] = r.id;
                    this->pRepsOut_sizes[level][a]++;
                }
            }
            else
            {
                if (a == b)
                {
                    this->pOrgsInTmp[level][a][this->pOrgsIn_sizes[level][a]] = Record(r.id, r.start, r.end);
                    this->pOrgsIn_sizes[level][a]++;
                }
                else
                {
                    this->pOrgsOutTmp[level][a][this->pOrgsOut_sizes[level][a]] = RecordStart(r.id, r.start);
                    this->pOrgsOut_sizes[level][a]++;
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
                //                this->pOrgsOut[level][prevb][this->pOrgsOut[level][prevb].numRecords] = RecordStart(r.id, r.start);
                //                this->pOrgsOut[level][prevb].numRecords++;
                this->pOrgsInTmp[level][prevb][this->pOrgsIn_sizes[level][prevb]] = Record(r.id, r.start, r.end);
                this->pOrgsIn_sizes[level][prevb]++;
            }
            else
            {
                //                if (a == prevb)
                //                {
                this->pRepsInTmp[level][prevb][this->pRepsIn_sizes[level][prevb]] = RecordEnd(r.id, r.end);
                this->pRepsIn_sizes[level][prevb]++;
                //                }
                //                else
                //                {
                //                    this->pRepsOut[level][prevb][this->pRepsOut[level][prevb].numRecords] = r.id;
                //                    this->pRepsOut[level][prevb].numRecords++;
                //                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M_SubsSortSopt_CM::HINT_M_SubsSortSopt_CM(const Relation &R, const unsigned int numBits, const unsigned int maxBits) : HierarchicalIndex(R, numBits, maxBits)
{
    // Step 1: one pass to count the contents inside each partition.
    this->pOrgsIn_sizes  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsOut_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_sizes  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsOut_sizes = (size_t **)malloc(this->height*sizeof(size_t *));
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgsIn_sizes[l]  = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pOrgsOut_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pRepsIn_sizes[l]  = (size_t *)calloc(cnt, sizeof(size_t));
        this->pRepsOut_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);
    
    // Step 2: allocate necessary memory.
    this->pOrgsInTmp  = new Relation*[this->height];
    this->pOrgsOutTmp = new RelationStart*[this->height];
    this->pRepsInTmp  = new RelationEnd*[this->height];
    this->pRepsOut = new RelationId*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsInTmp[l]  = new Relation[cnt];
        this->pOrgsOutTmp[l] = new RelationStart[cnt];
        this->pRepsInTmp[l]  = new RelationEnd[cnt];
        this->pRepsOut[l] = new RelationId[cnt];
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgsInTmp[l][pId].resize(this->pOrgsIn_sizes[l][pId]);
            this->pOrgsOutTmp[l][pId].resize(this->pOrgsOut_sizes[l][pId]);
            this->pRepsInTmp[l][pId].resize(this->pRepsIn_sizes[l][pId]);
            this->pRepsOut[l][pId].resize(this->pRepsOut_sizes[l][pId]);
        }
    }
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        memset(this->pOrgsIn_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pOrgsOut_sizes[l], 0, cnt*sizeof(RecordId));
        memset(this->pRepsIn_sizes[l], 0, cnt*sizeof(size_t));
        memset(this->pRepsOut_sizes[l], 0, cnt*sizeof(size_t));
    }
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    
    // Step 4: sort partition contents.
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgsInTmp[l][pId].sortByStart();
            this->pOrgsOutTmp[l][pId].sortByStart();
            this->pRepsInTmp[l][pId].sortByEnd();
        }
    }
    
    // Free auxiliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgsIn_sizes[l]);
        free(this->pOrgsOut_sizes[l]);
        free(this->pRepsIn_sizes[l]);
        free(this->pRepsOut_sizes[l]);
    }
    free(this->pOrgsIn_sizes);
    free(this->pOrgsOut_sizes);
    free(this->pRepsIn_sizes);
    free(this->pRepsOut_sizes);
    
    // Copy and free auxiliary memory.
    this->pOrgsInIds  = new RelationId*[this->height];
    this->pOrgsOutIds = new RelationId*[this->height];
    this->pRepsInIds  = new RelationId*[this->height];
    this->pOrgsInTimestamps = new vector<pair<Timestamp, Timestamp> >*[this->height];
    this->pOrgsOutTimestamp = new vector<Timestamp>*[this->height];
    this->pRepsInTimestamp  = new vector<Timestamp>*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgsInIds[l]  = new RelationId[cnt];
        this->pOrgsOutIds[l] = new RelationId[cnt];
        this->pRepsInIds[l]  = new RelationId[cnt];
        this->pOrgsInTimestamps[l] = new vector<pair<Timestamp, Timestamp> >[cnt];
        this->pOrgsOutTimestamp[l] = new vector<Timestamp>[cnt];
        this->pRepsInTimestamp[l]  = new vector<Timestamp>[cnt];
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            auto cnt = this->pOrgsInTmp[l][pId].size();
            this->pOrgsInIds[l][pId].resize(cnt);
            this->pOrgsInTimestamps[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pOrgsInIds[l][pId][j] = this->pOrgsInTmp[l][pId][j].id;
                this->pOrgsInTimestamps[l][pId][j].first = this->pOrgsInTmp[l][pId][j].start;
                this->pOrgsInTimestamps[l][pId][j].second = this->pOrgsInTmp[l][pId][j].end;
            }
            
            cnt = this->pOrgsOutTmp[l][pId].size();
            this->pOrgsOutIds[l][pId].resize(cnt);
            this->pOrgsOutTimestamp[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pOrgsOutIds[l][pId][j] = this->pOrgsOutTmp[l][pId][j].id;
                this->pOrgsOutTimestamp[l][pId][j] = this->pOrgsOutTmp[l][pId][j].start;
            }
            
            cnt = this->pRepsInTmp[l][pId].size();
            this->pRepsInIds[l][pId].resize(cnt);
            this->pRepsInTimestamp[l][pId].resize(cnt);
            for (auto j = 0; j < cnt; j++)
            {
                this->pRepsInIds[l][pId][j] = this->pRepsInTmp[l][pId][j].id;
                this->pRepsInTimestamp[l][pId][j] = this->pRepsInTmp[l][pId][j].end;
            }
        }
        
        delete[] this->pOrgsInTmp[l];
        delete[] this->pOrgsOutTmp[l];
        delete[] this->pRepsInTmp[l];
    }
    delete[] this->pOrgsInTmp;
    delete[] this->pOrgsOutTmp;
    delete[] this->pRepsInTmp;
}


HINT_M_SubsSortSopt_CM::~HINT_M_SubsSortSopt_CM()
{
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgsInIds[l];
        delete[] this->pOrgsInTimestamps[l];
        delete[] this->pOrgsOutIds[l];
        delete[] this->pOrgsOutTimestamp[l];
        delete[] this->pRepsInIds[l];
        delete[] this->pRepsInTimestamp[l];
        delete[] this->pRepsOut[l];
    }
    
    delete[] this->pOrgsInIds;
    delete[] this->pOrgsInTimestamps;
    delete[] this->pOrgsOutIds;
    delete[] this->pOrgsOutTimestamp;
    delete[] this->pRepsInIds;
    delete[] this->pRepsInTimestamp;
    delete[] this->pRepsOut;
}



inline bool CompareTimestampsStart(const pair<Timestamp, Timestamp> &lhs, const pair<Timestamp, Timestamp> &rhs)
{
    return (lhs.first < rhs.first);
}


size_t HINT_M_SubsSortSopt_CM::executeBottomUp(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterStart, iterEnd;
    vector<Timestamp>::iterator iterSStart, iterSEnd;
    vector<Timestamp>::iterator iterEStart, iterEEnd;
    RelationIdIterator iterIStart, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    pair<Timestamp, Timestamp> qdummyE(Q.end+1, Q.end+1);
    Timestamp qdummySE = Q.end+1;
    Timestamp qdummyS = Q.start;
    bool foundzero = false;
    bool foundone = false;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            iterIStart = this->pRepsInIds[l][a].begin();
            iterIEnd = this->pRepsInIds[l][a].end();
            for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
                result ^= (*iter);
            iterIStart =this->pRepsOut[l][a].begin();
            iterIEnd = this->pRepsOut[l][a].end();
            for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
                result ^= (*iter);
            
            // Handle rest: consider only originals
            for (auto j = a; j <= b; j++)
            {
                iterIStart = this->pOrgsInIds[l][j].begin();
                iterIEnd = this->pOrgsInIds[l][j].end();
                for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
                    result ^= (*iter);
                iterIStart = this->pOrgsOutIds[l][j].begin();
                iterIEnd = this->pOrgsOutIds[l][j].end();
                for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
                    result ^= (*iter);
            }
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                iterStart = this->pOrgsInTimestamps[l][a].begin();
                iterEnd = lower_bound(iterStart, this->pOrgsInTimestamps[l][a].end(), qdummyE, CompareTimestampsStart);
                for (iter = iterStart; iter != iterEnd; iter++)
                {
                    if (Q.start <= iter->second)
                        result ^= this->pOrgsInIds[l][a][iter-iterStart];
                }
                iterSStart = this->pOrgsOutTimestamp[l][a].begin();
                iterSEnd = lower_bound(iterSStart, this->pOrgsOutTimestamp[l][a].end(), qdummySE);
                for (vector<Timestamp>::iterator iter = iterSStart; iter != iterSEnd; iter++)
                    result ^= this->pOrgsOutIds[l][a][iter-iterSStart];
            }
            else
            {
                // Lemma 1
                iterStart = this->pOrgsInTimestamps[l][a].begin();
                iterEnd = this->pOrgsInTimestamps[l][a].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                {
                    if (Q.start <= iter->second)
                        result ^= this->pOrgsInIds[l][a][iter-iterStart];
                }
                iterSStart = this->pOrgsOutTimestamp[l][a].begin();
                iterSEnd = this->pOrgsOutTimestamp[l][a].end();
                for (vector<Timestamp>::iterator iter = iterSStart; iter != iterSEnd; iter++)
                    result ^= this->pOrgsOutIds[l][a][iter-iterSStart];
            }
            
            // Lemma 1, 3
            iterEStart = this->pRepsInTimestamp[l][a].begin();
            iterEEnd = this->pRepsInTimestamp[l][a].end();
            vector<Timestamp>::iterator pivot = lower_bound(iterEStart, iterEEnd, qdummyS);
            for (vector<Timestamp>::iterator iter = pivot; iter != iterEEnd; iter++)
                result ^= this->pRepsInIds[l][a][iter-iterEStart];
            iterIStart = this->pRepsOut[l][a].begin();
            iterIEnd = this->pRepsOut[l][a].end();
            for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
                result ^= (*iter);
            
            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                for (auto j = a+1; j < b; j++)
                {
                    iterIStart = this->pOrgsInIds[l][j].begin();
                    iterIEnd = this->pOrgsInIds[l][j].end();
                    for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
                        result ^= (*iter);
                    iterIStart = this->pOrgsOutIds[l][j].begin();
                    iterIEnd = this->pOrgsOutIds[l][j].end();
                    for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
                        result ^= (*iter);
                }
                
                // Handle the partition that contains b: consider only originals, comparisons needed
                iterStart = this->pOrgsInTimestamps[l][b].begin();
                iterEnd = lower_bound(iterStart, this->pOrgsInTimestamps[l][b].end(), qdummyE, CompareTimestampsStart);
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= this->pOrgsInIds[l][b][iter-iterStart];
                iterSStart = this->pOrgsOutTimestamp[l][b].begin();
                iterSEnd = lower_bound(iterSStart, this->pOrgsOutTimestamp[l][b].end(), qdummySE);
                for (vector<Timestamp>::iterator iter = iterSStart; iter != iterSEnd; iter++)
                    result ^= this->pOrgsOutIds[l][b][iter-iterSStart];
            }
            
            if (b%2) //last bit of b is 1
                foundone = 1;
            if (!(a%2)) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterIStart = this->pOrgsInIds[this->numBits][0].begin();
        iterIEnd = this->pOrgsInIds[this->numBits][0].end();
        for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
            result ^= (*iter);
    }
    else
    {
        // Comparisons needed
        iterStart = this->pOrgsInTimestamps[this->numBits][0].begin();
        iterEnd = lower_bound(iterStart, this->pOrgsInTimestamps[this->numBits][0].end(), qdummyE, CompareTimestampsStart);
        for (iter = iterStart; iter != iterEnd; iter++)
        {
            if (Q.start <= iter->second)
                result ^= this->pOrgsInIds[this->numBits][0][iter-iterStart];
        }
    }
    
    
    return result;
}



inline void HINT_M_ALL::updateCounters(const Record &r)
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
                if (a == b)
                    this->pRepsIn_sizes[level][a]++;
                else
                    this->pRepsOut_sizes[level][a]++;
            }
            else
            {
                if (a == b)
                    this->pOrgsIn_sizes[level][a]++;
                else
                    this->pOrgsOut_sizes[level][a]++;
                firstfound = 1;
            }
            a++;
        }
        if (!(b%2))
        { //last bit of b is 0
            prevb = b;
            b--;
            if ((!firstfound) && b < a)
            {
                //                this->pOrgsOut_sizes[level][prevb]++;
                this->pOrgsIn_sizes[level][prevb]++;
            }
            else
            {
                //                if (a == prevb)
                this->pRepsIn_sizes[level][prevb]++;
                //                else
                //                    this->pRepsOut_sizes[level][prevb]++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


inline void HINT_M_ALL::updatePartitions(const Record &r)
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
                    this->pRepsInTmp[level][this->pRepsIn_offsets[level][a]++] = RecordEnd(r.id, r.end);
                }
                else
                {
                    this->pRepsOut[level][this->pRepsOut_offsets[level][a]++] = r.id;
                }
            }
            else
            {
                if (a == b)
                {
                    this->pOrgsInTmp[level][this->pOrgsIn_offsets[level][a]++] = Record(r.id, r.start, r.end);
                }
                else
                {
                    this->pOrgsOutTmp[level][this->pOrgsOut_offsets[level][a]++] = RecordStart(r.id, r.start);
                }
                firstfound = 1;
            }
            a++;
        }
        if (!(b%2))
        { //last bit of b is 0
            prevb = b;
            b--;
            if ((!firstfound) && b < a)
            {
                this->pOrgsInTmp[level][this->pOrgsIn_offsets[level][prevb]++] = Record(r.id, r.start, r.end);
            }
            else
            {
                //                if (a == prevb)
                //                {
                this->pRepsInTmp[level][this->pRepsIn_offsets[level][prevb]++] = RecordEnd(r.id, r.end);
                //                }
                //                else
                //                {
                //                }
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_M_ALL::HINT_M_ALL(const Relation &R, const unsigned int numBits, const unsigned int maxBits)  : HierarchicalIndex(R, numBits, maxBits)
{
    tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> dummySE;
    tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> dummyS;
    tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> dummyE;
    tuple<Timestamp, RelationIdIterator, PartitionId> dummyI;
    vector<tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> >::iterator iterSEO, iterSEOStart, iterSEOEnd;
    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> >::iterator iterSO, iterSOStart, iterSOEnd;
    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> >::iterator iterEO, iterEOStart, iterEOEnd;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    PartitionId tmp = -1;
    
    
    // Step 1: one pass to count the contents inside each partition.
    this->pOrgsIn_sizes  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsOut_sizes = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_sizes  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsOut_sizes = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pOrgsIn_offsets  = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pOrgsOut_offsets = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pRepsIn_offsets  = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pRepsOut_offsets = (size_t **)malloc(this->height*sizeof(size_t *));
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgsIn_sizes[l]  = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pOrgsOut_sizes[l] = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pRepsIn_sizes[l]  = (size_t *)calloc(cnt, sizeof(size_t));
        this->pRepsOut_sizes[l] = (size_t *)calloc(cnt, sizeof(size_t));
        this->pOrgsIn_offsets[l]  = (RecordId *)calloc(cnt+1, sizeof(RecordId));
        this->pOrgsOut_offsets[l] = (RecordId *)calloc(cnt+1, sizeof(RecordId));
        this->pRepsIn_offsets[l]  = (size_t *)calloc(cnt+1, sizeof(size_t));
        this->pRepsOut_offsets[l] = (size_t *)calloc(cnt+1, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);
    
    
    // Step 2: allocate necessary memory.
    this->pOrgsInTmp  = new Relation[this->height];
    this->pOrgsOutTmp = new RelationStart[this->height];
    this->pRepsInTmp  = new RelationEnd[this->height];
    this->pRepsOut    = new RelationId[this->height];
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumOin = 0, sumOout = 0, sumRin = 0, sumRout = 0;
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgsIn_offsets[l][pId]  = sumOin;
            this->pOrgsOut_offsets[l][pId] = sumOout;
            this->pRepsIn_offsets[l][pId]  = sumRin;
            this->pRepsOut_offsets[l][pId] = sumRout;
            sumOin  += this->pOrgsIn_sizes[l][pId];
            sumOout += this->pOrgsOut_sizes[l][pId];
            sumRin  += this->pRepsIn_sizes[l][pId];
            sumRout += this->pRepsOut_sizes[l][pId];
        }
        this->pOrgsIn_offsets[l][cnt]  = sumOin;
        this->pOrgsOut_offsets[l][cnt] = sumOout;
        this->pRepsIn_offsets[l][cnt]  = sumRin;
        this->pRepsOut_offsets[l][cnt] = sumRout;
        
        this->pOrgsInTmp[l].resize(sumOin);
        this->pOrgsOutTmp[l].resize(sumOout);
        this->pRepsInTmp[l].resize(sumRin);
        this->pRepsOut[l].resize(sumRout);
    }
    
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    
    
    // Step 4: sort partition contents; first need to reset the offsets
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumOin = 0, sumOout = 0, sumRin = 0, sumRout = 0;
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgsIn_offsets[l][pId]  = sumOin;
            this->pOrgsOut_offsets[l][pId] = sumOout;
            this->pRepsIn_offsets[l][pId]  = sumRin;
            this->pRepsOut_offsets[l][pId] = sumRout;
            sumOin  += this->pOrgsIn_sizes[l][pId];
            sumOout += this->pOrgsOut_sizes[l][pId];
            sumRin  += this->pRepsIn_sizes[l][pId];
            sumRout += this->pRepsOut_sizes[l][pId];
        }
        this->pOrgsIn_offsets[l][cnt]  = sumOin;
        this->pOrgsOut_offsets[l][cnt] = sumOout;
        this->pRepsIn_offsets[l][cnt]  = sumRin;
        this->pRepsOut_offsets[l][cnt] = sumRout;
    }
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        for (auto pId = 0; pId < cnt; pId++)
        {
            sort(this->pOrgsInTmp[l].begin()+this->pOrgsIn_offsets[l][pId], this->pOrgsInTmp[l].begin()+this->pOrgsIn_offsets[l][pId+1]);
            sort(this->pOrgsOutTmp[l].begin()+this->pOrgsOut_offsets[l][pId], this->pOrgsOutTmp[l].begin()+this->pOrgsOut_offsets[l][pId+1]);
            sort(this->pRepsInTmp[l].begin()+this->pRepsIn_offsets[l][pId], this->pRepsInTmp[l].begin()+this->pRepsIn_offsets[l][pId+1]);
        }
    }
    
    
    // Step 5: break-down data to create id- and timestamp-dedicated arrays; free auxiliary memory.
    this->pOrgsInIds  = new RelationId[this->height];
    this->pOrgsOutIds = new RelationId[this->height];
    this->pRepsInIds  = new RelationId[this->height];
    this->pOrgsInTimestamps = new vector<pair<Timestamp, Timestamp> >[this->height];
    this->pOrgsOutTimestamp = new vector<Timestamp>[this->height];
    this->pRepsInTimestamp  = new vector<Timestamp>[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pOrgsInTmp[l].size();
        
        this->pOrgsInIds[l].resize(cnt);
        this->pOrgsInTimestamps[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsInIds[l][j] = this->pOrgsInTmp[l][j].id;
            this->pOrgsInTimestamps[l][j].first = this->pOrgsInTmp[l][j].start;
            this->pOrgsInTimestamps[l][j].second = this->pOrgsInTmp[l][j].end;
        }
        
        cnt = pOrgsOutTmp[l].size();
        this->pOrgsOutIds[l].resize(cnt);
        this->pOrgsOutTimestamp[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgsOutIds[l][j] = this->pOrgsOutTmp[l][j].id;
            this->pOrgsOutTimestamp[l][j] = this->pOrgsOutTmp[l][j].start;
        }
        
        
        cnt = pRepsInTmp[l].size();
        this->pRepsInIds[l].resize(cnt);
        this->pRepsInTimestamp[l].resize(cnt);
        for (auto j = 0; j < cnt; j++)
        {
            this->pRepsInIds[l][j] = this->pRepsInTmp[l][j].id;
            this->pRepsInTimestamp[l][j] = this->pRepsInTmp[l][j].end;
        }
    }
    
    
    // Free auxliary memory
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgsIn_offsets[l]);
        free(this->pOrgsOut_offsets[l]);
        free(this->pRepsIn_offsets[l]);
        free(this->pRepsOut_offsets[l]);
    }
    free(this->pOrgsIn_offsets);
    free(this->pOrgsOut_offsets);
    free(this->pRepsIn_offsets);
    free(this->pRepsOut_offsets);
    
    delete[] this->pOrgsInTmp;
    delete[] this->pOrgsOutTmp;
    delete[] this->pRepsInTmp;
    
    
    //    vector<tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> >   *pOrgsIn_ioffsets;
    //    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> > *pOrgsOut_ioffsets;
    //    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> > *pRepsIn_ioffsets;
    //    vector<tuple<Timestamp, RelationIdIterator, PartitionId> > *pRepsOut_ioffsets;
    
    // Step 4: create offset pointers
    this->pOrgsIn_ioffsets  = new vector<tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> >[this->height];
    this->pOrgsOut_ioffsets = new vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> >[this->height];
    this->pRepsIn_ioffsets  = new vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> >[this->height];
    this->pRepsOut_ioffsets = new vector<tuple<Timestamp, RelationIdIterator, PartitionId> >[this->height];
    for (int l = this->height-1; l > -1; l--)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumOin = 0, sumOout = 0, sumRin = 0, sumRout = 0;
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            get<0>(dummySE) = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pOrgsIn_sizes[l][pId] > 0)
            {
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSEOStart = this->pOrgsIn_ioffsets[l+1].begin();
                    iterSEOEnd = this->pOrgsIn_ioffsets[l+1].end();
                    iterSEO = lower_bound(iterSEOStart, iterSEOEnd, dummySE, CompareStartEnd2);
                    tmp = (iterSEO != iterSEOEnd)? (iterSEO-iterSEOStart): -1;
                }
                this->pOrgsIn_ioffsets[l].push_back(tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId>(pId, this->pOrgsInIds[l].begin()+sumOin, this->pOrgsInTimestamps[l].begin()+sumOin, tmp));
            }
            
            get<0>(dummyS) = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pOrgsOut_sizes[l][pId] > 0)
            {
                tmp = -1;
                if (l < this->height-1)
                {
                    iterSOStart = this->pOrgsOut_ioffsets[l+1].begin();
                    iterSOEnd = this->pOrgsOut_ioffsets[l+1].end();
                    iterSO = lower_bound(iterSOStart, iterSOEnd, dummyS, CompareStart2);
                    tmp = (iterSO != iterSOEnd)? (iterSO-iterSOStart): -1;
                }
                this->pOrgsOut_ioffsets[l].push_back(tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId>(pId, this->pOrgsOutIds[l].begin()+sumOout, this->pOrgsOutTimestamp[l].begin()+sumOout, tmp));
            }
            
            get<0>(dummyE) = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pRepsIn_sizes[l][pId] > 0)
            {
                tmp = -1;
                if (l < this->height-1)
                {
                    iterEOStart = this->pRepsIn_ioffsets[l+1].begin();
                    iterEOEnd = this->pRepsIn_ioffsets[l+1].end();
                    iterEO = lower_bound(iterEOStart, iterEOEnd, dummyE, CompareEnd2);
                    tmp = (iterEO != iterEOEnd)? (iterEO-iterEOStart): -1;
                }
                this->pRepsIn_ioffsets[l].push_back(tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId>(pId, this->pRepsInIds[l].begin()+sumRin, this->pRepsInTimestamp[l].begin()+sumRin, tmp));
            }
            
            get<0>(dummyI) = pId >> 1;//((pId >> (this->maxBits-this->numBits)) >> 1);
            if (this->pRepsOut_sizes[l][pId] > 0)
            {
                tmp = -1;
                if (l < this->height-1)
                {
                    iterIOStart = this->pRepsOut_ioffsets[l+1].begin();
                    iterIOEnd = this->pRepsOut_ioffsets[l+1].end();
                    iterIO = lower_bound(iterIOStart, iterIOEnd, dummyI, CompareId);
                    tmp = (iterIO != iterIOEnd)? (iterIO-iterIOStart): -1;
                }
                this->pRepsOut_ioffsets[l].push_back(tuple<Timestamp, RelationIdIterator, PartitionId>(pId, this->pRepsOut[l].begin()+sumRout, tmp));
            }
            
            sumOin += this->pOrgsIn_sizes[l][pId];
            sumOout += this->pOrgsOut_sizes[l][pId];
            sumRin += this->pRepsIn_sizes[l][pId];
            sumRout += this->pRepsOut_sizes[l][pId];
        }
    }
    
    
    // Free auxliary memory
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgsIn_sizes[l]);
        free(this->pOrgsOut_sizes[l]);
        free(this->pRepsIn_sizes[l]);
        free(this->pRepsOut_sizes[l]);
    }
    free(this->pOrgsIn_sizes);
    free(this->pOrgsOut_sizes);
    free(this->pRepsIn_sizes);
    free(this->pRepsOut_sizes);
}


HINT_M_ALL::~HINT_M_ALL()
{
    delete[] this->pOrgsIn_ioffsets;
    delete[] this->pOrgsOut_ioffsets;
    delete[] this->pRepsIn_ioffsets;
    delete[] this->pRepsOut_ioffsets;
    
    delete[] this->pOrgsInIds;
    delete[] this->pOrgsInTimestamps;
    delete[] this->pOrgsOutIds;
    delete[] this->pOrgsOutTimestamp;
    delete[] this->pRepsInIds;
    delete[] this->pRepsInTimestamp;
    delete[] this->pRepsOut;
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareStartEnd2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pOrgsInIds[level].end());
                
                next_from = get<3>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsIn_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsIn_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pOrgsIn_ioffsets[level][from]) == a))
            {
                iterStart = get<1>(this->pOrgsIn_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pOrgsIn_ioffsets[level][from+1]) : this->pOrgsInIds[level].end());
                
                next_from = get<3>(this->pOrgsIn_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= (*iter);
    }
    else
        next_from = -1;
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn(unsigned int level, Timestamp a, Timestamp qstart, pair<Timestamp, Timestamp> qdummyE, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterStart, iterEnd;
    RelationIdIterator iterI;
    tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareStartEnd2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterI = get<1>(*iterIO);
                iterStart = get<2>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<2>(*(iterIO+1)) : this->pOrgsInTimestamps[level].end());
                
                next_from = get<3>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsIn_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsIn_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pOrgsIn_ioffsets[level][from]) == a))
            {
                iterI = get<1>(this->pOrgsIn_ioffsets[level][from]);
                iterStart = get<2>(this->pOrgsIn_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<2>(this->pOrgsIn_ioffsets[level][from+1]) : this->pOrgsInTimestamps[level].end());
                
                next_from = get<3>(this->pOrgsIn_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterStart, iterEnd, qdummyE);
        for (iter = iterStart; iter != pivot; iter++)
        {
            if (qstart <= iter->second)
                result ^= (*iterI);
            iterI++;
        }
    }
    else
        next_from = -1;
}


inline void HINT_M_ALL::scanFirstPartition_OrgsIn(unsigned int level, Timestamp a, Timestamp qstart, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterStart, iterEnd;
    RelationIdIterator iterI;
    tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareStartEnd2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterI = get<1>(*iterIO);
                iterStart = get<2>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<2>(*(iterIO+1)) : this->pOrgsInTimestamps[level].end());
                
                next_from = get<3>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsIn_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsIn_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pOrgsIn_ioffsets[level][from]) == a))
            {
                iterI = get<1>(this->pOrgsIn_ioffsets[level][from]);
                iterStart = get<2>(this->pOrgsIn_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<2>(this->pOrgsIn_ioffsets[level][from+1]) : this->pOrgsInTimestamps[level].end());
                
                next_from = get<3>(this->pOrgsIn_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        for (iter = iterStart; iter != iterEnd; iter++)
        {
            if (qstart <= iter->second)
                result ^= (*iterI);
            iterI++;
        }
    }
    else
        next_from = -1;
}


inline void HINT_M_ALL::scanFirstPartition_OrgsOut(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsOut_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pOrgsOut_ioffsets[level].begin();
            iterIOEnd = this->pOrgsOut_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareStart2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pOrgsOutIds[level].end());
                
                next_from = get<3>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsOut_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsOut_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pOrgsOut_ioffsets[level][from]) == a))
            {
                iterStart = get<1>(this->pOrgsOut_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pOrgsOut_ioffsets[level][from+1]) : this->pOrgsOutIds[level].end());
                
                next_from = get<3>(this->pOrgsOut_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= (*iter);
    }
    else
        next_from = -1;
}


inline void HINT_M_ALL::scanFirstPartition_RepsIn(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> >::iterator iterIO, iterIO2, iterIOStart, iterIOEnd;
    size_t cnt = this->pRepsIn_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pRepsIn_ioffsets[level].begin();
            iterIOEnd = this->pRepsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareEnd2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pRepsInIds[level].end());
                
                next_from = get<3>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pRepsIn_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pRepsIn_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pRepsIn_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pRepsIn_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pRepsIn_ioffsets[level][from]) == a))
            {
                iterStart = get<1>(this->pRepsIn_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pRepsIn_ioffsets[level][from+1]) : this->pRepsInIds[level].end());
                
                next_from = get<3>(this->pRepsIn_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= (*iter);
    }
    else
        next_from = -1;
}


inline void HINT_M_ALL::scanFirstPartition_RepsIn(unsigned int level, Timestamp a, RecordEnd qdummyS, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterStart, iterEnd;
    RelationIdIterator iterI;
    tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pRepsIn_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pRepsIn_ioffsets[level].begin();
            iterIOEnd = this->pRepsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareEnd2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterStart = get<2>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<2>(*(iterIO+1)) : this->pRepsInTimestamp[level].end());
                
                next_from = get<3>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pRepsIn_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pRepsIn_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pRepsIn_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pRepsIn_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pRepsIn_ioffsets[level][from]) == a))
            {
                iterStart = get<2>(this->pRepsIn_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<2>(this->pRepsIn_ioffsets[level][from+1]) : this->pRepsInTimestamp[level].end());
                
                next_from = get<3>(this->pRepsIn_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        vector<Timestamp>::iterator pivot = lower_bound(iterStart, iterEnd, qdummyS.end);
        iterI = this->pRepsInIds[level].begin()+(pivot-this->pRepsInTimestamp[level].begin());
        for (iter = pivot; iter != iterEnd; iter++)
        {
            result ^= *iterI;
            iterI++;
        }
    }
    else
        next_from = -1;
}


inline void HINT_M_ALL::scanFirstPartition_RepsOut(unsigned int level, Timestamp a, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIdIterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pRepsOut_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = a;
            iterIOStart = this->pRepsOut_ioffsets[level].begin();
            iterIOEnd = this->pRepsOut_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareId);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pRepsOut[level].end());
                
                next_from = get<2>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pRepsOut_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pRepsOut_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pRepsOut_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pRepsOut_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pRepsOut_ioffsets[level][from]) == a))
            {
                iterStart = get<1>(this->pRepsOut_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pRepsOut_ioffsets[level][from+1]) : this->pRepsOut[level].end());
                
                next_from = get<2>(this->pRepsOut_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        for (iter = iterStart; iter != iterEnd; iter++)
            result ^= (*iter);
    }
    else
        next_from = -1;
}


inline void HINT_M_ALL::scanLastPartition_OrgsIn(unsigned int level, Timestamp b, pair<Timestamp, Timestamp> qdummyE, PartitionId &next_from, size_t &result)
{
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterStart, iterEnd;
    RelationIdIterator iterI;
    tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = b;
            iterIOStart = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareStartEnd2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == b))
            {
                iterI = get<1>(*iterIO);
                iterStart = get<2>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<2>(*(iterIO+1)) : this->pOrgsInTimestamps[level].end());
                
                next_from = get<3>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsIn_ioffsets[level][from]);
            /*if (tmp < b)
             {
             while ((get<0>(this->pOrgsIn_ioffsets[level][from]) < b) && (from < cnt))
             from++;
             }
             else*/ if (tmp > b)
             {
                 while ((get<0>(this->pOrgsIn_ioffsets[level][from]) > b) && (from > -1))
                     from--;
                 if ((get<0>(this->pOrgsIn_ioffsets[level][from]) != b) || (from == -1))
                     from++;
             }
            
            if ((from != cnt) && (get<0>(this->pOrgsIn_ioffsets[level][from]) == b))
            {
                iterI = get<1>(this->pOrgsIn_ioffsets[level][from]);
                iterStart = get<2>(this->pOrgsIn_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<2>(this->pOrgsIn_ioffsets[level][from+1]) : this->pOrgsInTimestamps[level].end());
                
                next_from = get<3>(this->pOrgsIn_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        vector<pair<Timestamp, Timestamp> >::iterator pivot = lower_bound(iterStart, iterEnd, qdummyE);
        //        RelationIdIterator tmp = this->pOrgsInIds[level].begin()+(iterStart-this->pOrgsInTimestamps[level].begin());
        for (iter = iterStart; iter != pivot; iter++)
        {
            result ^= *iterI;
            iterI++;
        }
    }
    else
        next_from = -1;
}



inline void HINT_M_ALL::scanLastPartition_OrgsOut(unsigned int level, Timestamp b, RecordStart qdummySE, PartitionId &next_from, size_t &result)
{
    vector<Timestamp>::iterator iter, iterStart, iterEnd;
    RelationIdIterator iterI;
    tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsOut_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = b;
            iterIOStart = this->pOrgsOut_ioffsets[level].begin();
            iterIOEnd = this->pOrgsOut_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, CompareStart2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == b))
            {
                iterI = get<1>(*iterIO);
                iterStart = get<2>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<2>(*(iterIO+1)) : this->pOrgsOutTimestamp[level].end());
                
                next_from = get<3>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsOut_ioffsets[level][from]);
            /*if (tmp < b)
             {
             while ((get<0>(this->pOrgsOut_ioffsets[level][from]) < b) && (from < cnt))
             from++;
             }
             else*/ if (tmp > b)
             {
                 while ((get<0>(this->pOrgsOut_ioffsets[level][from]) > b) && (from > -1))
                     from--;
                 if ((get<0>(this->pOrgsOut_ioffsets[level][from]) != b) || (from == -1))
                     from++;
             }
            
            if ((from != cnt) && (get<0>(this->pOrgsOut_ioffsets[level][from]) == b))
            {
                iterI = get<1>(this->pOrgsOut_ioffsets[level][from]);
                iterStart = get<2>(this->pOrgsOut_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<2>(this->pOrgsOut_ioffsets[level][from+1]) : this->pOrgsOutTimestamp[level].end());
                
                next_from = get<3>(this->pOrgsOut_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
        
        vector<Timestamp>::iterator pivot = lower_bound(iterStart, iterEnd, qdummySE.start);
        //        RelationIdIterator tmp = this->pOrgsOutIds[level].begin()+(iterStart-this->pOrgsOutTimestamp[level].begin());
        for (iter = iterStart; iter != pivot; iter++)
        {
            result ^= *iterI;
            iterI++;
        }
    }
    else
        next_from = -1;
}



inline void HINT_M_ALL::scanPartitions_OrgsIn(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationIdIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> qdummyA, qdummyB;
    vector<tuple<Timestamp, RelationIdIterator, vector<pair<Timestamp, Timestamp> >::iterator, PartitionId> >::iterator iterIO, iterIO2, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsIn_ioffsets[level].size();
    PartitionId from = next_from, to = next_to;
    
    
    if (cnt > 0)
    {
        from = next_from;
        to = next_to;
        
        // Adjusting pointers.
        if ((from == -1) || (to == -1))
        {
            get<0>(qdummyA) = a;
            iterIOStart = this->pOrgsIn_ioffsets[level].begin();
            iterIOEnd = this->pOrgsIn_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummyA, CompareStartEnd2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) <= b))
            {
                next_from = get<3>(*iterIO);
                
                get<0>(qdummyB) = b;
                iterStart = get<1>(*iterIO);
                
                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyB, CompareStartEnd2);
                //                iterIO2 = iterIO;
                //                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= b))
                //                    iterIO2++;
                
                iterEnd = ((iterIO2 != iterIOEnd) ? iterEnd = get<1>(*iterIO2): this->pOrgsInIds[level].end());
                for (iter = iterStart; iter != iterEnd; iter++)
                    //                    cout << "\tr" << iter->id << endl;
                    result ^= (*iter);
                
                if (iterIO2 != iterIOEnd)
                    next_to = get<3>(*iterIO2);
                else
                    next_to = -1;
            }
            else
                next_from = -1;
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsIn_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsIn_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            tmp = get<0>(this->pOrgsIn_ioffsets[level][to]);
            if (tmp > b)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][to]) > b) && (to > -1))
                    to--;
                to++;
            }
            //                else if (tmp <= b)
            else if (tmp == b)
            {
                while ((get<0>(this->pOrgsIn_ioffsets[level][to]) <= b) && (to < cnt))
                    to++;
            }
            
            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterStart = get<1>(this->pOrgsIn_ioffsets[level][from]);
                iterEnd   = (to != cnt)? get<1>(this->pOrgsIn_ioffsets[level][to]): this->pOrgsInIds[level].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= (*iter);
                
                next_from = get<3>(this->pOrgsIn_ioffsets[level][from]);
                next_to   = (to != cnt) ? get<3>(this->pOrgsIn_ioffsets[level][to])  : -1;
            }
            else
                next_from = next_to = -1;
        }
    }
    else
    {
        next_from = -1;
        next_to = -1;
    }
}


inline void HINT_M_ALL::scanPartitions_OrgsOut(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationIdIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> qdummyA, qdummyB;
    vector<tuple<Timestamp, RelationIdIterator, vector<Timestamp>::iterator, PartitionId> >::iterator iterIO, iterIO2, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgsOut_ioffsets[level].size();
    PartitionId from = next_from, to = next_to;
    
    
    if (cnt > 0)
    {
        // Adjusting pointers.
        if ((from == -1) || (to == -1))
        {
            get<0>(qdummyA) = a;
            iterIOStart = this->pOrgsOut_ioffsets[level].begin();
            iterIOEnd = this->pOrgsOut_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummyA, CompareStart2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) <= b))
            {
                next_from = get<3>(*iterIO);
                
                get<0>(qdummyB) = b;
                iterStart = get<1>(*iterIO);
                
                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyB, CompareStart2);
                //                iterIO2 = iterIO;
                //                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= b))
                //                    iterIO2++;
                
                iterEnd = ((iterIO2 != iterIOEnd) ? iterEnd = get<1>(*iterIO2): this->pOrgsOutIds[level].end());
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= (*iter);
                
                if (iterIO2 != iterIOEnd)
                    next_to = get<3>(*iterIO2);
                else
                    next_to = -1;
            }
            else
                next_from = -1;
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgsOut_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgsOut_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            tmp = get<0>(this->pOrgsOut_ioffsets[level][to]);
            if (tmp > b)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][to]) > b) && (to > -1))
                    to--;
                to++;
            }
            //                else if (tmp <= b)
            else if (tmp == b)
            {
                while ((get<0>(this->pOrgsOut_ioffsets[level][to]) <= b) && (to < cnt))
                    to++;
            }
            
            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterStart = get<1>(this->pOrgsOut_ioffsets[level][from]);
                iterEnd   = (to != cnt)? get<1>(this->pOrgsOut_ioffsets[level][to]): this->pOrgsOutIds[level].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= (*iter);
                
                next_from = get<3>(this->pOrgsOut_ioffsets[level][from]);
                next_to   = (to != cnt) ? get<3>(this->pOrgsOut_ioffsets[level][to])  : -1;
            }
            else
                next_from = next_to = -1;
        }
    }
    else
    {
        next_from = -1;
        next_to = -1;
    }
}


size_t HINT_M_ALL::executeBottomUp(StabbingQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterStart, iterEnd;
    vector<Timestamp>::iterator iterSStart, iterSEnd;
    vector<Timestamp>::iterator iterEStart, iterEEnd;
    RelationIdIterator iterIStart, iterIEnd;
    Timestamp a = Q.point >> (this->maxBits-this->numBits); // prefix
    RecordStart qdummySE(0, Q.point+1);
    RecordEnd qdummyS(0, Q.point);
    pair<Timestamp, Timestamp> qdummyE(Q.point+1, Q.point+1);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOoutA = -1, next_fromRinA = -1, next_fromRoutA = -1, next_fromOinB = -1, next_fromOoutB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOoutAB = -1, next_toOoutAB = -1, next_fromR = -1, next_fromO = -1, next_toO = -1;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            this->scanFirstPartition_OrgsIn(l, a, next_fromOinA, result);
            this->scanFirstPartition_OrgsOut(l, a, next_fromOoutA, result);
            
            this->scanFirstPartition_RepsIn(l, a, next_fromRinA, result);
            this->scanFirstPartition_RepsOut(l, a, next_fromRoutA, result);
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            this->scanFirstPartition_OrgsIn(l, a, Q.point, qdummyE, next_fromOinA, result);
            this->scanLastPartition_OrgsOut(l, a, qdummySE, next_fromOoutA, result);
            
            this->scanFirstPartition_RepsIn(l, a, qdummyS, next_fromRinA, result);
            this->scanFirstPartition_RepsOut(l, a, next_fromRoutA, result);
            
            if (a%2) //last bit of b is 1
                foundone = 1;
            if (!(a%2)) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
    }
    
    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterIStart = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
            result ^= (*iter);
    }
    else
    {
        // Comparisons needed
        iterStart = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = lower_bound(iterStart, this->pOrgsInTimestamps[this->numBits].end(), qdummyE, CompareTimestampsStart2);
        for (iter = iterStart; iter != iterEnd; iter++)
        {
            if (Q.point <= iter->second)
                result ^= this->pOrgsInIds[this->numBits][iter-iterStart];
        }
    }
    
    
    return result;
}


size_t HINT_M_ALL::executeBottomUp(RangeQuery Q)
{
    size_t result = 0;
    vector<pair<Timestamp, Timestamp> >::iterator iter, iterStart, iterEnd;
    vector<Timestamp>::iterator iterSStart, iterSEnd;
    vector<Timestamp>::iterator iterEStart, iterEEnd;
    RelationIdIterator iterIStart, iterIEnd;
    Timestamp a = Q.start >> (this->maxBits-this->numBits); // prefix
    Timestamp b = Q.end   >> (this->maxBits-this->numBits); // prefix
    RecordStart qdummySE(0, Q.end+1);
    RecordEnd qdummyS(0, Q.start);
    pair<Timestamp, Timestamp> qdummyE(Q.end+1, Q.end+1);
    bool foundzero = false;
    bool foundone = false;
    PartitionId next_fromOinA = -1, next_fromOoutA = -1, next_fromRinA = -1, next_fromRoutA = -1, next_fromOinB = -1, next_fromOoutB = -1, next_fromOinAB = -1, next_toOinAB = -1, next_fromOoutAB = -1, next_toOoutAB = -1, next_fromR = -1, next_fromO = -1, next_toO = -1;
    
    
    for (auto l = 0; l < this->numBits; l++)
    {
        if (foundone && foundzero)
        {
            // Partition totally covers lowest-level partition range that includes query range
            // all contents are guaranteed to be results
            
            // Handle the partition that contains a: consider both originals and replicas
            this->scanFirstPartition_RepsIn(l, a, next_fromRinA, result);
            this->scanFirstPartition_RepsOut(l, a, next_fromRoutA, result);
            
            this->scanPartitions_OrgsIn(l, a, b, next_fromOinAB, next_toOinAB, result);
            this->scanPartitions_OrgsOut(l, a, b, next_fromOoutAB, next_toOoutAB, result);
        }
        else
        {
            // Comparisons needed
            
            // Handle the partition that contains a: consider both originals and replicas, comparisons needed
            if (a == b)
            {
                // Special case when query overlaps only one partition, Lemma 3
                this->scanFirstPartition_OrgsIn(l, a, Q.start, qdummyE, next_fromOinA, result);
                this->scanLastPartition_OrgsOut(l, a, qdummySE, next_fromOoutA, result);
            }
            else
            {
                // Lemma 1
                this->scanFirstPartition_OrgsIn(l, a, Q.start, next_fromOinA, result);
                this->scanFirstPartition_OrgsOut(l, a, next_fromOoutA, result);
            }
            
            // Lemma 1, 3
            this->scanFirstPartition_RepsIn(l, a, qdummyS, next_fromRinA, result);
            this->scanFirstPartition_RepsOut(l, a, next_fromRoutA, result);
            
            if (a < b)
            {
                // Handle the rest before the partition that contains b: consider only originals, no comparisons needed
                this->scanPartitions_OrgsIn(l, a+1, b-1, next_fromOinAB, next_toOinAB, result);
                this->scanPartitions_OrgsOut(l, a+1, b-1, next_fromOoutAB, next_toOoutAB, result);
                
                // Handle the partition that contains b: consider only originals, comparisons needed
                this->scanLastPartition_OrgsIn(l, b, qdummyE, next_fromOinB, result);
                this->scanLastPartition_OrgsOut(l, b, qdummySE, next_fromOoutB, result);
            }
            
            if (b%2) //last bit of b is 1
                foundone = 1;
            if (!(a%2)) //last bit of a is 0
                foundzero = 1;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    if (foundone && foundzero)
    {
        // All contents are guaranteed to be results
        iterIStart = this->pOrgsInIds[this->numBits].begin();
        iterIEnd = this->pOrgsInIds[this->numBits].end();
        for (RelationIdIterator iter = iterIStart; iter != iterIEnd; iter++)
            result ^= (*iter);
    }
    else
    {
        // Comparisons needed
        iterStart = this->pOrgsInTimestamps[this->numBits].begin();
        iterEnd = lower_bound(iterStart, this->pOrgsInTimestamps[this->numBits].end(), qdummyE, CompareTimestampsStart2);
        for (iter = iterStart; iter != iterEnd; iter++)
        {
            if (Q.start <= iter->second)
                result ^= this->pOrgsInIds[this->numBits][iter-iterStart];
        }
    }
    
    
    return result;
}
