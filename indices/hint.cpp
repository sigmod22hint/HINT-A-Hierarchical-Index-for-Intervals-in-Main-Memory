#include "hint.h"
#include <unordered_set>



inline bool Compare2(const tuple<Timestamp, RelationIdIterator, PartitionId> &lhs, const tuple<Timestamp, RelationIdIterator, PartitionId> &rhs)
{
    return (get<0>(lhs) < get<0>(rhs));
}


inline void HINT::updateCounters(const Record &r)
{
    int level = 0;
    Timestamp a = r.start >> (this->maxBits-this->numBits);
    Timestamp b = r.end   >> (this->maxBits-this->numBits);
    Timestamp prevb;
    int firstfound = 0;
    
    
    while ((level < this->height) && (a <= b))
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


inline void HINT::updatePartitions(const Record &r)
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
                this->pReps[level][a][this->pReps_sizes[level][a]] = r.id;
                this->pReps_sizes[level][a]++;
            }
            else
            {
                this->pOrgs[level][a][this->pOrgs_sizes[level][a]] = r.id;
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
                this->pOrgs[level][prevb][this->pOrgs_sizes[level][prevb]] = r.id;
                this->pOrgs_sizes[level][prevb]++;
            }
            else
            {
                this->pReps[level][prevb][this->pReps_sizes[level][prevb]] = r.id;
                this->pReps_sizes[level][prevb]++;
            }
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


//inline void HINT::updatePartitionsUpdates(const Record &r)
//{
//    int level = 0;
//    Timestamp a = r.start >> (this->maxBits-this->numBits);
//    Timestamp b = r.end   >> (this->maxBits-this->numBits);
//    Timestamp prevb;
//    int firstfound = 0;
//    
//    
//    while (level < this->height && a <= b)
//    {
//        if (a%2)
//        { //last bit of a is 1
//            if (firstfound)
//            {
//                this->pReps[level][a].push_back(r.id);
////                this->pReps_sizes[level][a]++;
//            }
//            else
//            {
//                this->pOrgs[level][a].push_back(r.id);
////                this->pOrgs_sizes[level][a]++;
//                firstfound = 1;
//            }
//            //a+=(int)(pow(2,level));
//            a++;
//        }
//        if (!(b%2))
//        { //last bit of b is 0
//            prevb = b;
//            b--;
//            //b-=(int)(pow(2,level));
//            if ((!firstfound) && b < a)
//            {
//                this->pOrgs[level][prevb].push_back(r.id);
////                this->pOrgs_sizes[level][prevb]++;
//            }
//            else
//            {
//                this->pReps[level][prevb].push_back(r.id);
////                this->pReps_sizes[level][prevb]++;
//            }
//        }
//        a >>= 1; // a = a div 2
//        b >>= 1; // b = b div 2
//        level++;
//    }
//}


HINT::HINT(const Relation &R, const unsigned int numBits) : HierarchicalIndex(R, numBits, numBits)
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
    this->pOrgs = new RelationId*[this->height];
    this->pReps = new RelationId*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));

        this->pOrgs[l] = new RelationId[cnt];
        this->pReps[l] = new RelationId[cnt];

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
//        auto cnt = (int)(pow(2, this->numBits-l));
//        auto countR = 0, countO = 0, sumO = 0, sumR = 0;
//        for (int i = 0; i < cnt; i++)
//        {
//            if (this->pOrgs_sizes[l][i] > 0)
//            {
//                countO++;
//                sumO += this->pOrgs_sizes[l][i];
//            }
//        }
//        for (int i = 0; i < cnt; i++)
//        {
//            if (this->pReps_sizes[l][i] > 0)
//            {
//                countR++;
//                sumR += this->pReps_sizes[l][i];
//            }
//        }
//        cout << l << "\t" << countO << "\t" << ((countO)? (float)sumO/countO: 0) << "\t" << countR << "\t" << ((countR)? (float)sumR/countR: 0) << endl;
        free(this->pOrgs_sizes[l]);
        free(this->pReps_sizes[l]);
    }
    free(this->pOrgs_sizes);
    free(this->pReps_sizes);
}


// For updates only
HINT::HINT(const Relation &R, const Relation &M, const unsigned int numBits) : HierarchicalIndex(R, numBits, numBits)
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
    for (const Record &r : M)   // To allocate necessary memory for the updates
        this->updateCounters(r);

    // Step 2: allocate necessary memory.
    this->pOrgs = new RelationId*[this->height];
    this->pReps = new RelationId*[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        this->pOrgs[l] = new RelationId[cnt];
        this->pReps[l] = new RelationId[cnt];
        
        for (auto j = 0; j < cnt; j++)
        {
            this->pOrgs[l][j].reserve(this->pOrgs_sizes[l][j]);
            this->pReps[l][j].reserve(this->pReps_sizes[l][j]);
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
        this->updatePartitionsUpdates(r);
    
    // Free auxiliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgs_sizes[l]);
        free(this->pReps_sizes[l]);
    }
    free(this->pOrgs_sizes);
    free(this->pReps_sizes);
}

void HINT::print(char c)
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


void HINT::getStats()
{
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);
        size_t yo = this->numEmptyPartitions;

        this->numPartitions += cnt;
        for (int j = 0; j < cnt; j++)
        {
            this->numOriginals += this->pOrgs[l][j].size();
            this->numReplicas += this->pReps[l][j].size();
            if ((this->pOrgs[l][j].empty()) && (this->pReps[l][j].empty()))
                this->numEmptyPartitions++;
        }
    }

    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicas)/(this->numPartitions-numEmptyPartitions);
}


HINT::~HINT()
{
    for (auto l = 0; l < this->height; l++)
    {
        delete[] this->pOrgs[l];
        delete[] this->pReps[l];
    }
    delete[] this->pOrgs;
    delete[] this->pReps;
}


size_t HINT::execute(StabbingQuery Q)
{
    size_t result = 0;
    RelationIdIterator iterEnd;
    Timestamp a = Q.point;


    // Traverse relations using iterators
    for (auto l = 0; l < this->numBits; l++)
    {
        // Handle the partition that contains a.
        iterEnd = this->pOrgs[l][a].end();
        for (RelationIdIterator iter = this->pOrgs[l][a].begin(); iter != iterEnd; iter++)
            result ^= (*iter);
        
        iterEnd = this->pReps[l][a].end();
        for (RelationIdIterator iter = this->pReps[l][a].begin(); iter != iterEnd; iter++)
            result ^= (*iter);

        a >>= 1; // a = a div 2
    }
    
    // Handle root.
    iterEnd = this->pOrgs[this->numBits][0].end();
    for (RelationIdIterator iter = this->pOrgs[this->numBits][0].begin(); iter != iterEnd; iter++)
        result ^= (*iter);
    

    return result;
}


size_t HINT::execute(RangeQuery Q)
{
    size_t result = 0;
    RelationIdIterator iterEnd;
    Timestamp a = Q.start;
    Timestamp b = Q.end;
    

    for (auto l = 0; l < this->numBits; l++)
    {
        // Handle the partition that contains a.
        iterEnd = this->pOrgs[l][a].end();
        for (RelationIdIterator iter = this->pOrgs[l][a].begin(); iter != iterEnd; iter++)
            result ^= (*iter);
        
        iterEnd = this->pReps[l][a].end();
        for (RelationIdIterator iter = this->pReps[l][a].begin(); iter != iterEnd; iter++)
            result ^= (*iter);
        
        // Handle the rest up to the partition that contains b.
        for (auto j = a+1; j <= b; j++)
        {
            iterEnd = this->pOrgs[l][j].end();
            for (RelationIdIterator iter = this->pOrgs[l][j].begin(); iter != iterEnd; iter++)
                result ^= (*iter);
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root.
    iterEnd = this->pOrgs[this->numBits][0].end();
    for (RelationIdIterator iter = this->pOrgs[this->numBits][0].begin(); iter != iterEnd; iter++)
        result ^= (*iter);
    
    
    return result;
}



inline void HINT_SS::updateCounters(const Record &r)
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


inline void HINT_SS::updatePartitions(const Record &r)
{
    int level = 0;
    Timestamp a = r.start >> (this->maxBits-this->numBits);
    Timestamp b = r.end   >> (this->maxBits-this->numBits);
    int firstfound = 0;
    Timestamp prevb;
    
    
    while (level < this->height && a <= b)
    {
        if (a%2)
        { //last bit of a is 1
            if (firstfound)
                this->pReps[level][this->pReps_offsets[level][a]++] = r.id;
            else
            {
                this->pOrgs[level][this->pOrgs_offsets[level][a]++] = r.id;
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
                this->pOrgs[level][this->pOrgs_offsets[level][prevb]++] = r.id;
            else
                this->pReps[level][this->pReps_offsets[level][prevb]++] = r.id;
        }
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
        level++;
    }
}


HINT_SS::HINT_SS(const Relation &R, const unsigned int numBits) : HierarchicalIndex(R, numBits, numBits)
{
    tuple<Timestamp, RelationIdIterator, PartitionId> dummy;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    PartitionId tmp = -1;
    

    // Step 1: one pass to count the contents inside each partition.
    this->pOrgs_sizes   = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pReps_sizes   = (size_t **)malloc(this->height*sizeof(size_t *));
    this->pOrgs_offsets = (RecordId **)malloc(this->height*sizeof(RecordId *));
    this->pReps_offsets = (size_t **)malloc(this->height*sizeof(size_t *));
    
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        
        //calloc allocates memory and sets each counter to 0
        this->pOrgs_sizes[l]   = (RecordId *)calloc(cnt, sizeof(RecordId));
        this->pReps_sizes[l]   = (size_t *)calloc(cnt, sizeof(size_t));
        this->pOrgs_offsets[l] = (RecordId *)calloc(cnt+1, sizeof(RecordId));
        this->pReps_offsets[l] = (size_t *)calloc(cnt+1, sizeof(size_t));
    }
    
    for (const Record &r : R)
        this->updateCounters(r);
    
    
    // Step 2: allocate necessary memory.
    this->pOrgs = new RelationId[this->height];
    this->pReps = new RelationId[this->height];
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumO = 0, sumR = 0;
        
        for (auto pId = 0; pId < cnt; pId++)
        {
            this->pOrgs_offsets[l][pId] = sumO;
            this->pReps_offsets[l][pId] = sumR;
            sumO += this->pOrgs_sizes[l][pId];
            sumR += this->pReps_sizes[l][pId];
        }
        this->pOrgs_offsets[l][cnt] = sumO;
        this->pReps_offsets[l][cnt] = sumR;
        
        this->pOrgs[l].resize(sumO);
        this->pReps[l].resize(sumR);
    }
    
    
    // Step 3: fill partitions.
    for (const Record &r : R)
        this->updatePartitions(r);
    
    
    // Free auxliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgs_offsets[l]);
        free(this->pReps_offsets[l]);
    }
    free(this->pOrgs_offsets);
    free(this->pReps_offsets);
    
    
    // Step 4: create offset pointers.
    this->pOrgs_ioffsets = new vector<tuple<Timestamp, RelationIdIterator, PartitionId> >[this->height];
    this->pReps_ioffsets = new vector<tuple<Timestamp, RelationIdIterator, PartitionId> >[this->height];
    for (int l = this->height-1; l > -1; l--)
    {
        auto cnt = (int)(pow(2, this->numBits-l));
        size_t sumO = 0, sumR = 0;
        
        //        cout << "l" << l << " (" << cnt << ")" << endl;
        for (auto pId = 0; pId < cnt; pId++)
        {
            get<0>(dummy) = pId >> 1;   //((pId >> (this->maxBits-this->numBits)) >> 1); ????
            if (this->pOrgs_sizes[l][pId] > 0)
            {
                //                cout << "\tp" << pId << ": " << this->pOrgs_sizes[l][pId] << endl;
                tmp = -1;
                if (l < this->height-1)
                {
                    //                    cout << "\t\tlower  bound of " << get<0>(dummy) << " on prev level (" << this->pOrgs_ioffsets[l+1].size() << "):";
                    iterIOStart = this->pOrgs_ioffsets[l+1].begin();
                    iterIOEnd = this->pOrgs_ioffsets[l+1].end();
                    //                    for (iterIO = this->pOrgs_ioffsets[l+1].begin(); iterIO != iterIOEnd; iterIO++)
                    //                        cout << " [p" << get<0>(*iterIO) << ", r" << *(get<1>(*iterIO)) << ", " << get<2>(*iterIO) << "]";
                    //                    cout << endl;
                    iterIO = lower_bound(iterIOStart, iterIOEnd, dummy, Compare2);
                    tmp = (iterIO != iterIOEnd)? (iterIO-iterIOStart): -1;
                }
                this->pOrgs_ioffsets[l].push_back(tuple<Timestamp, RelationIdIterator, PartitionId>(pId, this->pOrgs[l].begin()+sumO, tmp));
                //                cout << "\t\tadd [p" << pId << ", r" << (*(this->pOrgs[l].begin()+sumO)) << ", " << tmp << "]";cout<<endl;//getchar();
            }
            //            else
            //                cout << "\t\tempty" << endl;
            if (this->pReps_sizes[l][pId] > 0)
            {
                //                cout << "\tp" << pId << ": " << this->pReps_sizes[l][pId] << endl;
                tmp = -1;
                if (l < this->height-1)
                {
                    ////                    cout << "\t\tlower  bound of " << get<0>(dummy) << " on prev level (" << this->pReps_ioffsets[l+1].size() << "):";
                    iterIOStart = this->pReps_ioffsets[l+1].begin();
                    iterIOEnd = this->pReps_ioffsets[l+1].end();
                    ////                    for (iterIO = this->pReps_ioffsets[l+1].begin(); iterIO != iterIOEnd; iterIO++)
                    ////                        cout << " [p" << get<0>(*iterIO) << ", r" << *(get<1>(*iterIO)) << ", " << get<2>(*iterIO) << "]";
                    ////                    cout << endl;
                    iterIO = lower_bound(iterIOStart, iterIOEnd, dummy, Compare2);
                    tmp = (iterIO != iterIOEnd)? (iterIO-iterIOStart): -1;
                }
                this->pReps_ioffsets[l].push_back(tuple<Timestamp, RelationIdIterator, PartitionId>(pId, this->pReps[l].begin()+sumR, tmp));
                //                cout << "\t\tadd [p" << pId << ", r" << (*(this->pReps[l].begin()+sumO)) << ", " << tmp << "]";getchar();
            }
            
            sumO += this->pOrgs_sizes[l][pId];
            sumR += this->pReps_sizes[l][pId];
        }
        //        this->pOrgs_ioffsets[l][cnt] = this->pOrgs[l].end();
        //        this->pReps_ioffsets[l][cnt] = this->pReps[l].end();
        
    }
    
    
    // Free auxliary memory.
    for (auto l = 0; l < this->height; l++)
    {
        free(this->pOrgs_sizes[l]);
        free(this->pReps_sizes[l]);
    }
    free(this->pOrgs_sizes);
    free(this->pReps_sizes);
    
    //    for (auto l = 0; l < this->height; l++)
    //    {
    //        cout << "l = " << l << ": pOrgs_ioffsets = " << pOrgs_ioffsets.size() << ", pReps_ioffsets = " << pReps_ioffsets.size() << endl;
    //    }
}


void HINT_SS::getStats()
{
    for (auto l = 0; l < this->height; l++)
    {
        auto cnt = pow(2, this->numBits-l);

        this->numPartitions += cnt;
        this->numOriginals += this->pOrgs[l].size();
        this->numReplicas += this->pReps[l].size();

        unordered_set<PartitionId> U;
        for (int j = 0; j < this->pOrgs_ioffsets[l].size(); j++)
            U.insert(get<0>(this->pOrgs_ioffsets[l][j]));
        for (int j = 0; j < this->pReps_ioffsets[l].size(); j++)
            U.insert(get<0>(this->pReps_ioffsets[l][j]));
        
        this->numEmptyPartitions += cnt-U.size();
    }
    
    this->avgPartitionSize = (float)(this->numIndexedRecords+this->numReplicas)/(this->numPartitions-numEmptyPartitions);
}


HINT_SS::~HINT_SS()
{
    delete[] this->pOrgs_ioffsets;
    delete[] this->pReps_ioffsets;
    delete[] this->pOrgs;
    delete[] this->pReps;
}


inline void HINT_SS::scanPartition_Orgs(unsigned int level, Timestamp t, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIdIterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgs_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = t;
            iterIOStart = this->pOrgs_ioffsets[level].begin();
            iterIOEnd = this->pOrgs_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, Compare2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == t))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pOrgs[level].end());
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= (*iter);
                
                next_from = get<2>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pOrgs_ioffsets[level][from]);
            if (tmp < t)
            {
                while ((get<0>(this->pOrgs_ioffsets[level][from]) < t) && (from < cnt))
                    from++;
            }
            else if (tmp > t)
            {
                while ((get<0>(this->pOrgs_ioffsets[level][from]) > t) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgs_ioffsets[level][from]) != t) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pOrgs_ioffsets[level][from]) == t))
            {
                iterStart = get<1>(this->pOrgs_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pOrgs_ioffsets[level][from+1]) : this->pOrgs[level].end());
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= (*iter);
                
                next_from = get<2>(this->pOrgs_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
    }
    else
        next_from = -1;
}


inline void HINT_SS::scanPartition_Reps(unsigned int level, Timestamp t, PartitionId &next_from, size_t &result)
{
    RelationIdIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIdIterator, PartitionId> qdummy;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    size_t cnt = this->pReps_ioffsets[level].size();
    PartitionId from = next_from;
    
    
    if (cnt > 0)
    {
        if (from == -1)
        {
            get<0>(qdummy) = t;
            iterIOStart = this->pReps_ioffsets[level].begin();
            iterIOEnd = this->pReps_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummy, Compare2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == t))
            {
                iterStart = get<1>(*iterIO);
                iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pReps[level].end());
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= (*iter);
                
                next_from = get<2>(*iterIO);
            }
        }
        else
        {
            Timestamp tmp = get<0>(this->pReps_ioffsets[level][from]);
            if (tmp < t)
            {
                while ((get<0>(this->pReps_ioffsets[level][from]) < t) && (from < cnt))
                    from++;
            }
            else if (tmp > t)
            {
                while ((get<0>(this->pReps_ioffsets[level][from]) > t) && (from > -1))
                    from--;
                if ((get<0>(this->pReps_ioffsets[level][from]) != t) || (from == -1))
                    from++;
            }
            
            if ((from != cnt) && (get<0>(this->pReps_ioffsets[level][from]) == t))
            {
                iterStart = get<1>(this->pReps_ioffsets[level][from]);
                iterEnd = ((from+1 != cnt) ? get<1>(this->pReps_ioffsets[level][from+1]) : this->pReps[level].end());
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= (*iter);
                
                next_from = get<2>(this->pReps_ioffsets[level][from]);
            }
            else
                next_from = -1;
        }
    }
    else
        next_from = -1;
}


inline void HINT_SS::scanPartitions_Orgs(unsigned int level, Timestamp a, Timestamp b, PartitionId &next_from, PartitionId &next_to, size_t &result)
{
    RelationIdIterator iter, iterStart, iterEnd;
    tuple<Timestamp, RelationIdIterator, PartitionId> qdummyA, qdummyB;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> >::iterator iterIO, iterIO2, iterIOStart, iterIOEnd;
    size_t cnt = this->pOrgs_ioffsets[level].size();
    PartitionId from = next_from, to = next_to;
    
    if (cnt > 0)
    {
        from = next_from;
        to = next_to;
        
        // Adjusting pointers.
        if ((from == -1) || (to == -1))
        {
            get<0>(qdummyA) = a;
            iterIOStart = this->pOrgs_ioffsets[level].begin();
            iterIOEnd = this->pOrgs_ioffsets[level].end();
            iterIO = lower_bound(iterIOStart, iterIOEnd, qdummyA, Compare2);
            if ((iterIO != iterIOEnd) && (get<0>(*iterIO) <= b))
            {
                next_from = get<2>(*iterIO);
                
                get<0>(qdummyB) = b;
                iterStart = get<1>(*iterIO);
                
                iterIO2 = upper_bound(iterIO, iterIOEnd, qdummyB, Compare2);
                //                vector<tuple<Timestamp, RelationIdIterator, PartitionId> >::iterator  yo = upper_bound(iterIO, iterIOEnd, qdummyB, Compare2);
                //                iterIO2 = iterIO;
                //                while ((iterIO2 != iterIOEnd) && (get<0>(*iterIO2) <= b))
                //                    iterIO2++;
                
                iterEnd = ((iterIO2 != iterIOEnd) ? iterEnd = get<1>(*iterIO2): pOrgs[level].end());
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= (*iter);
                
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
            Timestamp tmp = get<0>(this->pOrgs_ioffsets[level][from]);
            if (tmp < a)
            {
                while ((get<0>(this->pOrgs_ioffsets[level][from]) < a) && (from < cnt))
                    from++;
            }
            else if (tmp > a)
            {
                while ((get<0>(this->pOrgs_ioffsets[level][from]) > a) && (from > -1))
                    from--;
                if ((get<0>(this->pOrgs_ioffsets[level][from]) != a) || (from == -1))
                    from++;
            }
            
            tmp = get<0>(this->pOrgs_ioffsets[level][to]);
            if (tmp > b)
            {
                while ((get<0>(this->pOrgs_ioffsets[level][to]) > b) && (to > -1))
                    to--;
                to++;
            }
            //                else if (tmp <= b)
            else if (tmp == b)
            {
                while ((get<0>(this->pOrgs_ioffsets[level][to]) <= b) && (to < cnt))
                    to++;
            }
            
            if ((from != cnt) && (from != -1) && (from < to))
            {
                iterStart = get<1>(this->pOrgs_ioffsets[level][from]);
                iterEnd   = (to != cnt)? get<1>(this->pOrgs_ioffsets[level][to]): this->pOrgs[level].end();
                for (iter = iterStart; iter != iterEnd; iter++)
                    result ^= (*iter);
                
                next_from = get<2>(this->pOrgs_ioffsets[level][from]);
                next_to   = (to != cnt) ? get<2>(this->pOrgs_ioffsets[level][to])  : -1;
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


size_t HINT_SS::execute(StabbingQuery Q)
{
    size_t result = 0;
    RelationIdIterator iter, iterStart, iterEnd;
    Timestamp a = Q.point;
    tuple<Timestamp, RelationIdIterator, PartitionId> qdummyA;
    vector<tuple<Timestamp, RelationIdIterator, PartitionId> >::iterator iterIO, iterIOStart, iterIOEnd;
    PartitionId next_fromO = -1, fromO, next_fromR = -1, fromR;
    
    
    // Handle all levels before the root
    for (auto l = 0; l < this->numBits; l++)
    {
        size_t cnt = this->pReps_ioffsets[l].size();
        fromR = next_fromR;
        if (cnt > 0)
        {
            if (fromR == -1)
            {
                get<0>(qdummyA) = a;
                iterIOStart = this->pReps_ioffsets[l].begin();
                iterIOEnd = this->pReps_ioffsets[l].end();
                iterIO = lower_bound(iterIOStart, iterIOEnd, qdummyA, Compare2);
                if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
                {
                    iterStart = get<1>(*iterIO);
                    iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pReps[l].end());
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= (*iter);
                    
                    next_fromR = get<2>(*iterIOStart);
                }
            }
            else
            {
                Timestamp tmp = get<0>(this->pReps_ioffsets[l][fromR]);
                if (tmp < a)
                {
                    while ((get<0>(this->pReps_ioffsets[l][fromR]) < a) && (fromR < cnt))
                        fromR++;
                }
                else if (tmp > a)
                {
                    while ((get<0>(this->pReps_ioffsets[l][fromR]) > a) && (fromR > -1))
                        fromR--;
                    if ((get<0>(this->pReps_ioffsets[l][fromR]) != a) || (fromR == -1))
                        fromR++;
                }
                
                if ((fromR != cnt) && (get<0>(this->pReps_ioffsets[l][fromR]) == a))
                {
                    iterStart = get<1>(this->pReps_ioffsets[l][fromR]);
                    iterEnd = ((fromR+1 != cnt) ? get<1>(this->pReps_ioffsets[l][fromR+1]) : this->pReps[l].end());
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= (*iter);
                    
                    next_fromR = get<2>(this->pReps_ioffsets[l][fromR]);
                }
                else
                    next_fromR = -1;
            }
        }
        
        cnt = this->pOrgs_ioffsets[l].size();
        fromO = next_fromO;
        if (cnt > 0)
        {
            // Adjusting pointers.
            if (fromO == -1)
            {
                get<0>(qdummyA) = a;
                iterIOStart = this->pOrgs_ioffsets[l].begin();
                iterIOEnd = this->pOrgs_ioffsets[l].end();
                iterIO = lower_bound(iterIOStart, iterIOEnd, qdummyA, Compare2);
                if ((iterIO != iterIOEnd) && (get<0>(*iterIO) == a))
                {
                    iterStart = get<1>(*iterIO);
                    iterEnd = ((iterIO+1 != iterIOEnd) ? get<1>(*(iterIO+1)) : this->pOrgs[l].end());
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= (*iter);
                    
                    next_fromO = get<2>(*iterIO);
                }
            }
            else
            {
                Timestamp tmp = get<0>(this->pOrgs_ioffsets[l][fromO]);
                if (tmp < a)
                {
                    while ((get<0>(this->pOrgs_ioffsets[l][fromO]) < a) && (fromO < cnt))
                        fromO++;
                }
                else if (tmp > a)
                {
                    while ((get<0>(this->pOrgs_ioffsets[l][fromO]) > a) && (fromO > -1))
                        fromO--;
                    if ((get<0>(this->pOrgs_ioffsets[l][fromO]) != a) || (fromO == -1))
                        fromO++;
                }
                
                if ((fromO != cnt) && (get<0>(this->pOrgs_ioffsets[l][fromO]) == a))
                {
                    iterStart = get<1>(this->pOrgs_ioffsets[l][fromO]);
                    iterEnd = ((fromO+1 != cnt) ? get<1>(this->pOrgs_ioffsets[l][fromO+1]) : this->pOrgs[l].end());
                    for (iter = iterStart; iter != iterEnd; iter++)
                        result ^= (*iter);
                    
                    next_fromO = get<2>(this->pOrgs_ioffsets[l][fromO]);
                }
                else
                    next_fromO = -1;
            }
        }
        
        a >>= 1; // a = a div 2
    }
    
    // Handle root.
    iterEnd = this->pOrgs[this->numBits].end();
    for (RelationIdIterator iter = this->pOrgs[this->numBits].begin(); iter != iterEnd; iter++)
        result ^= (*iter);
    
    
    return result;
}


size_t HINT_SS::execute(RangeQuery Q)
{
    size_t result = 0;
    RelationIdIterator iter, iterStart, iterEnd;
    Timestamp a = Q.start;
    Timestamp b = Q.end;
    PartitionId fromO = -1, toO = -1, fromR = -1;
    
    
    // Handle all levels before the root
    for (auto l = 0; l < this->numBits; l++)
    {
        this->scanPartition_Reps(l, a, fromR, result);
        
        this->scanPartitions_Orgs(l, a, b, fromO, toO, result);
        
        a >>= 1; // a = a div 2
        b >>= 1; // b = b div 2
    }
    
    // Handle root
    iterEnd = this->pOrgs[this->numBits].end();
    for (RelationIdIterator iter = this->pOrgs[this->numBits].begin(); iter != iterEnd; iter++)
        result ^= (*iter);
    
    
    return result;
}
