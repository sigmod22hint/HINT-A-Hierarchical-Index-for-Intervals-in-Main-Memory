#include "periodindex.h"



AdaptivePeriodIndex::AdaptivePeriodIndex(Relation &R, const unsigned int numLevels, const unsigned int numBuckets)
{
    this->numIndexedRecords = R.size();
    this->numLevels = numLevels;
    this->granS = 0;
    int max = -2147483647;
    int min = 2147483647;
    int levelSize = 0;
    int granularity = numBuckets*100;
    for (const Record &r : R){
        if(r.end > max)
            max = r.end;

        if(r.start < min)
            min = r.start;
    }

    this->min = min;
    this->max = max;
    
//    cout << this->min << " " << this->max << endl;

    this->numBuckets = numBuckets;
    this->granule = (this->max - this->min) / granularity ;

    this->histogram = (int *)malloc((granularity)*sizeof(int));

    this->endCounters = (int **)malloc((this->numLevels)*sizeof(int *));
    this->bucketcounters = (int **)malloc((this->numLevels)*sizeof(int *));
    for(auto i=0; i < this->numLevels; ++i){
        levelSize = pow(2, i);
        this->bucketcounters[i] = (int *)calloc((int)(levelSize*this->numBuckets),sizeof(int));
        this->endCounters[i] = (int *)calloc((int)(levelSize*this->numBuckets),sizeof(int));
    }

    R.sortByStart();


    for (const Record &r : R)
        this->updateHistogram(r);




    long  int S = 0;
    for(int i = 0; i < granularity; i++)
        S+=histogram[i];


    this->IntervalsPerBucket = S / this->numBuckets;

    S=0;

    int k = 0;

   for(int i = 0; i < granularity; i++){
        S+=histogram[i];


        if(S > this->IntervalsPerBucket){
            this->endCounters[0][k] = (i+1) * this->granule;
            for(int f=1; f < this->numLevels; ++f){
                for(int j=k*pow(2,f); j < (k+1)*pow(2,f); ++j){
                    if(j>0)
                        this->endCounters[f][j] = ((this->endCounters[0][k]-this->endCounters[0][k-1]) / pow(2,f)) +this->endCounters[f][j-1] ;
                    else
                        this->endCounters[f][j] = (this->endCounters[0][k] / pow(2,f));
                }
            }

            k++;  
            S = 0;
        }
    }


    this->endCounters[0][k] = this->max;
    for(int f=1; f < this->numLevels; ++f){
        for(int j=k*pow(2,f); j < ((k+1)*pow(2,f) - 1); ++j){
               this->endCounters[f][j] = ((this->endCounters[0][k]-this->endCounters[0][k-1]) / pow(2,f)) +this->endCounters[f][j-1];
        }
        this->endCounters[f][int((k+1)*pow(2,f)-1)] = this->max;
    }

    // for(auto i=0; i < this->numLevels; ++i){
    //     levelSize = pow(2, i);
    //     for(int j =0; j < levelSize*this->numBuckets; ++j)
    //         cout << this->endCounters[i][j] << " ";
    //     cout << " " << endl;
    // }

    // exit(EXIT_SUCCESS);

    for (const Record &r : R)
        this->updateCounters(r);
    

    this->buckets = (Relation **)malloc((this->numLevels)*sizeof(Relation*));




    for(auto i=0; i < this->numLevels; ++i){
        levelSize = pow(2, i);
        this->buckets[i] =  new Relation[levelSize*this->numBuckets];

        for (auto j = 0; j < levelSize*this->numBuckets ; j++) {
            this->buckets[i][j].resize(this->bucketcounters[i][j]);
            this->bucketcounters[i][j] = 0;

        }
    }

    //assignment of intervals to buckets

    for (const Record &r : R)
        this->updateBuckets(r);


}


AdaptivePeriodIndex::AdaptivePeriodIndex(Relation &R, Relation &M, const unsigned int numLevels, const unsigned int numBuckets)
{
    this->numIndexedRecords = R.size();
    this->numLevels = numLevels;
    this->granS = 0;
    int max = -2147483647;
    int min = 2147483647;
    int levelSize = 0;
    int granularity = numBuckets*100;
    for (const Record &r : R){
        if(r.end > max)
            max = r.end;
        
        if(r.start < min)
            min = r.start;
    }
    
    for (const Record &r : M){
         if(r.end > max)
             max = r.end;
    
         if(r.start < min)
             min = r.start;
    }
    
    this->min = min;
    this->max = max;
    
    //    cout << this->min << " " << this->max << endl;
    
    this->numBuckets = numBuckets;
    this->granule = (this->max - this->min) / granularity ;
    
    this->histogram = (int *)malloc((granularity)*sizeof(int));
    
    this->endCounters = (int **)malloc((this->numLevels)*sizeof(int *));
    this->bucketcounters = (int **)malloc((this->numLevels)*sizeof(int *));
    for(auto i=0; i < this->numLevels; ++i){
        levelSize = pow(2, i);
        this->bucketcounters[i] = (int *)calloc((int)(levelSize*this->numBuckets),sizeof(int));
        this->endCounters[i] = (int *)calloc((int)(levelSize*this->numBuckets),sizeof(int));
    }
    
    R.sortByStart();
    //    cout << "R sorted";gecthar();
    M.sortByStart();
    
    for (const Record &r : R)
        this->updateHistogram(r);
    
    
    for (const Record &r : M)
        this->updateHistogram(r);
    
    long  int S = 0;
    for(int i = 0; i < granularity; i++)
        S+=histogram[i];
    
    // exit(1);
    this->IntervalsPerBucket = S / this->numBuckets;
    
    S=0;
    
    int k = 0;
    
    // exit(0);
    
    for(int i = 0; i < granularity; i++){
        S+=histogram[i];
        if(S > this->IntervalsPerBucket){
            this->endCounters[0][k] = (i+1) * this->granule;
            for(int f=1; f < this->numLevels; ++f){
                for(int j=k*pow(2,f); j < (k+1)*pow(2,f); ++j){
                    if(j>0)
                        this->endCounters[f][j] = ((this->endCounters[0][k]-this->endCounters[0][k-1]) / pow(2,f)) +this->endCounters[f][j-1] ;
                    else
                        this->endCounters[f][j] = (this->endCounters[0][k] / pow(2,f));
                }
            }
            
            k++;
            S = 0;
        }
    }
    
    
    
    this->endCounters[0][k] = this->max;
    for(int f=1; f < this->numLevels; ++f){
        for(int j=k*pow(2,f); j < ((k+1)*pow(2,f) - 1); ++j){
            this->endCounters[f][j] = ((this->endCounters[0][k]-this->endCounters[0][k-1]) / pow(2,f)) +this->endCounters[f][j-1];
        }
        this->endCounters[f][int((k+1)*pow(2,f)-1)] = this->max;
    }
    
    // for(auto i=0; i < this->numLevels; ++i){
    //     levelSize = pow(2, i);
    //     for(int j =0; j < levelSize*this->numBuckets; ++j)
    //         cout << this->endCounters[i][j] << " ";
    //     cout << " " << endl;
    // }
    
    // exit(EXIT_SUCCESS);
    
    for (const Record &r : R)
        this->updateCounters(r);
    
    for (const Record &r : M)   // To allocate necessary memory for the updates
        this->updateCounters(r);
    
    
    this->buckets = (Relation **)malloc((this->numLevels)*sizeof(Relation*));
    
    
    
    
    for(int i=0; i < this->numLevels; ++i){
        int levelSize = pow(2, i);
        this->buckets[i] =  new Relation[levelSize*this->numBuckets];
        for (auto j = 0; j < levelSize*this->numBuckets ; j++) {
            this->buckets[i][j].resize(this->bucketcounters[i][j]);
            this->bucketcounters[i][j] = 0;
            
        }
    }
    
    //assignment of intervals to buckets
    
    for (const Record &r : R)
        this->updateBuckets(r);
    
    
}



inline void AdaptivePeriodIndex::updateHistogram(const Record &rec){

    int start = rec.start / this->granule;
    int end = rec.end / this->granule;
    

    for(int i = start; i <= end; i++)
        this->histogram[i]++;

}


inline void AdaptivePeriodIndex::updateCounters(const Record &rec){

    int level = 0;
    int sizeBucket = 0;
    int pos=0;
    int duration = rec.end-rec.start;

    while(rec.start > this->endCounters[0][pos])
        pos++;       
    


    if(pos > 0)
        sizeBucket = this->endCounters[0][pos] - this->endCounters[0][pos-1];
    else
        sizeBucket = this->endCounters[0][pos];


    while((duration < sizeBucket/2)&&(level != this->numLevels -1 )){
        level++;
        sizeBucket = sizeBucket / 2;
    }


    int startpos = pos*(pow(2,level));

    while(rec.start > this->endCounters[level][startpos])
        startpos++; 

    for(int i = startpos; i < pow(2,level) * this->numBuckets; i++){
        this->bucketcounters[level][i]++;      
        if(rec.end < this->endCounters[level][i])
            break;
    }


}


void AdaptivePeriodIndex::updateBuckets( const Record &rec)
{
    int level = 0;
    int sizeBucket = 0;
    int pos=0;
    int duration = rec.end-rec.start;

    while(rec.start > this->endCounters[0][pos])
        pos++;



    if(pos > 0)
        sizeBucket = this->endCounters[0][pos] - this->endCounters[0][pos-1];
    else
        sizeBucket = this->endCounters[0][pos];


    while((duration < sizeBucket/2)&&(level != this->numLevels -1 )){
        level++;
        sizeBucket = sizeBucket / 2;
    }



    int startpos = pos*(pow(2,level));

    while(rec.start > this->endCounters[level][startpos])
        startpos++; 

    for(int i = startpos; i < pow(2,level) * this->numBuckets; i++){
        this->buckets[level][i][this->bucketcounters[level][i]++] = rec;
        if(rec.end < this->endCounters[level][i])
            break;
    }

}


size_t AdaptivePeriodIndex::execute(StabbingQuery Q)
{
    size_t result = 0;
    int level = 0;
    int sizeBucket = 0;
    int pos= 0;
    int temp = 0;


    int startValue = Q.point;



    while(startValue > this->endCounters[0][pos])
        pos++;


    for(int i = 0; i < this->numLevels; ++i){
        while(startValue > this->endCounters[i][pos])
            pos++;

        for(int k = 0; k < this->bucketcounters[i][pos]; ++k){
            if(!(this->buckets[i][pos][k].end < Q.point || this->buckets[i][pos][k].start > Q.point))
                    result^= this->buckets[i][pos][k].id;
        }
     
    }
    return result;
}


size_t AdaptivePeriodIndex::execute(RangeQuery Q)
{
    size_t result = 0;
    int level = 0;
    int sizeBucket = 0;
    int pos= 0;
    int temp = 0;
    
    
    int startValue = Q.start;
    int endValue = Q.end;
    
    
    if(startValue < this->min)
        startValue = this->min;
    
    if(endValue > this->max)
        endValue = this->max;
    
    
    while(startValue > this->endCounters[0][pos])
        pos++;
    
    
    for(int i = 0; i < this->numLevels; ++i){
        while(startValue > this->endCounters[i][pos])
            pos++;
        for(int j = pos; j < pow(2,i) * this->numBuckets; ++j){
            for(int k = 0; k < this->bucketcounters[i][j]; ++k){
                if(!(this->buckets[i][j][k].end < Q.start || this->buckets[i][j][k].start > Q.end)){
                    temp = std::max(this->buckets[i][j][k].start, startValue);
                    if(j>0){
                        if(temp > this->endCounters[i][j-1])
                            result^= this->buckets[i][j][k].id;
                    }
                    else{
                        result^= this->buckets[i][j][k].id;
                    }
                }
            }
            if(Q.end < this->endCounters[i][j])
                break;
        }
    }
    return result;
}


void AdaptivePeriodIndex::getStats()
{
    int levelSize;

    int sumR = 0 ;
    this->numPartitions = 0 ;
    this->numEmptyPartitions = 0;



    for(int i=0; i < this->numLevels; ++i){
        levelSize = pow(2, i);
        this->numPartitions += this->numBuckets * levelSize;
        for(int j=0; j < this->numBuckets * levelSize; ++j){
            sumR+=this->bucketcounters[i][j];
            if(this->bucketcounters[i][j] == 0)
                this->numEmptyPartitions++;
        }
            
    }

    this->numOriginals = this->numIndexedRecords;
    this->numReplicas  = sumR - this->numOriginals;
    this->avgPartitionSize = (float)(sumR/this->numPartitions);
    this->sizeInBytes = this->numIndexedRecords*(sizeof(RecordId)+sizeof(Timestamp)*2);
}
