#include "timelineindex.h"



TimelineIndex::TimelineIndex(const Relation &R, const unsigned int numCheckpoints)
{
	this->numRecords = R.size();
	this->numCheckpoints =  numCheckpoints;
	this->min = R.gstart;
	this->max = R.gend;
	this->eventList.build(R, 0, 1);	
 	this->VersionMap = new CheckPoint[this->numCheckpoints+1];
    this->validIntervalCount = new int[this->numCheckpoints+1];


	int checkpointFrequency = (R.gend - R.gstart) / this->numCheckpoints;
	this->checkpointFrequency = checkpointFrequency;
	int checkpointTimestamp = R.gstart;
	int checkpointSpot = 0;
	int counter = 0 ;


	for(EndPointIndexEntry &r : this->eventList){

		while(r.endpoint > checkpointTimestamp){
			this->VersionMap[counter].checkpointTimestamp = checkpointTimestamp;
			this->VersionMap[counter].checkpointSpot = checkpointSpot;


			counter++;
			checkpointTimestamp+=checkpointFrequency;	
		}
		
		if(r.endpoint == checkpointTimestamp && r.isStart == 0){
			this->VersionMap[counter].checkpointTimestamp = checkpointTimestamp;
			this->VersionMap[counter].checkpointSpot = checkpointSpot;


			counter++;
			checkpointTimestamp+=checkpointFrequency;
		}

			
        checkpointSpot++;		

		if(!(r.isStart))
			continue;


		Timestamp end = (R[r.rid].end - this->min) / checkpointFrequency;
		Timestamp start = (r.endpoint - this->min) / checkpointFrequency;


		int startMod = (r.endpoint-this->min) % checkpointFrequency;

		if(startMod != 0)
			start++;


		for ( int i = start; i <= end; i++)
	        this->validIntervalCount[i]++;


 

	}
    
    for (int i = 0; i <= this->numCheckpoints; i++){
    	this->VersionMap[i].validIntervalIds = new int[this->validIntervalCount[i]];
    	this->validIntervalCount[i] = 0;
    }

	for(EndPointIndexEntry &r : this->eventList){


		if(!(r.isStart))
			continue;



		Timestamp end = (R[r.rid].end - this->min) / checkpointFrequency;
		
		// if (end > this->numCheckpoints -1)
  //     		end = this->numCheckpoints -1;
		Timestamp start = (r.endpoint - this->min) / checkpointFrequency;


		int startMod = (r.endpoint-this->min) % checkpointFrequency;

		if(startMod != 0)
			start++;


		for ( int i = start; i <= end; i++)
            this->VersionMap[i].validIntervalIds[this->validIntervalCount[i]++] = r.rid;


	}

	// for(int i=0; i <= this->numCheckpoints; i++){
	// 	cout <<  this->VersionMap[i].checkpointTimestamp << " " <<  this->VersionMap[i].checkpointSpot << " " << this->validIntervalCount[i] << endl;
	// }

	// exit(0);
}


size_t TimelineIndex::execute(RangeQuery Q)
{
    size_t result = 0;
	int startPoint = (Q.start-this->min) / this->checkpointFrequency;
	int qstartPoint = 0;
	// unordered_set<RecordId> tempValidIntervalIds = this->VersionMap[startPoint]->validIntervalIds;
	unordered_set<int> newInts;
	boost::dynamic_bitset<> deletedInts(this->numRecords);

	for(int i = this->VersionMap[startPoint].checkpointSpot; i < this->eventList.size(); ++i ){
		if (this->eventList[i].endpoint >= Q.start){
			// Report checkpoint's valid ints
			
			for(int j = 0; j < this->validIntervalCount[startPoint]; j++){
				if (deletedInts.test(this->VersionMap[startPoint].validIntervalIds[j]) == 0){//if (deletedInts.find(this->VersionMap[startPoint].validIntervalIds[i]) == deletedInts.end())
					result^= this->VersionMap[startPoint].validIntervalIds[j]; // cout << this->VersionMap[startPoint].validIntervalIds[j] << endl; // 
		
				}

			}

			// Report between Q.start and cp valid ints
			for(int r: newInts){
				result ^= r; //cout << r << endl; //  
			}

			qstartPoint = i;
			break;
			
		}


		if(this->eventList[i].isStart){
			newInts.insert(this->eventList[i].rid);
		}else if(newInts.erase(this->eventList[i].rid)==0){
			deletedInts[this->eventList[i].rid] = 1;		
		}

	}

	// cout << "newInts: " << newInts.size() << " deletedInts: " << deletedInts.size() << endl;
	int endPoint = Q.end;


	//cout << result << " "<< qstartPoint << endl;


	for(int i = qstartPoint; i < this->eventList.size(); i++){
	
			
			if(this->eventList[i].endpoint > endPoint)
				break;	

			if(this->eventList[i].isStart)
				result^= this->eventList[i].rid;
	}

    return result;

}

size_t TimelineIndex::execute(StabbingQuery Q)
{
    size_t result = 0;
	int startPoint = (Q.point-this->min) / this->checkpointFrequency;
	int qstartPoint = 0;
	// unordered_set<RecordId> tempValidIntervalIds = this->VersionMap[startPoint]->validIntervalIds;
	unordered_set<int> newInts;
	boost::dynamic_bitset<> deletedInts(this->numRecords);

	for(int i = this->VersionMap[startPoint].checkpointSpot; i < this->eventList.size(); ++i ){
		if (this->eventList[i].endpoint >= Q.point){
			// Report checkpoint's valid ints
			
			for(int j = 0; j < this->validIntervalCount[startPoint]; j++){
				if (deletedInts.test(this->VersionMap[startPoint].validIntervalIds[j]) == 0){//if (deletedInts.find(this->VersionMap[startPoint].validIntervalIds[i]) == deletedInts.end())
					result^= this->VersionMap[startPoint].validIntervalIds[j]; // cout << this->VersionMap[startPoint].validIntervalIds[j] << endl; // 
		
				}

			}

			// Report between Q.start and cp valid ints
			for(int r: newInts){
				result ^= r; //cout << r << endl; //  
			}

			qstartPoint = i;
			break;
			
		}


		if(this->eventList[i].isStart){
			newInts.insert(this->eventList[i].rid);
		}else if(newInts.erase(this->eventList[i].rid)==0){
			deletedInts[this->eventList[i].rid] = 1;		
		}

	}

	// cout << "newInts: " << newInts.size() << " deletedInts: " << deletedInts.size() << endl;
	int endPoint = Q.point;


	//cout << result << " "<< qstartPoint << endl;


	for(int i = qstartPoint; i < this->eventList.size(); i++){
	
			
			if(this->eventList[i].endpoint > endPoint)
				break;	

			if(this->eventList[i].isStart)
				result^= this->eventList[i].rid;
	}

    return result;

}


void TimelineIndex::getStats()
{

}


TimelineIndex::~TimelineIndex()
{

}


// CheckPoint::CheckPoint(int checkpointTimestamp, unordered_set<RecordId> validIntervalIds, int checkpointSpot){
// 	this->validIntervalIds = validIntervalIds;
// 	this->checkpointTimestamp = checkpointTimestamp;
// 	this->checkpointSpot = checkpointSpot;
// }
CheckPoint::CheckPoint(){

}
