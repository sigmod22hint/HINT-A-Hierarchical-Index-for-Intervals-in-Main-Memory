#include "eventlist.h"



EventListEntry::EventListEntry()
{
}


EventListEntry::EventListEntry(Timestamp t, bool isStart, RecordId rid)
{
	this->endpoint = t;
	this->isStart = isStart;
	this->rid = rid;
}


EventListEntry::~EventListEntry()
{
}


bool EventListEntry::operator < (const EventListEntry& rhs) const
{
//    Timestamp diff = this->endpoint - rhs.endpoint;
	
//    return ((this->endpoint < rhs.endpoint) || (this->endpoint == rhs.endpoint) && (this->isStart) && (!rhs.isStart) ||
//            (this->endpoint == rhs.endpoint) && (this->isStart) && (rhs.isStart) && (this->rid < rhs.rid));
        return ((this->endpoint < rhs.endpoint) || (this->endpoint == rhs.endpoint) && (this->isStart) && (!rhs.isStart));
}


bool EventListEntry::operator <= (const EventListEntry& rhs) const
{
	return !(rhs < *this);
}


void EventListEntry::print() const
{
	cout << "<" << this->endpoint << ", " << ((this->isStart)? "START": "END") << ", r" << this->rid << ">" << endl;
}


void EventListEntry::print(char c) const
{
	cout << "<" << this->endpoint << ", " << ((this->isStart)? "START": "END") << ", " << c << this->rid << ">" << endl;
}




EventList::EventList()
{

}


void EventList::build(const Relation &R, size_t from = 0, size_t by = 1)
{
	// If |R|=3, then if by=2 and from=0, the tupleCount=2, and if from=1, the tupleCount=1
    size_t numRecordsR = R.size();
	size_t tupleCount = (numRecordsR + by - 1 - from) / by;


    // Step 1: create event entries.
	this->numEntries = tupleCount * 2;
	this->reserve(this->numEntries);
    for (size_t i = from; i < numRecordsR; i += by)
	{
        this->emplace_back(R[i].start, true,  i);
        this->emplace_back(R[i].end,   false, i);
    }

    // Step 2: sort index.
	sort(this->begin(), this->end());

    this->maxOverlappingRecordCount = R.size();
}





void EventList::print(char c)
{
	for (const EventListEntry& endpoint : (*this))
		endpoint.print(c);
}


EventList::~EventList()
{
	//delete[] this->entries;
}
