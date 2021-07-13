#pragma once
#ifndef _EVENT_LIST_H_
#define _EVENT_LIST_H_

#include "../def_global.h"
#include "../containers/relation.h"



class EventListEntry
{
public:
	Timestamp endpoint;
	bool isStart;
	RecordId rid;
	
	EventListEntry();
	EventListEntry(Timestamp t, bool isStart, RecordId rid);
	bool operator < (const EventListEntry& rhs) const;
	bool operator <= (const EventListEntry& rhs) const;
	void print() const;
	void print(char c) const;
	~EventListEntry();
};

class EventList : public vector<EventListEntry>
{
public:
	size_t numEntries;
	size_t maxOverlappingRecordCount;

	EventList();
	void build(const Relation &R, size_t from, size_t by);
	void print(char c);
	~EventList();
};

typedef vector<EventListEntry>::const_iterator EventListIterator;
#endif //_EVENT_LIST_H_
