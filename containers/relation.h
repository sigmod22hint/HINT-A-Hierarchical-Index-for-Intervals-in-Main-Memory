#ifndef _RELATION_H_
#define _RELATION_H_

#include "../def_global.h"



class Record
{
public:
    RecordId id;
    Timestamp start;
    Timestamp end;
    
    Record();
    Record(RecordId id, Timestamp start, Timestamp end);
    
    bool operator < (const Record &rhs) const;
    bool operator >= (const Record &rhs) const;
    void print(const char c) const;
    ~Record();
};



class RecordStart
{
public:
    RecordId id;
    Timestamp start;
    
    RecordStart();
    RecordStart(RecordId id, Timestamp start);
    
    bool operator < (const RecordStart &rhs) const;
    bool operator >= (const RecordStart &rhs) const;
    void print(const char c) const;
    ~RecordStart();
};



// copy of RecordStart
class RecordEnd
{
public:
    RecordId id;
    Timestamp end;
    
    RecordEnd();
    RecordEnd(RecordId id, Timestamp end);
    
    bool operator < (const RecordEnd &rhs) const;
    bool operator >= (const RecordEnd &rhs) const;
    void print(const char c) const;
    ~RecordEnd();
};



// Ascending order
inline bool CompareByStart(const Record &lhs, const Record &rhs)
{
    return (lhs.start < rhs.start);
};


// Descending order
inline bool CompareByEnd(const Record &lhs, const Record &rhs)
{
    if (lhs.end == rhs.end)
        return (lhs.id < rhs.id);
    else
        return (lhs.end < rhs.end);
};



class Relation : public vector<Record>
{
public:
    Timestamp gstart;
    Timestamp gend;
    Timestamp longestRecord;
    float avgRecordExtent;
    
    Relation();
    Relation(Relation &R);
    void load(const char *filename);
    void sortByStart();
    void sortByEnd();
    void print(char c);
    ~Relation();
    
    // Range queries - used by the linear scan method.
    size_t execute(StabbingQuery Q);
    size_t execute(RangeQuery Q);    
};
typedef Relation::const_iterator RelationIterator;



class RelationStart : public vector<RecordStart>
{
public:
    RelationStart();
    void sortByStart();
    void print(char c);
    ~RelationStart();
};
typedef RelationStart::const_iterator RelationStartIterator;



// Copy of RelationStart
class RelationEnd : public vector<RecordEnd>
{
public:
    RelationEnd();
    void sortByEnd();
    void print(char c);
    ~RelationEnd();
};
typedef RelationEnd::const_iterator RelationEndIterator;


class RelationId : public vector<RecordId>
{
public:
    RelationId();
    void print(char c);
    ~RelationId();
};
typedef RelationId::const_iterator RelationIdIterator;
#endif //_RELATION_H_
