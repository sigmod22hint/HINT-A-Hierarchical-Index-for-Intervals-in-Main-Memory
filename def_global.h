#pragma once
#ifndef _GLOBAL_DEF_H_
#define _GLOBAL_DEF_H_

#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <vector>
#include <string>
#include <algorithm>
#include <fstream>
#include <chrono>
#include <unistd.h>
#include <tuple>
using namespace std;


#define QUERY_STABBING 0
#define QUERY_RANGE    1

#define HINT_OPTIMIZATIONS_NO 0
#define HINT_OPTIMIZATIONS_SS 1

#define HINT_M_OPTIMIZATIONS_NO                   0
#define HINT_M_OPTIMIZATIONS_SUBS                 1
#define HINT_M_OPTIMIZATIONS_SUBS_SORT            2
#define HINT_M_OPTIMIZATIONS_SUBS_SOPT            3
#define HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT       4
#define HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT_SS    5
#define HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT_CM    6
#define HINT_M_OPTIMIZATIONS_ALL                  7


typedef int PartitionId;
typedef int RecordId;
typedef int Timestamp;


struct RunSettings
{
	string       method;
	const char   *dataFile;
	const char   *queryFile;
	bool         sorted;
	bool         verbose;
    unsigned int typeQuery;
	unsigned int numPartitions;
	unsigned int numLevels;
	unsigned int numBits;
	unsigned int maxBits;
	unsigned int sizeBucket;
	bool         topDown;
	bool         isAutoTuned;
	unsigned int numRuns;
	size_t       numComparisons;
    unsigned int typeOptimizations;
	
	void init()
	{
        sorted            = false;
		verbose	          = false;
		sizeBucket        = 10;   // TODO
		topDown           = false;
		isAutoTuned       = false;
		numLevels         = 0;
		numRuns           = 1;
		numComparisons    = 0;
        typeOptimizations = 0;
	};
};


struct StabbingQuery
{
	size_t id;
	Timestamp point;
    
    StabbingQuery()
    {
        
    };
    StabbingQuery(size_t i, Timestamp p)
    {
        id = i;
        point = p;
    };
};

struct RangeQuery
{
	size_t id;
	Timestamp start, end;

    RangeQuery()
    {
        
    };
    RangeQuery(size_t i, Timestamp s, Timestamp e)
    {
        id = i;
        start = s;
        end = e;
    };
};


class Timer
{
private:
	using Clock = std::chrono::high_resolution_clock;
	Clock::time_point start_time, stop_time;
	
public:
	Timer()
	{
		start();
	}
	
	void start()
	{
		start_time = Clock::now();
	}
	
	
	double getElapsedTimeInSeconds()
	{
		return std::chrono::duration<double>(stop_time - start_time).count();
	}
	
	
	double stop()
	{
		stop_time = Clock::now();
		return getElapsedTimeInSeconds();
	}
};


string toUpperCase(char *buf);
bool checkQuery(string strQuery, RunSettings &settings);
bool checkOptimizations(string strOptimizations, RunSettings &settings);
void process_mem_usage(double& vm_usage, double& resident_set);

#endif // _GLOBAL_DEF_H_
