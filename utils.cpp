#include "def_global.h"



string toUpperCase(char *buf)
{
    auto i = 0;
    while (buf[i])
    {
        buf[i] = toupper(buf[i]);
        i++;
    }
    
    return string(buf);
}


bool checkQuery(string strQuery, RunSettings &settings)
{
    if (strQuery == "STABBING")
    {
        settings.typeQuery = QUERY_STABBING;
        return true;
    }
    else if (strQuery == "RANGE")
    {
        settings.typeQuery = QUERY_RANGE;
        return true;
    }

    return false;
}


bool checkOptimizations(string strOptimizations, RunSettings &settings)
{
    if (strOptimizations == "")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_NO;
        return true;
    }
    else if (strOptimizations == "SUBS+SORT")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_SUBS_SORT;
        return true;
    }
    else if (strOptimizations == "SUBS+SOPT")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_SUBS_SOPT;
        return true;
    }
    else if (strOptimizations == "SUBS+SORT+SOPT")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT;
        return true;
    }
    else if (strOptimizations == "SUBS+SORT+SOPT+SS")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT_SS;
        return true;
    }
    else if (strOptimizations == "SUBS+SORT+SOPT+CM")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT_CM;
        return true;
    }
    else if (strOptimizations == "ALL")
    {
        settings.typeOptimizations = HINT_M_OPTIMIZATIONS_ALL;
        return true;
    }
    
    return false;
}


void process_mem_usage(double& vm_usage, double& resident_set)
{
    vm_usage     = 0.0;
    resident_set = 0.0;
    
    // the two fields we want
    unsigned long vsize;
    long rss;
    {
        std::string ignore;
        std::ifstream ifs("/proc/self/stat", std::ios_base::in);
        ifs >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
        >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
        >> ignore >> ignore >> vsize >> rss;
    }
    
    long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
    vm_usage = vsize / 1024.0;
    resident_set = rss * page_size_kb;
}
