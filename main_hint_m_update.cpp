#include "getopt.h"
#include "def_global.h"
#include "./containers/relation.h"
#include "./indices/hint_m.h"



void usage()
{
    cerr << endl;
    cerr << "PROJECT" << endl;
    cerr << "       HINT: A Hierarchical Index for Intervals in Main Memory" << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./update_hint_m.exec [OPTION]... [DATA] [UPDATES]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       -? or -h" << endl;
    cerr << "              display this help message and exit" << endl;
    cerr << "       -q type" << endl;
    cerr << "              set query type: \"STABBING\" or \"RANGE\"" << endl;
    cerr << "       -v" << endl;
    cerr << "              activate verbose mode" << endl;
    cerr << "       -b bits" << endl;
    cerr << "              set the number of bits" << endl;
    cerr << "       -t" << endl;
    cerr << "              evaluate query traversing the hierarchy in a top-down fashion; by default bottom-up" << endl;
    cerr << "       -o optimizations" << endl;
    cerr << "              set optimizations to be used: \"subs+sort\" or \"subs+sopt\" or \"subs+sort+sopt\" or \"subs+sort+sopt+ss\" or \"subs+sort+sopt+cm\" or \"all\"; omit option for base HINT^m" << endl;
    cerr << "EXAMPLES" << endl;
    cerr << "       TODO" << endl << endl;
}


int main(int argc, char **argv)
{
    Timer tim;
    Relation R, U, M;
    HierarchicalIndex *idxR, *delta_idxR;
    size_t totalResult = 0, queryresult = 0, numQueries = 0;
    size_t numInsertions = 0, numDeletions = 0;
    double totalInsertTime = 0, inserttime = 0, totalDeleteTime = 0, deletetime = 0, totalQueryTime = 0, querytime = 0;
    Timestamp gstart = std::numeric_limits<Timestamp>::max(), gend = std::numeric_limits<Timestamp>::min();
    Timestamp ustart, uend;
    RecordId uId;
    RunSettings settings;
    char c, operation;
    string strQuery = "", strOptimizations = "";

    
    // Parse command line input
    settings.init();
    settings.method = "hint_m";
    while ((c = getopt(argc, argv, "?hvq:b:to:")) != -1)
    {
        switch (c)
        {
            case '?':
            case 'h':
                usage();
                return 0;
                
            case 'v':
                settings.verbose = true;
                break;

            case 'q':
                strQuery = toUpperCase((char*)optarg);
                break;
                
            case 'b':
                settings.numBits = atoi(optarg);
                break;
                
            case 't':
                settings.topDown = true;
                break;
                
            case 'o':
                strOptimizations = toUpperCase((char*)optarg);
                break;

            default:
                cerr << endl << "Error - unknown option '" << c << "'" << endl << endl;
                usage();
                return 1;
        }
    }
    
    
    // Sanity check
    if (argc-optind != 2)
    {
        usage();
        return 1;
    }
    if (!checkQuery(strQuery, settings))
    {
        if (strQuery == "")
            cerr << endl << "Error - query type not defined" << endl << endl;
        else
            cerr << endl << "Error - unknown query type \"" << strQuery << "\"" << endl << endl;
        usage();
        return 1;
    }
    if (!checkOptimizations(strOptimizations, settings))
    {
        cerr << endl << "Error - unknown optimizations type \"" << strOptimizations << "\"" << endl << endl;
        usage();
        return 1;
    }
    if (settings.numBits <= 0)
    {
        cerr << endl << "Error - invalid number of bits \"" << settings.numBits << "\"" << endl << endl;
        return 1;
    }
    settings.dataFile = argv[optind];
    settings.queryFile = argv[optind+1];
    
    
    // Load data and operations
    R.load(settings.dataFile);
    settings.maxBits = int(log2(R.gend-R.gstart)+1);
    
    ifstream fU(settings.queryFile);
    if (!fU)
    {
        cerr << endl << "Error - cannot open updates file \"" << settings.queryFile << "\"" << endl << endl;
        return 1;
    }

    while (fU >> operation >> ustart >> uend >> uId)
        if(operation == 'U')
            U.emplace_back(0, ustart, uend);
    fU.close();


    // Build index
    switch (settings.typeOptimizations)
    {
        // Base HINT^m, no optimizations activated
        case HINT_M_OPTIMIZATIONS_NO:
        // HINT^m with subs+sort optimization activated
        case HINT_M_OPTIMIZATIONS_SUBS_SORT:
        // HINT^m with subs+sort+sopt optimization activated
        case HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT:
        // HINT^m with subs+sort+sopt and skewness & sparsity optimizations activated
        case HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT_SS:
        // HINT^m with subs+sort+sopt and cash misses optimizations activated
        case HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT_CM:
            cerr << "Updates currently supported only on HINT^m with subs+sopt or all optimizations" << endl;
            return 1;
//            break;
            
        // HINT^m with subs+sopt optimization activated
        case HINT_M_OPTIMIZATIONS_SUBS_SOPT:
            idxR = new HINT_M_SubsSopt(R, U, settings.numBits, settings.maxBits);
            break;
            
        // HINT^m with all optimizations activated
        case HINT_M_OPTIMIZATIONS_ALL:
            idxR = new HINT_M_ALL(R, settings.numBits, settings.maxBits);
            delta_idxR = new HINT_M_SubsSopt(M, U, settings.numBits, settings.maxBits);
            break;
    }


    
    // Execute updates
    fU.open(settings.queryFile);
    size_t numRecords = R.size();
    size_t sumU = 0;
    if (settings.verbose)
        cout << "Interval\tMethod\tBits\tOptimization\tTime" << endl;
    while (fU >> operation >> ustart >> uend >> uId)
    {
        switch (operation)
        {
            case 'I':
                sumU += uend-ustart;
                numInsertions++;

                if (settings.typeOptimizations == HINT_M_OPTIMIZATIONS_SUBS_SOPT)
                {
                    tim.start();
                    idxR->insert(Record(numRecords, ustart, uend));
                    inserttime = tim.stop();
                }
                else if (settings.typeOptimizations == HINT_M_OPTIMIZATIONS_ALL)
                {
                    tim.start();
                    delta_idxR->insert(Record(numRecords, ustart, uend));
                    inserttime = tim.stop();
                }
                numRecords++;

                if (settings.verbose)
                    cout << "[" << ustart << "," << uend << "]\t" << "INSERTION" << "\t" << settings.method << "\t" << settings.numBits << "\t" << "-" << "\t" << inserttime << endl;

                totalInsertTime += inserttime;
                break;

            case 'Q':
                numQueries++;
                
                if (settings.typeQuery == QUERY_STABBING)
                {
                    cerr << "Updates currently not supported for stabbing queries with HINT^m" << endl;
                    return 1;
                }
                else if (settings.typeQuery == QUERY_RANGE)
                {
                    if (settings.topDown)
                    {
                        cerr << "Updates currently not supported for range queris on HINT^m with the top-down strategy" << endl;
                        return 1;
                    }
                    else
                    {
                        if (settings.typeOptimizations == HINT_M_OPTIMIZATIONS_SUBS_SOPT)
                        {
                            tim.start();
                            queryresult = idxR->executeBottomUp(RangeQuery(numQueries, ustart, uend));
                            querytime = tim.stop();
                        }
                        else if (settings.typeOptimizations == HINT_M_OPTIMIZATIONS_ALL)
                        {
                            tim.start();
                            queryresult  = idxR->executeBottomUp(RangeQuery(numQueries, ustart, uend));
                            queryresult ^= delta_idxR->executeBottomUp(RangeQuery(numQueries, ustart, uend));
                            querytime = tim.stop();
                        }
                    }
                }
                
                if (settings.verbose)
                    cout << "[" << ustart << "," << uend << "]\t" << strQuery << "\t" << settings.method << "\t" << settings.numBits << "\t" << ((settings.topDown)? "top-down": "bottop-up") << "\t" << strOptimizations << "\t" << queryresult << "\t" << querytime << endl;

                
                totalQueryTime += querytime;
                totalResult += queryresult;
                break;

            case 'D':
                numDeletions++;

                if (settings.typeOptimizations == HINT_M_OPTIMIZATIONS_SUBS_SOPT)
                {
                    tim.start();
                    idxR->remove(Record(uId, ustart, uend));
                    deletetime = tim.stop();
                }
                else if (settings.typeOptimizations == HINT_M_OPTIMIZATIONS_ALL)
                {
                    tim.start();
                    idxR->remove(Record(uId, ustart, uend));
                    delta_idxR->remove(Record(uId, ustart, uend));
                    deletetime = tim.stop();
                }

                if (settings.verbose)
                    cout << "[" << ustart << "," << uend << "]\t" << "DELETION" << "\t" << settings.method << "\t" << settings.numPartitions << "\t" << deletetime << endl;
                    
                totalDeleteTime += deletetime;
                break;
        }

    }
    fU.close();
    
    
    // Report
    cout << endl;
    cout << "HINT^m" << endl;
    cout << "======" << endl;
    cout << "Num of intervals           : " << R.size() << endl;
    cout << "Domain size                : " << (R.gend-R.gstart) << endl;
    cout << "Avg interval extent [%]    : "; printf("%f\n", R.avgRecordExtent*100/(R.gend-R.gstart));
    cout << endl;
    cout << "Optimizations              : " << ((settings.typeOptimizations == HINT_M_OPTIMIZATIONS_NO)? "no": strOptimizations) << endl;
    cout << "Num of bits                : " << settings.numBits << endl;
    cout << "Query type                 : " << strQuery << endl;
    cout << "Strategy                   : " << ((settings.topDown) ? "top-down": "bottom-up") << endl;
    cout << "Num of insertions          : " << numInsertions << endl;
    cout << "Num of deletions           : " << numDeletions << endl;
    cout << "Num of queries             : " << numQueries << endl;
    cout << "Total result [XOR]         : " << totalResult << endl;
    printf( "Total querying time [secs] : %f\n", totalQueryTime);
    printf( "Total inserting time [secs]: %f\n", totalInsertTime);
    printf( "Total deleting time [secs] : %f\n", totalDeleteTime);


    delete idxR;
    if (settings.typeOptimizations == HINT_M_OPTIMIZATIONS_ALL)
        delete delta_idxR;
    
    
    return 0;
}
