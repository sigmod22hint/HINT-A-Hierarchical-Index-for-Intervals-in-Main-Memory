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
    cerr << "       ./query_hint_m.exec [OPTION]... [DATA] [QUERIES]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       -? or -h" << endl;
    cerr << "              display this help message and exit" << endl;
    cerr << "       -v" << endl;
    cerr << "              activate verbose mode" << endl;
    cerr << "       -q type" << endl;
    cerr << "              set query type: \"STABBING\" or \"RANGE\"" << endl;
    cerr << "       -b bits" << endl;
    cerr << "              set the number of bits" << endl;
    cerr << "       -t" << endl;
    cerr << "              evaluate query traversing the hierarchy in a top-down fashion; by default bottom-up" << endl;
    cerr << "       -o optimizations" << endl;
    cerr << "              set optimizations to be used: \"subs+sort\" or \"subs+sopt\" or \"subs+sort+sopt\" or \"subs+sort+sopt+ss\" or \"subs+sort+sopt+cm\" or \"all\"; omit option for base HINT^m" << endl;
    cerr << "       -r runs" << endl;
    cerr << "              set the number of runs per query; by default 1" << endl << endl;
    cerr << "EXAMPLES" << endl;
    cerr << "       TODO" << endl << endl;
}


int main(int argc, char **argv)
{
    Timer tim;
    Relation R;
    HierarchicalIndex *idxR;
    size_t totalResult = 0, queryresult = 0, numQueries = 0;
    double totalIndexTime = 0, totalQueryTime = 0, querytime = 0, avgQueryTime = 0;
    Timestamp qstart, qend;
    RunSettings settings;
    char c;
    double vmDQ = 0, rssDQ = 0, vmI = 0, rssI = 0;
    string strQuery = "", strOptimizations = "";

    
    // Parse command line input
    settings.init();
    settings.method = "hint_m";
    while ((c = getopt(argc, argv, "?hvq:b:to:r:")) != -1)
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
                
            case 'r':
                settings.numRuns = atoi(optarg);
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
    
    
    // Load data and queries
    R.load(settings.dataFile);
    settings.maxBits = int(log2(R.gend-R.gstart)+1);
    
    ifstream fQ(settings.queryFile);
    if (!fQ)
    {
        cerr << endl << "Error - cannot open query file \"" << settings.queryFile << "\"" << endl << endl;
        return 1;
    }
    process_mem_usage(vmDQ, rssDQ);

    
    // Build index
    switch (settings.typeOptimizations)
    {
        // Base HINT^m, no optimizations activated
        case HINT_M_OPTIMIZATIONS_NO:
            tim.start();
            idxR = new HINT_M(R, settings.numBits, settings.maxBits);
            totalIndexTime = tim.stop();
            break;

        // HINT^m with subs+sort optimization activated
        case HINT_M_OPTIMIZATIONS_SUBS_SORT:
            tim.start();
            idxR = new HINT_M_SubsSort(R, settings.numBits, settings.maxBits);
            totalIndexTime = tim.stop();
            break;

        // HINT^m with subs+sopt optimization activated
        case HINT_M_OPTIMIZATIONS_SUBS_SOPT:
            tim.start();
            idxR = new HINT_M_SubsSopt(R, settings.numBits, settings.maxBits);
            totalIndexTime = tim.stop();
            break;

        // HINT^m with subs+sort+sopt optimization activated
        case HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT:
            tim.start();
            idxR = new HINT_M_SubsSortSopt(R, settings.numBits, settings.maxBits);
            totalIndexTime = tim.stop();
            break;

        // HINT^m with subs+sort+sopt and skewness & sparsity optimizations activated
        case HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT_SS:
            tim.start();
            idxR = new HINT_M_SubsSortSopt_SS(R, settings.numBits, settings.maxBits);
            totalIndexTime = tim.stop();
            break;

        // HINT^m with subs+sort+sopt and cash misses optimizations activated
        case HINT_M_OPTIMIZATIONS_SUBS_SORT_SOPT_CM:
            tim.start();
            idxR = new HINT_M_SubsSortSopt_CM(R, settings.numBits, settings.maxBits);
            totalIndexTime = tim.stop();
            break;

        // HINT^m with all optimizations activated
        case HINT_M_OPTIMIZATIONS_ALL:
            tim.start();
            idxR = new HINT_M_ALL(R, settings.numBits, settings.maxBits);
            totalIndexTime = tim.stop();
            break;
    }
    process_mem_usage(vmI, rssI);

    
    // Execute queries
    size_t sumQ = 0;
    if (settings.verbose)
        cout << "Query\tType\tMethod\tBits\tStrategy\tOptimizations\tResult\tTime" << endl;
    while (fQ >> qstart >> qend)
    {
        sumQ += qend-qstart;
        numQueries++;
        
        double sumT = 0;
        for (auto r = 0; r < settings.numRuns; r++)
        {
            if (settings.typeQuery == QUERY_STABBING)
            {
                if (settings.topDown)
                {
                    // Not tested
                }
                else
                {
                    // Tested only for HINT^m with all optimizations activated
                    if (settings.typeOptimizations == HINT_M_OPTIMIZATIONS_ALL)
                    {
                        tim.start();
                        queryresult = idxR->executeBottomUp(StabbingQuery(numQueries, qstart+(qend-qstart)/2));
                        querytime = tim.stop();
                    }
                    else
                    {
                        cerr << endl << "Stabbing queries currently supported only for HINT^m with all optimizations activated, use -o all" << endl << endl;
                        return 1;
                    }
                }
            }
            else if (settings.typeQuery == QUERY_RANGE)
            {
                if (settings.topDown)
                {
                    // Tested only for HINT^m with all optimizations activated
                    if (settings.typeOptimizations == HINT_M_OPTIMIZATIONS_NO)
                    {
                        tim.start();
                        queryresult = idxR->executeTopDown(RangeQuery(numQueries, qstart, qend));
                        querytime = tim.stop();
                    }
                    else
                    {
                        cerr << endl << "Top-down strategy currently supported only for HINT^m with no optimizations activated, omit -o option" << endl << endl;
                        return 1;
                    }
                }
                else
                {
                    tim.start();
                    queryresult = idxR->executeBottomUp(RangeQuery(numQueries, qstart, qend));
                    querytime = tim.stop();
                }
            }
            sumT += querytime;
            totalQueryTime += querytime;
            
            if (settings.verbose)
                cout << "[" << qstart << "," << qend << "]\t" << strQuery << "\t" << settings.method << "\t" << settings.numBits << "\t" << ((settings.topDown)? "top-down": "bottop-up") << "\t" << strOptimizations << "\t" << queryresult << "\t" << querytime << endl;
        }
        totalResult += queryresult;
        avgQueryTime += sumT/settings.numRuns;
    }
    fQ.close();
    
    
    // Report
    idxR->getStats();
    cout << endl;
    cout << "HINT^m" << endl;
    cout << "======" << endl;
    cout << "Num of intervals          : " << R.size() << endl;
    cout << "Domain size               : " << (R.gend-R.gstart) << endl;
    cout << "Avg interval extent [%]   : "; printf("%f\n", R.avgRecordExtent*100/(R.gend-R.gstart));
    cout << endl;
    cout << "Optimizations             : " << ((settings.typeOptimizations == HINT_M_OPTIMIZATIONS_NO)? "no": strOptimizations) << endl;
    cout << "Num of bits               : " << settings.numBits << endl;
    cout << "Num of partitions         : " << idxR->numPartitions << endl;
    cout << "Num of Originals (In)     : " << idxR->numOriginalsIn << endl;
    cout << "Num of Originals (Out)    : " << idxR->numOriginalsOut << endl;
    cout << "Num of replicas (In)      : " << idxR->numReplicasIn << endl;
    cout << "Num of replicas (Out)     : " << idxR->numReplicasOut << endl;
    cout << "Num of empty partitions   : " << idxR->numEmptyPartitions << endl;
    printf( "Avg partition size        : %f\n", idxR->avgPartitionSize);
    printf( "Read VM [Bytes]           : %ld\n", (size_t)(vmI-vmDQ)*1024);
    printf( "Read RSS [Bytes]          : %ld\n", (size_t)(rssI-rssDQ)*1024);
    printf( "Indexing time [secs]      : %f\n\n", totalIndexTime);
    cout << "Query type                : " << strQuery << endl;
    cout << "Strategy                  : " << ((settings.topDown) ? "top-down": "bottom-up") << endl;
    cout << "Num of runs per query     : " << settings.numRuns << endl;
    cout << "Num of queries            : " << numQueries << endl;
    if (settings.typeQuery == QUERY_RANGE)
    {
        cout << "Avg query extent [%]      : "; printf("%f\n", (((float)sumQ/numQueries)*100)/(R.gend-R.gstart));
    }
    cout << "Total result [XOR]        : " << totalResult << endl;
    printf( "Total querying time [secs]: %f\n", totalQueryTime/settings.numRuns);
    printf( "Avg querying time [secs]  : %f\n\n", avgQueryTime/numQueries);
    printf( "Throughput [queries/sec]  : %f\n\n", numQueries/(totalQueryTime/settings.numRuns));

    delete idxR;
    
    
    return 0;
}
