#include "getopt.h"
#include "def_global.h"
#include "./containers/relation.h"



void usage()
{
    cerr << endl;
    cerr << "PROJECT" << endl;
    cerr << "       HINT: A Hierarchical Index for Intervals in Main Memory" << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./query_lscan.exec [OPTION]... [DATA] [QUERIES]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       -?" << endl;
    cerr << "              display this help message and exit" << endl;
    cerr << "       -v" << endl;
    cerr << "              activate verbose mode" << endl;
    cerr << "       -q type" << endl;
    cerr << "              set query type: \"STABBING\" or \"RANGE\"" << endl;
    cerr << "       -r runs" << endl;
    cerr << "              set the number of runs per query; by default 1" << endl << endl;
    cerr << "EXAMPLES" << endl;
    cerr << "       TODO" << endl << endl;
}


int main(int argc, char **argv)
{
    Timer tim;
    Relation R;
    size_t totalResult = 0, queryresult = 0, numQueries = 0;
    double totalSortingTime = 0, totalQueryTime = 0, querytime = 0, avgQueryTime = 0;
    Timestamp qstart, qend;
    RunSettings settings;
    char c;
    double vmDQ = 0, rssDQ = 0, vmI = 0, rssI = 0;
    string strQuery = "";

    
    // Parse command line input
    settings.init();
    settings.method = "lscan";
    while ((c = getopt(argc, argv, "?hvq:r:")) != -1)
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
    settings.dataFile = argv[optind];
    settings.queryFile = argv[optind+1];
    
    
    // Load data and queries
    R.load(settings.dataFile);
    
    ifstream fQ(settings.queryFile);
    if (!fQ)
    {
        cerr << endl << "Error - cannot open query file \"" << settings.queryFile << "\"" << endl << endl;
        return 1;
    }
    process_mem_usage(vmDQ, rssDQ);

    
    // Execute queries
    size_t sumQ = 0;
    if (settings.verbose)
        cout << "Query\tType\tMethod\tResult\tTime" << endl;
    while (fQ >> qstart >> qend)
    {
        sumQ += qend-qstart;
        numQueries++;
        
        double sumT = 0;
        for (auto r = 0; r < settings.numRuns; r++)
        {
            if (settings.typeQuery == QUERY_STABBING)
            {
                tim.start();
                queryresult = R.execute(StabbingQuery(numQueries, qstart+(qend-qstart)/2));
                querytime = tim.stop();
            }
            else if (settings.typeQuery == QUERY_RANGE)
            {
                tim.start();
                queryresult = R.execute(RangeQuery(numQueries, qstart, qend));
                querytime = tim.stop();
            }
            sumT += querytime;
            totalQueryTime += querytime;
            
            if (settings.verbose)
                cout << "[" << qstart << "," << qend << "]\t" << strQuery << "\t" << settings.method << "\t" << queryresult << "\t" << querytime << endl;
        }
        totalResult += queryresult;
        avgQueryTime += sumT/settings.numRuns;
    }
    fQ.close();
    
    
    // Report
    cout << endl;
    cout << "Linear scan" << endl;
    cout << "===========" << endl;
    cout << "Num of intervals          : " << R.size() << endl;
    cout << "Domain size               : " << (R.gend-R.gstart) << endl;
    cout << "Avg interval extent [%]   : "; printf("%f\n", R.avgRecordExtent*100/(R.gend-R.gstart));
    cout << endl;
    printf( "Read VM [Bytes]           : %ld\n", (size_t)(vmI-vmDQ)*1024);
    printf( "Read RSS [Bytes]          : %ld\n", (size_t)(rssI-rssDQ)*1024);
    cout << "Query type                : " << strQuery << endl;
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
    
    
    return 0;
}
