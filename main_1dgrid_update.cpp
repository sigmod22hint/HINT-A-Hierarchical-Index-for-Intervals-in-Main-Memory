#include "getopt.h"
#include "def_global.h"
#include "./containers/relation.h"
#include "./indices/1dgrid.h"



void usage()
{
    cerr << endl;
    cerr << "PROJECT" << endl;
    cerr << "       HINT: A Hierarchical Index for Intervals in Main Memory" << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./update_1d_grid.exec [OPTION]... [DATA] [UPDATES]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       -? or -h" << endl;
    cerr << "              display this help message and exit" << endl;
    cerr << "       -v" << endl;
    cerr << "              activate verbose mode" << endl;
    cerr << "       -q type" << endl;
    cerr << "              set query type: \"STABBING\" or \"RANGE\"" << endl;
    cerr << "       -p partitions" << endl;
    cerr << "              set the number of partitions to divide the domain; by default 100" << endl;
    cerr << "EXAMPLES" << endl;
    cerr << "       TODO" << endl << endl;
}


int main(int argc, char **argv)
{
    Timer tim;
    Relation R, U;
    OneDimensionalGrid *idxR;
    size_t totalResult = 0, queryresult = 0, numQueries = 0;
    size_t numInsertions = 0, numDeletions = 0;
    double totalInsertTime = 0, inserttime = 0, totalDeleteTime = 0, deletetime = 0, totalQueryTime = 0, querytime = 0;
    Timestamp gstart = std::numeric_limits<Timestamp>::max(), gend = std::numeric_limits<Timestamp>::min();
    Timestamp ustart, uend;
    RecordId uId;
    RunSettings settings;
    char c, operation;
    string strQuery = "";

    
    // Parse command line input
    settings.init();
    settings.method = "1D-grid";
    while ((c = getopt(argc, argv, "?hvq:p:")) != -1)
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
                
            case 'p':
                settings.numPartitions = atoi(optarg);
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
    if (settings.numPartitions <= 0)
    {
        cerr << endl << "Error - invalid number of partitions \"" << settings.numPartitions << "\"" << endl << endl;
        return 1;
    }
    settings.dataFile = argv[optind];
    settings.queryFile = argv[optind+1];
    
    
    // Load data and queries
    R.load(settings.dataFile);
    
    ifstream fU(settings.queryFile);
    if (!fU)
    {
        cerr << endl << "Error - cannot open updates file \"" << settings.queryFile << "\"" << endl << endl;
        return 1;
    }
    

    // Build index
    while (fU >> operation >> ustart >> uend >> uId)
        if (operation == 'I')
            U.emplace_back(0, ustart, uend);
    fU.close();
    idxR = new OneDimensionalGrid(R, U, settings.numPartitions);


    // Execute updates
    fU.open(settings.queryFile);
    size_t numRecords = R.size();
    size_t sumU = 0;
    if (settings.verbose)
        cout << "Interval\tMethod\tPartitions\tTime" << endl;
    while (fU >> operation >> ustart >> uend >> uId)
    {
        sumU += uend-ustart;
        switch (operation)
        {
            case 'I':
                numInsertions++;

                tim.start();
                idxR->insert(Record(numRecords, ustart, uend));
                numRecords++;
                inserttime = tim.stop();

                if (settings.verbose)
                    cout << "[" << ustart << "," << uend << "]\t" << "INSERTION" << "\t" << settings.method << "\t" << settings.numPartitions << "\t" << inserttime << endl;
                
                totalInsertTime += inserttime;
                break;

            case 'Q':
                numQueries++;

                if (settings.typeQuery == QUERY_STABBING)
                {
                    tim.start();
                    queryresult = idxR->execute(StabbingQuery(numQueries, ustart+(uend-ustart)/2));
                    querytime = tim.stop();
                }
                else if (settings.typeQuery == QUERY_RANGE)
                {
                    tim.start();
                    queryresult = idxR->execute(RangeQuery(numQueries, ustart, uend));
                    querytime = tim.stop();
                }
                if (settings.verbose)
                    cout << "[" << ustart << "," << uend << "]\t" << strQuery << "\t" << settings.method << "\t" << settings.numPartitions << "\t" << querytime << endl;


                totalQueryTime += querytime;
                totalResult += queryresult;
                break;
                
            case 'D':
                numDeletions++;

                tim.start();
                idxR->remove(Record(uId, ustart, uend));
                deletetime = tim.stop();

                if (settings.verbose)
                    cout << "[" << ustart << "," << uend << "]\t" << "DELETION" << "\t" << settings.method << "\t" << settings.numPartitions << "\t" << deletetime << endl;
                
                totalDeleteTime += deletetime;
                break;
        }
    }
    fU.close();
    
    
    // Report
    idxR->getStats();
    cout << endl;
    cout << "1D grid" << endl;
    cout << "=======" << endl;
    cout << "Num of intervals           : " << R.size() << endl;
    cout << "Domain size                : " << (R.gend-R.gstart) << endl;
    cout << "Avg interval extent [%]    : "; printf("%f\n", R.avgRecordExtent*100/(R.gend-R.gstart));
    cout << endl;
    cout << "Num of partitions          : " << settings.numPartitions << endl;
    cout << "Num of insertions          : " << numInsertions << endl;
    cout << "Num of deletions           : " << numDeletions << endl;
    cout << "Num of queries             : " << numQueries << endl;
    cout << "Total query result [XOR]   : " << totalResult << endl;
    printf( "Total querying time [secs] : %f\n", totalQueryTime);
    printf( "Total inserting time [secs]: %f\n", totalInsertTime);
    printf( "Total deleting time [secs] : %f\n", totalDeleteTime);
    cout << endl;

    delete idxR;
    
    
    return 0;
}
