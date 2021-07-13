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
    size_t numUpdates = 0;
    double totalUpdateTime = 0, updatetime = 0;
    Timestamp gstart = std::numeric_limits<Timestamp>::max(), gend = std::numeric_limits<Timestamp>::min();
    Timestamp ustart, uend;
    RunSettings settings;
    char c;

    
    // Parse command line input
    settings.init();
    settings.method = "1D-grid";
    while ((c = getopt(argc, argv, "?hvp:")) != -1)
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
    while (fU >> ustart >> uend)
        U.emplace_back(0, ustart, uend);
    fU.close();
    idxR = new OneDimensionalGrid(R, U, settings.numPartitions);


    // Execute updates
    fU.open(settings.queryFile);
    size_t numRecords = R.size();
    size_t sumU = 0;
    if (settings.verbose)
        cout << "Interval\tMethod\tPartitions\tTime" << endl;
    while (fU >> ustart >> uend)
    {
        sumU += uend-ustart;
        numUpdates++;

        tim.start();
        idxR->updatePartitionsUpdates(Record(numRecords, ustart, uend));
        numRecords++;
        updatetime = tim.stop();

        if (settings.verbose)
            cout << "[" << ustart << "," << uend << "]\t" << "UPDATE" << "\t" << settings.method << "\t" << settings.numPartitions << "\t" << updatetime << endl;
        
        totalUpdateTime += updatetime;
    }
    fU.close();
    
    
    // Report
    idxR->getStats();
    cout << endl;
    cout << "1D grid" << endl;
    cout << "=======" << endl;
    cout << "Num of intervals          : " << R.size() << endl;
    cout << "Domain size               : " << (R.gend-R.gstart) << endl;
    cout << "Avg interval extent [%]   : "; printf("%f\n", R.avgRecordExtent*100/(R.gend-R.gstart));
    cout << endl;
    cout << "Num of partitions         : " << settings.numPartitions << endl;
    cout << "Num of updates            : " << numUpdates << endl;
    cout << "Avg update extent [%]     : "; printf("%f\n", (((float)sumU/numUpdates)*100)/(gend-gstart));
    printf( "Total updating time [secs]: %f\n", totalUpdateTime);
    cout << endl;

    delete idxR;
    
    
    return 0;
}
