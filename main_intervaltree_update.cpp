#include "getopt.h"
#include "def_global.h"
#include "./indices/intervaltree.h"



void usage()
{
    cerr << endl;
    cerr << "PROJECT" << endl;
    cerr << "       HINT: A Hierarchical Index for Intervals in Main Memory" << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./update_intervaltree.exec [OPTION]... [DATA] [UPDATES]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       -? or -h" << endl;
    cerr << "              display this help message and exit" << endl;
    cerr << "       -v" << endl;
    cerr << "              activate verbose mode" << endl;
    cerr << "       -l levels" << endl;
    cerr << "              set the height of the tree; by default 16" << endl;
    cerr << "EXAMPLES" << endl;
    cerr << "       TODO" << endl << endl;
}


int main(int argc, char **argv)
{
    Timer tim;
    Timestamp s, e;
    size_t numRecords = 0;
    IntervalTree<int, RecordId>::interval_vector R;
    IntervalTree<int, RecordId> *idxR;
    size_t numUpdates = 0;
    double totalUpdateTime = 0, updatetime = 0;
    Timestamp gstart = std::numeric_limits<Timestamp>::max(), gend = std::numeric_limits<Timestamp>::min();
    Timestamp ustart, uend;
    RunSettings settings;
    char c;

    
    // Parse command line input
    settings.init();
    settings.method = "interval-tree";
    settings.numLevels = 16;
    while ((c = getopt(argc, argv, "?hvl:r:")) != -1)
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

            case 'l':
                settings.numLevels = atoi(optarg);
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
    if (settings.numLevels <= 0)
    {
        cerr << endl << "Error - invalid number of levels \"" << settings.numLevels << "\"" << endl << endl;
        return 1;
    }
    settings.dataFile = argv[optind];
    settings.queryFile = argv[optind+1];
    
    
    // Load data and queries
    ifstream inp(settings.dataFile);
    if (!inp)
    {
        cerr << "Error - cannot open data file \"" << settings.dataFile << "\"" << endl;
        exit(1);
    }
    size_t sumR = 0;
    while (inp >> s >> e)
    {
        gstart = std::min(gstart, s);
        gend   = std::max(gend  , e);
        sumR += (e-s);
        R.push_back(Interval<int, RecordId>(s, e, numRecords));
        numRecords++;
    }
    inp.close();
    
    ifstream fU(settings.queryFile);
    if (!fU)
    {
        cerr << endl << "error - cannot open updates file \"" << settings.queryFile << "\"" << endl << endl;
        return 1;
    }
    
    
    // Build index
    idxR = new IntervalTree<int, RecordId>(std::move(R), settings.numLevels);

    
    // Execute updates
    size_t sumU = 0;
    if (settings.verbose)
        cout << "Interval\tMethod\tLevels\tTime" << endl;
    while (fU >> ustart >> uend)
    {
        sumU += uend-ustart;
        numUpdates++;
        
        tim.start();
        idxR->update(Interval<int, RecordId>(ustart, uend, numRecords));
        numRecords++;
        updatetime = tim.stop();

        if (settings.verbose)
                cout << "[" << ustart << "," << uend << "]\t" << "UPDATE" << "\t" << settings.method << "\t" << settings.numLevels << "\t" << updatetime << endl;

        totalUpdateTime += updatetime;
    }
    fU.close();
    
    
    // Report
    cout << endl;
    cout << "Interval tree" << endl;
    cout << "=============" << endl;
    cout << "Num of intervals          : " << numRecords << endl;
    cout << "Domain size               : " << (gend-gstart) << endl;
    cout << "Avg interval extent [%]   : "; printf("%f\n", (((float)sumR/numRecords)*100)/(gend-gstart));
    cout << endl;
    cout << "Height                    : " << settings.numLevels << endl;
    cout << "Num of updates            : " << numUpdates << endl;
    cout << "Avg update extent [%]     : "; printf("%f\n", (((float)sumU/numUpdates)*100)/(gend-gstart));
    printf( "Total updating time [secs]: %f\n", totalUpdateTime);
    cout << endl;
    
    delete idxR;
    
    
    return 0;
}
