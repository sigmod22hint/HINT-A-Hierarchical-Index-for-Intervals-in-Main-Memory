# HINT: A Hierarchical Index for Intervals in Main Memory

Source code for the 'HINT: A Hierarchical Index for Intervals in Main Memory' submission (paper ID 177) to ACM SIGMOD 2022

## Dependencies
- g++/gcc
- Boost Library 

**Note**: the Boost library is required to compile the Timeline index implementation.

## Samples

Directory  ```samples``` includes the BOOKS dataset used in the experiments, the default query file and the two files needed for the testing the update functionality 
- BOOKS.txt
- BOOKS_c0.1%_q10000.txt
- BOOKS_first90.txt
- BOOKS_updates.mix


## Compile
Compile using ```make all``` or ```make <option>``` where <option> can be one of the following:
   - intervaltree 
   - periodindex 
   - timeline
   - 1dgrid 
   - hint
   - hint_m 
   - query 
   - update

## Shared parameters among all indexing methods
| Parameter | Description | Comment |
| ------ | ------ | ------ |
| -? or -h | display help message | |
| -v | activate verbose mode to print out for every query or update | |
| -q | set query type "stabbing" or "range" | Only for querying |
| -r | set the number of runs per query; by default 1 | Only for querying |

## Indexing methods

### Interval tree:

#### Source code files
- main_intervaltree.cpp
- indices/intervaltree.h

#### Execution 
| Extra parameter | Description | Comment |
| ------ | ------ | ------ |
| -l | set the height of the tree; by default 16 | default value used in the experiments |
  
 - ##### Stabbing query    

    ```sh
    $ ./query_intervaltree.exec -q stabbing samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
        
- ##### Range query

    ```sh
    $ ./query_intervaltree.exec -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Update - Mixed Workload 10k Queries/5k Insertions/1k Deletions

    ```sh
    $ ./update_intervaltree.exec -q range samples/BOOKS_first90.txt samples/BOOKS_updates.mix
    ```


### Period index: 

#### Source code files
- main_periodindex.cpp
- indices/periodindex.h
- indices/periodindex.cpp

#### Execution
| Extra parameter | Description | Comment |
| ------ | ------ | ------ |
| -l | set the number of levels | 4 for BOOKS in the experiments |
| -p | set the number of partitions | 100 for BOOKS in the experiments |

- ##### Stabbing query    

    ```sh
    $ ./query_periodindex.exec -l 4 -p 100 -q stabbing samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Range query

    ```sh
    $ ./query_periodindex.exec -l 4 -p 100 -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Update - Mixed Workload 10k Queries/5k Insertions/1k Deletions

    ```sh
    $ ./update_periodindex.exec -l 4 -p 100 -q range samples/BOOKS_first90.txt samples/BOOKS_updates.mix
    ```


### Timeline index: 

#### Source code files
- main_timelineindex.cpp
- containers/endpoint_index.h
- containers/endpoint_index.cpp
- indices/timelineindex.h
- indices/timelineindex.cpp

#### Execution
| Extra parameter | Description | Comment |
| ------ | ------ | ------ |
| -c | set the number of checkpoints | 6000 for BOOKS in the experiments |

- ##### Stabbing query    

    ```sh
    $ ./query_timelineindex.exec -c 6000 -q stabbing samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Range query

    ```sh
    $ ./query_timelineindex.exec -c 6000 -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Update
    Not supported


### 1D-grid: 

#### Source code files
- main_1dgrid.cpp
- indices/1dgrid.h
- indices/1dgrid.cpp

#### Execution
| Extra parameter | Description | Comment |
| ------ | ------ | ------ |
| -p | set the number of partitions | 500 for BOOKS in the experiments |

- ##### Stabbing query    

    ```sh
    $ ./query_1dgrid.exec -p 500 -q stabbing samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Range query

    ```sh
    $ ./query_1dgrid.exec -p 500 -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Update - Mixed Workload 10k Queries/5k Insertions/1k Deletions

    ```sh
    $ ./update_1dgrid.exec -p 500 -q range samples/BOOKS_first90.txt samples/BOOKS_updates.mix
    ```


### HINT: 

#### Source code files
- main_hint.cpp
- indices/hierarchicalindex.h
- indices/hierarchicalindex.cpp
- indices/hint.h
- indices/hint.cpp

#### Execution
| Extra parameter | Description | Comment |
| ------ | ------ | ------ |
| -o | activate the skewness & sparsity optimization |  Only for querying |

- ##### Stabbing query    

    ```sh
    $ ./query_hint.exec -q stabbing samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ```sh
    $ ./query_hint.exec -o -q stabbing samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Range query

    ```sh
    $ ./query_hint.exec -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ```sh
    $ ./query_hint.exec -o -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Update - Mixed Workload 10k Queries/5k Insertions/1k Deletions

    ```sh
    $ ./update_hint.exec -q range samples/BOOKS_first90.txt samples/BOOKS_updates.mix
    ```


### HINT<sup>m</sup>: 

#### Source code files
- main_hint_m.cpp
- indices/hierarchicalindex.h
- indices/hierarchicalindex.cpp
- indices/hint_m.h
- indices/hint_m.cpp

#### Execution
| Extra parameter | Description | Comment |
| ------ | ------ | ------ |
| -b |  set the number of bits | 10 for BOOKS in the experiments |
| -t  |  activate the top-down evaluation approach |  Available only for base HINT<sup>m</sup> and the range query; omit option for bottom-up |
| -o |  set optimizations to be used: "subs+sort" or "subs+sopt" or "subs+sort+sopt" or "subs+sort+sopt+ss" or "subs+sort+sopt+cm" or "all"| Omit option for base HINT<sup>m</sup> |

- ##### Stabbing query    

    ```sh
    $ ./query_hint_m.exec -b 10 -o all -q stabbing samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Range query

    ###### base with top-down
    ```sh
    $ ./query_hint_m.exec -t -b 10 -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### base with bottom-up
    ```sh
    $ ./query_hint_m.exec -b 10 -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### subs+sort (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -b 10 -o subs+sort -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### subs+sopt (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -b 10 -o subs+sopt -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### subs+sort+sopt (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -b 10 -o subs+sort+sopt -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### subs+sort+sopt+ss (only bottom-up, "ss" -> sparsity & skewness optimization)
    ```sh
    $ ./query_hint_m.exec -b 10 -o subs+sort+sopt+ss -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### subs+sort+sopt+cm (only bottom-up, "cm" -> cache misses optimization)
    ```sh
    $ ./query_hint_m.exec -b 10 -o subs+sort+sopt+cm -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```
    ###### all optimizations  (only bottom-up)
    ```sh
    $ ./query_hint_m.exec -b 10 -o all -q range samples/BOOKS.txt samples/BOOKS_c0.1%_n10000.txt
    ```

- ##### Update - Mixed Workload 10k Queries/5k Insertions/1k Deletions (only bottom-up)

    ```sh
    $ ./update_hint_m.exec -b 10 -q range -o subs+sopt samples/BOOKS_first90.txt samples/BOOKS_updates.mix
    ```

    ```sh
    $ ./update_hint_m.exec -b 10 -q range -o all samples/BOOKS_first90.txt samples/BOOKS_updates.mix
    ```

