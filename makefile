CC      = g++
CFLAGS  = -O3 -mavx -std=c++14 -w -march=native
LDFLAGS =


SOURCES = utils.cpp containers/relation.cpp containers/eventlist.cpp indices/periodindex.cpp indices/timelineindex.cpp indices/1dgrid.cpp indices/hierarchicalindex.cpp indices/hint.cpp indices/hint_m.cpp
OBJECTS = $(SOURCES:.cpp=.o)

all: query update

query: lscan intervaltree periodindex timeline 1dgrid hint hint_m
update: intervaltree_update periodindex_update 1dgrid_update hint_update hint_m_update


lscan: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o main_lscan.cpp -o query_lscan.exec $(LDADD)

intervaltree: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o main_intervaltree.cpp -o query_intervaltree.exec $(LDADD)

periodindex: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o indices/periodindex.o main_periodindex.cpp -o query_periodindex.exec $(LDADD)

timeline: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o containers/eventlist.o indices/timelineindex.o main_timelineindex.cpp -o query_timelineindex.exec $(LDADD)

1dgrid: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o indices/1dgrid.o main_1dgrid.cpp -o query_1dgrid.exec $(LDADD)

hint: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o indices/hierarchicalindex.o indices/hint.o main_hint.cpp -o query_hint.exec $(LDADD)

hint_m: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o indices/hierarchicalindex.o indices/hint_m.o main_hint_m.cpp -o query_hint_m.exec $(LDADD)


intervaltree_update: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o main_intervaltree_update.cpp -o update_intervaltree.exec $(LDADD)

periodindex_update: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o indices/periodindex.o main_periodindex_update.cpp -o update_periodindex.exec $(LDADD)

1dgrid_update: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o indices/1dgrid.o main_1dgrid_update.cpp -o update_1dgrid.exec $(LDADD)

hint_update: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o indices/hierarchicalindex.o indices/hint.o main_hint_update.cpp -o update_hint.exec $(LDADD)

hint_m_update: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) utils.o containers/relation.o indices/hierarchicalindex.o indices/hint_m.o main_hint_m_update.cpp -o update_hint_m.exec $(LDADD)


.cpp.o:
	$(CC) $(CFLAGS) -c $< -o $@

.cc.o:
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf utils.o
	rm -rf containers/*.o
	rm -rf indices/*.o
	rm -rf query_lscan.exec
	rm -rf query_intervaltree.exec
	rm -rf query_periodindex.exec
	rm -rf query_timelineindex.exec
	rm -rf query_1dgrid.exec
	rm -rf query_hint.exec
	rm -rf query_hint_m.exec
	rm -rf update_intervaltree.exec
	rm -rf update_periodindex.exec
	rm -rf update_1dgrid.exec
	rm -rf update_hint.exec
	rm -rf update_hint_m.exec