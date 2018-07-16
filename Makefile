STORE_OBJECTS=Store/Whisper.o Store/Store.o Store/QueryParser.o Store/QueryFunctions.o Store/QueryStore.o Store/DiskStore.o Store/CachedDiskStore.o Store/WriteBufferStore.o Store/RemoteStore.o Store/MultiStore.o Store/CarbonConsistentHashRing.o Store/ConsistentHashMultiStore.o Store/EmptyStore.o Store/ReadOnlyStore.o
RENDERER_OBJECTS=Renderer/Renderer.o Renderer/ImageRenderer.o Renderer/JSONRenderer.o Renderer/GraphiteRenderer.o Renderer/PickleRenderer.o Renderer/HTMLRenderer.o
THRIFT_OBJECTS=gen-cpp/cyclone_if_constants.o gen-cpp/cyclone_if_types.o gen-cpp/Cyclone.o
SERVER_OBJECTS=Server/CycloneHTTPServer.o Server/HTTPServer.o Server/ThriftServer.o Server/StreamServer.o Server/DatagramServer.o

OBJECTS=$(STORE_OBJECTS) $(RENDERER_OBJECTS) $(THRIFT_OBJECTS) $(SERVER_OBJECTS) Main.o
THRIFT=/usr/local/bin/thrift
CXX=g++
CXXFLAGS=-I/opt/local/include -I/usr/local/include -std=c++14 -g -DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H -Wall
LDFLAGS=-L/opt/local/lib -L/usr/local/lib -std=c++14 -levent -lthrift -lthriftnb -lphosg -lpthread -lz
EXECUTABLE=cyclone

all: $(EXECUTABLE) test

gen-cpp: cyclone_if.thrift
	$(THRIFT) --gen cpp cyclone_if.thrift

cyclone_client/cyclone_if: cyclone_if.thrift
	$(THRIFT) --gen py cyclone_if.thrift
	rm -rf cyclone_client/cyclone_if
	mv gen-py/cyclone_if ./cyclone_client/
	rm -rf gen-py

$(EXECUTABLE): gen-cpp $(OBJECTS)
	$(CXX) $(OBJECTS) $(LDFLAGS) -o $(EXECUTABLE)

test: Store/WhisperTest Store/StoreTest cyclone_client/cyclone_if
	./Store/WhisperTest
	./Store/StoreTest

Store/WhisperTest: Store/WhisperTest.o Store/Whisper.o $(THRIFT_OBJECTS)
	$(CXX) -std=c++14 -lstdc++ $^ -o $@ $(LDFLAGS)

Store/StoreTest: Store/StoreTest.o $(STORE_OBJECTS) $(THRIFT_OBJECTS)
	$(CXX) -std=c++14 -lstdc++ $^ -o $@ $(LDFLAGS)

clean:
	rm -rf *.dSYM gen-cpp gen-py cyclone_if *.o Store/*.o Store/*Test Renderer/*.o Server/*.o $(EXECUTABLE) Store/whisper_util* Renderer/render_util* gmon.out

.PHONY: clean test
