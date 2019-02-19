STORE_UTILS_OBJECTS=Store/Utils/RateLimiter.o Store/Utils/FunctionProfiler.o Store/Utils/CarbonConsistentHashRing.o Store/Utils/Errors.o
STORE_QUERY_OBJECTS=Store/Query/Parser.o Store/Query/Functions.o
STORE_WHISPER_OBJECTS=Store/Formats/Whisper.o
STORE_OBJECTS=$(STORE_UTILS_OBJECTS) $(STORE_WHISPER_OBJECTS) $(STORE_QUERY_OBJECTS) Store/StoreTask.o Store/Store.o Store/QueryStore.o Store/DiskStore.o Store/CachedDiskStore.o Store/WriteBufferStore.o Store/RemoteStore.o Store/MultiStore.o Store/ConsistentHashMultiStore.o Store/EmptyStore.o Store/ReadOnlyStore.o
RENDERER_OBJECTS=Renderer/Renderer.o Renderer/ImageRenderer.o Renderer/JSONRenderer.o Renderer/GraphiteRenderer.o Renderer/PickleRenderer.o Renderer/HTMLRenderer.o
THRIFT_OBJECTS=gen-cpp/cyclone_if_constants.o gen-cpp/cyclone_if_types.o gen-cpp/Cyclone.o
SERVER_OBJECTS=Server/Server.o Server/HTTPServer.o Server/CycloneHTTPServer.o Server/ThriftServer.o Server/StreamServer.o Server/DatagramServer.o

OBJECTS=$(STORE_OBJECTS) $(RENDERER_OBJECTS) $(THRIFT_OBJECTS) $(SERVER_OBJECTS) Main.o
THRIFT=/usr/local/bin/thrift
CXX=g++
CXXFLAGS=-I/opt/local/include -I/usr/local/include -std=c++14 -g -DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H -Wall
LDFLAGS=-L/opt/local/lib -L/usr/local/lib -std=c++14 -levent -lthrift -lthriftnb -lphosg -lpthread
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

test: Store/Utils/RateLimiterTest Store/Formats/WhisperTest Store/StoreTest cyclone_client/cyclone_if
	./Store/Utils/RateLimiterTest
	./Store/Formats/WhisperTest
	./Store/StoreTest

Store/Utils/RateLimiterTest: Store/Utils/RateLimiterTest.o Store/Utils/RateLimiter.o
	$(CXX) -std=c++14 -lstdc++ $^ -o $@ $(LDFLAGS)

Store/Formats/WhisperTest: Store/Formats/WhisperTest.o Store/Formats/Whisper.o $(THRIFT_OBJECTS)
	$(CXX) -std=c++14 -lstdc++ $^ -o $@ $(LDFLAGS)

Store/StoreTest: Store/StoreTest.o $(STORE_OBJECTS) $(THRIFT_OBJECTS)
	$(CXX) -std=c++14 -lstdc++ $^ -o $@ $(LDFLAGS)

clean:
	find . -name \*.o -delete
	find . -name \*Test -delete
	rm -rf *.dSYM gen-cpp gen-py cyclone_if $(EXECUTABLE) gmon.out

.PHONY: clean test
