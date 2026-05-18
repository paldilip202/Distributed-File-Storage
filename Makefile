CXX      = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -pthread

all: master chunkserver client

master:
	$(CXX) $(CXXFLAGS) src/master.cpp src/metadata.cpp -o master

chunkserver:
	$(CXX) $(CXXFLAGS) src/chunkserver.cpp src/chunker.cpp -o chunkserver

client:
	$(CXX) $(CXXFLAGS) src/client.cpp src/chunker.cpp src/metadata.cpp -o client

clean:
	rm -f master chunkserver client
	rm -rf chunks_* master_meta temp_chunks restored_*