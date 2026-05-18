#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>   
#include <filesystem>       
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "metadata.h"

// -------------------------------------------------------
// Global state — shared across all client handler threads
// -------------------------------------------------------
MetadataStore metaStore;
std::vector<std::string> chunkServers;  // list of "host:port" strings
std::mutex csMutex;  // protects chunkServers vector
int roundRobinIndex = 0;  // for distributing chunks across servers evenly

const int MASTER_PORT = 8080;

// -------------------------------------------------------
// Helper: send a complete string over a socket
// -------------------------------------------------------
void sendMsg(int sock, const std::string& msg) {
    send(sock, msg.c_str(), msg.size(), 0);
}

// -------------------------------------------------------
// Helper: read one line (up to '\n') from a socket
// -------------------------------------------------------
std::string recvLine(int sock) {
    std::string result;
    char c;
    while (recv(sock, &c, 1, 0) > 0) {
        if (c == '\n') break;
        result += c;
    }
    return result;
}

// -------------------------------------------------------
// Pick the next chunk server using round-robin
// Round-robin means we cycle through servers evenly:
// chunk0 → server1, chunk1 → server2, chunk2 → server1...
// -------------------------------------------------------
std::string pickChunkServer() {
    std::lock_guard<std::mutex> lock(csMutex);
    if (chunkServers.empty()) return "";
    std::string server = chunkServers[roundRobinIndex % chunkServers.size()];
    roundRobinIndex++;
    return server;
}

// -------------------------------------------------------
// Handle one client connection in its own thread
// -------------------------------------------------------
void handleClient(int clientSock) {
    std::string line = recvLine(clientSock);
    std::cout << "[Master] Received: " << line << "\n";

    std::istringstream iss(line);
    std::string command;
    iss >> command;

    if (command == "REGISTER") {
        // Chunk server announcing itself: "REGISTER localhost 9001"
        std::string host, port;
        iss >> host >> port;
        std::string address = host + ":" + port;

        std::lock_guard<std::mutex> lock(csMutex);
        chunkServers.push_back(address);
        std::cout << "[Master] Chunk server registered: " << address << "\n";
        sendMsg(clientSock, "OK\n");
    }

    else if (command == "STORE") {
        // Client wants to store a file: "STORE movie.mp4 5242880"
        std::string filename;
        size_t filesize;
        iss >> filename >> filesize;

        // Initialize empty metadata entry for this file
        FileMetadata meta;
        meta.filename = filename;
        meta.total_size = filesize;
        meta.total_chunks = 0;
        metaStore.addFile(filename, meta);

        // Tell client which chunk servers are available
        std::lock_guard<std::mutex> lock(csMutex);
        for (auto& cs : chunkServers) {
            sendMsg(clientSock, "CS " + cs + "\n");
        }
        sendMsg(clientSock, "END\n");
    }

    else if (command == "CHUNK_DONE") {
        // Client reports a chunk was stored: "CHUNK_DONE chunk_id host:port index size"
        std::string chunk_id, server;
        int index;
        size_t size;
        iss >> chunk_id >> server >> index >> size;

        ChunkInfo info;
        info.chunk_id = chunk_id;
        info.server_address = server;
        info.index = index;
        info.size = size;

        // Parse filename from chunk_id (everything before "_chunk_")
        std::string filename = chunk_id.substr(0, chunk_id.rfind("_chunk_"));

        metaStore.addChunk(filename, info);

        // Save to disk after every chunk (crash safety)
        metaStore.saveToDisk("master_meta", filename);

        sendMsg(clientSock, "OK\n");
    }

    else if (command == "FETCH") {
        // Client wants to fetch a file: "FETCH movie.mp4"
        std::string filename;
        iss >> filename;

        if (!metaStore.exists(filename)) {
            sendMsg(clientSock, "ERROR File not found\n");
            close(clientSock);
            return;
        }

        FileMetadata meta = metaStore.getFile(filename);

        // Sort chunks by index to ensure correct order
        std::sort(meta.chunks.begin(), meta.chunks.end(),
                  [](const ChunkInfo& a, const ChunkInfo& b) {
                      return a.index < b.index;
                  });

        sendMsg(clientSock, "OK " + std::to_string(meta.total_chunks) + "\n");

        // Send one line per chunk: "chunk_id host:port size index"
        for (auto& c : meta.chunks) {
            sendMsg(clientSock, c.chunk_id + " " + c.server_address +
                    " " + std::to_string(c.size) +
                    " " + std::to_string(c.index) + "\n");
        }
        sendMsg(clientSock, "END\n");
    }

    close(clientSock);
}

// -------------------------------------------------------
// Main: start TCP server and accept connections
// -------------------------------------------------------
int main() {
    // Load any previously saved metadata from disk on startup
    std::filesystem::create_directories("master_meta");

    int serverSock = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSock < 0) {
        std::cerr << "Socket creation failed\n";
        return 1;
    }

    // Allow reusing the port immediately after restart
    int opt = 1;
    setsockopt(serverSock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;  // accept connections on any network interface
    addr.sin_port = htons(MASTER_PORT); // htons converts port to network byte order

    if (bind(serverSock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Bind failed on port " << MASTER_PORT << "\n";
        return 1;
    }

    listen(serverSock, 10);  // queue up to 10 pending connections
    std::cout << "[Master] Running on port " << MASTER_PORT << "\n";

    // Accept loop — runs forever, handles each connection in a new thread
    while (true) {
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);
        int clientSock = accept(serverSock, (sockaddr*)&clientAddr, &clientLen);

        if (clientSock < 0) continue;

        // Detach thread: it runs independently and cleans itself up
        std::thread(handleClient, clientSock).detach();
    }

    return 0;
}