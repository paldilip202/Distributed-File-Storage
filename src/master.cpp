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

namespace fs = std::filesystem;

MetadataStore metaStore;
std::vector<std::string> chunkServers;
std::mutex csMutex;
int roundRobinIndex = 0;

const int MASTER_PORT = 8080;
const int REPLICATION_FACTOR = 2;  // every chunk goes to 2 servers

void sendMsg(int sock, const std::string& msg) {
    send(sock, msg.c_str(), msg.size(), 0);
}

std::string recvLine(int sock) {
    std::string result;
    char c;
    while (recv(sock, &c, 1, 0) > 0) {
        if (c == '\n') break;
        result += c;
    }
    return result;
}

// Returns REPLICATION_FACTOR servers for one chunk using round-robin.
// For chunk 0 with 2 servers: picks server[0], server[1]
// For chunk 1 with 2 servers: picks server[1], server[0]  ← rotated
// This ensures load is evenly spread across all servers.
std::vector<std::string> pickReplicaServers() {
    std::lock_guard<std::mutex> lock(csMutex);
    std::vector<std::string> selected;
    int n = chunkServers.size();
    if (n == 0) return selected;

    int count = std::min(REPLICATION_FACTOR, n);
    for (int i = 0; i < count; i++) {
        selected.push_back(chunkServers[(roundRobinIndex + i) % n]);
    }
    roundRobinIndex = (roundRobinIndex + 1) % n;
    return selected;
}

void handleClient(int clientSock) {
    std::string line = recvLine(clientSock);
    std::cout << "[Master] Received: " << line << "\n";

    std::istringstream iss(line);
    std::string command;
    iss >> command;

    if (command == "REGISTER") {
        std::string host, port;
        iss >> host >> port;
        std::string address = host + ":" + port;
        std::lock_guard<std::mutex> lock(csMutex);
        chunkServers.push_back(address);
        std::cout << "[Master] Registered chunk server: " << address << "\n";
        sendMsg(clientSock, "OK\n");
    }

    else if (command == "STORE") {
        std::string filename;
        size_t filesize;
        iss >> filename >> filesize;

        FileMetadata meta;
        meta.filename = filename;
        meta.total_size = filesize;
        meta.total_chunks = 0;
        metaStore.addFile(filename, meta);

        // Send all available chunk servers — client will pick replicas itself
        std::lock_guard<std::mutex> lock(csMutex);
        for (auto& cs : chunkServers)
            sendMsg(clientSock, "CS " + cs + "\n");
        sendMsg(clientSock, "END\n");
    }

    else if (command == "CHUNK_DONE") {
        // New format: CHUNK_DONE chunk_id <num_replicas> server1 server2 index size
        // Example:    CHUNK_DONE file_chunk_0 2 localhost:9001 localhost:9002 0 1048576
        std::string chunk_id;
        int numReplicas;
        iss >> chunk_id >> numReplicas;

        ChunkInfo info;
        info.chunk_id = chunk_id;
        for (int i = 0; i < numReplicas; i++) {
            std::string srv;
            iss >> srv;
            info.server_addresses.push_back(srv);
        }
        iss >> info.index >> info.size;

        std::string filename = chunk_id.substr(0, chunk_id.rfind("_chunk_"));
        metaStore.addChunk(filename, info);
        metaStore.saveToDisk("master_meta", filename);

        std::cout << "[Master] Chunk stored: " << chunk_id
                  << " on " << numReplicas << " server(s)\n";
        sendMsg(clientSock, "OK\n");
    }

    else if (command == "FETCH") {
        std::string filename;
        iss >> filename;

        if (!metaStore.exists(filename)) {
            sendMsg(clientSock, "ERROR File not found\n");
            close(clientSock);
            return;
        }

        FileMetadata meta = metaStore.getFile(filename);
        std::sort(meta.chunks.begin(), meta.chunks.end(),
                  [](const ChunkInfo& a, const ChunkInfo& b) {
                      return a.index < b.index;
                  });

        sendMsg(clientSock, "OK " + std::to_string(meta.total_chunks) + "\n");

        for (auto& c : meta.chunks) {
            // Send: chunk_id size index server1 server2 ...
            std::string msg = c.chunk_id + " " + std::to_string(c.size) +
                              " " + std::to_string(c.index);
            for (auto& srv : c.server_addresses)
                msg += " " + srv;
            msg += "\n";
            sendMsg(clientSock, msg);
        }
        sendMsg(clientSock, "END\n");
    }

    close(clientSock);
}

int main() {
    fs::create_directories("master_meta");

    int serverSock = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(serverSock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(MASTER_PORT);

    bind(serverSock, (sockaddr*)&addr, sizeof(addr));
    listen(serverSock, 10);
    std::cout << "[Master] Running on port " << MASTER_PORT << "\n";

    while (true) {
        sockaddr_in clientAddr{};
        socklen_t len = sizeof(clientAddr);
        int clientSock = accept(serverSock, (sockaddr*)&clientAddr, &len);
        if (clientSock < 0) continue;
        std::thread(handleClient, clientSock).detach();
    }

    return 0;
}