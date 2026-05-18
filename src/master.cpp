#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <filesystem>
#include <unordered_map>
#include <ctime>        // for time_t and std::time()
#include <chrono>       // for sleep_for
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "metadata.h"

namespace fs = std::filesystem;

// -------------------------------------------------------
// Global state
// -------------------------------------------------------
MetadataStore metaStore;
std::vector<std::string> chunkServers;      // active, healthy servers
std::mutex csMutex;
int roundRobinIndex = 0;

// Heartbeat tracking: maps "host:port" → timestamp of last heartbeat received.
// time_t is just a long integer representing seconds since Unix epoch (Jan 1 1970).
// std::time(nullptr) gives you the current timestamp.
std::unordered_map<std::string, time_t> lastHeartbeat;
std::mutex hbMutex;  // separate mutex for heartbeat map

const int MASTER_PORT = 8080;
const int REPLICATION_FACTOR = 2;
const int HEARTBEAT_TIMEOUT = 9;  // seconds — 3 missed heartbeats

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

std::vector<std::string> pickReplicaServers() {
    std::lock_guard<std::mutex> lock(csMutex);
    std::vector<std::string> selected;
    int n = chunkServers.size();
    if (n == 0) return selected;

    int count = std::min(REPLICATION_FACTOR, n);
    for (int i = 0; i < count; i++)
        selected.push_back(chunkServers[(roundRobinIndex + i) % n]);
    roundRobinIndex = (roundRobinIndex + 1) % n;
    return selected;
}

// -------------------------------------------------------
// Monitor thread — runs continuously in the background.
// Every second it wakes up, checks all known servers,
// and removes any that haven't sent a heartbeat within
// the HEARTBEAT_TIMEOUT window.
// -------------------------------------------------------
void monitorLoop() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        time_t now = std::time(nullptr);  // current timestamp in seconds

        std::lock_guard<std::mutex> hbLock(hbMutex);
        std::lock_guard<std::mutex> csLock(csMutex);

        // Walk through every server we've ever heard from
        for (auto it = lastHeartbeat.begin(); it != lastHeartbeat.end(); ++it) {
            std::string server = it->first;
            time_t lastSeen    = it->second;
            double silence     = difftime(now, lastSeen);  // seconds since last heartbeat

            // Is this server currently in our active list?
            bool isActive = std::find(chunkServers.begin(),
                                      chunkServers.end(), server) != chunkServers.end();

            if (isActive && silence > HEARTBEAT_TIMEOUT) {
                // Server has gone silent — declare it dead and remove from active pool.
                // New chunks will no longer be assigned to this server.
                std::cout << "\n[Master] ⚠ FAILURE DETECTED: " << server
                          << " (silent for " << (int)silence << "s) — marking as DEAD\n\n";

                chunkServers.erase(
                    std::remove(chunkServers.begin(), chunkServers.end(), server),
                    chunkServers.end()
                );
                // Note: we deliberately keep the server in lastHeartbeat map.
                // If it comes back online and sends REGISTER again, we re-add it.
            }
        }
    }
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

        {
            std::lock_guard<std::mutex> lock(csMutex);
            // Avoid duplicate registration — if server was dead and comes back,
            // it will send REGISTER again. Only add if not already present.
            if (std::find(chunkServers.begin(), chunkServers.end(), address)
                == chunkServers.end()) {
                chunkServers.push_back(address);
                std::cout << "[Master] Registered: " << address << "\n";
            } else {
                std::cout << "[Master] Re-registered (back online): " << address << "\n";
            }
        }

        // Initialize heartbeat timestamp immediately on registration
        {
            std::lock_guard<std::mutex> lock(hbMutex);
            lastHeartbeat[address] = std::time(nullptr);
        }

        sendMsg(clientSock, "OK\n");
    }

    else if (command == "HEARTBEAT") {
        // Chunk server is saying "I'm alive".
        // We simply update its timestamp in the heartbeat map.
        std::string host, port;
        iss >> host >> port;
        std::string address = host + ":" + port;

        {
            std::lock_guard<std::mutex> lock(hbMutex);
            lastHeartbeat[address] = std::time(nullptr);
        }

        sendMsg(clientSock, "OK\n");
        // Note: we do NOT print every heartbeat — it would flood the terminal.
        // Uncomment the line below only when debugging:
        // std::cout << "[Master] Heartbeat from " << address << "\n";
    }

    else if (command == "STORE") {
        std::string filename;
        size_t filesize;
        iss >> filename >> filesize;

        FileMetadata meta;
        meta.filename    = filename;
        meta.total_size  = filesize;
        meta.total_chunks = 0;
        metaStore.addFile(filename, meta);

        {
            std::lock_guard<std::mutex> lock(csMutex);
            for (auto& cs : chunkServers)
                sendMsg(clientSock, "CS " + cs + "\n");
        }
        sendMsg(clientSock, "END\n");
    }

    else if (command == "CHUNK_DONE") {
        // Format: CHUNK_DONE chunk_id <num_replicas> server1 server2 index size
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

        std::cout << "[Master] Chunk recorded: " << chunk_id
                  << " on " << numReplicas << " replica(s)\n";
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
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(MASTER_PORT);

    bind(serverSock, (sockaddr*)&addr, sizeof(addr));
    listen(serverSock, 10);

    std::cout << "[Master] Running on port " << MASTER_PORT << "\n";
    std::cout << "[Master] Heartbeat timeout: " << HEARTBEAT_TIMEOUT << "s\n\n";

    // Launch monitor thread — it runs forever in the background
    // checking for dead servers independently of the request-handling loop
    std::thread(monitorLoop).detach();

    while (true) {
        sockaddr_in clientAddr{};
        socklen_t len = sizeof(clientAddr);
        int clientSock = accept(serverSock, (sockaddr*)&clientAddr, &len);
        if (clientSock < 0) continue;
        std::thread(handleClient, clientSock).detach();
    }

    return 0;
}