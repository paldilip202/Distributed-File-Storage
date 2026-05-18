#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <filesystem>
#include <unordered_map>
#include <ctime>
#include <chrono>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "metadata.h"

namespace fs = std::filesystem;

// -------------------------------------------------------
// Global state shared across all threads
// -------------------------------------------------------
MetadataStore metaStore;
std::vector<std::string> chunkServers;   // currently healthy servers
std::mutex csMutex;
int roundRobinIndex = 0;

// Maps "host:port" → timestamp of last heartbeat received.
// time_t is a long integer representing seconds since Unix epoch.
std::unordered_map<std::string, time_t> lastHeartbeat;
std::mutex hbMutex;

const int MASTER_PORT      = 8080;
const int REPLICATION_FACTOR = 2;
const int HEARTBEAT_TIMEOUT  = 9;  // seconds before a server is declared dead

// -------------------------------------------------------
// Forward declaration — allows monitorLoop() to call
// replicateOrphanedChunks() even though the full definition
// appears later in the file. C++ compiles top-to-bottom so
// without this the compiler would reject the call.
// -------------------------------------------------------
void replicateOrphanedChunks(const std::string& deadServer);

// -------------------------------------------------------
// Socket helpers
// -------------------------------------------------------
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

// Opens a TCP connection to host:port and returns the socket fd.
// Returns -1 on failure instead of throwing, because the caller
// may want to try another server rather than crash.
int connectTo(const std::string& host, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }
    return sock;
}

// Parses "localhost:9001" into {"localhost", 9001}
std::pair<std::string, int> parseAddress(const std::string& addr) {
    size_t colon = addr.find(':');
    return { addr.substr(0, colon), std::stoi(addr.substr(colon + 1)) };
}

// Picks REPLICATION_FACTOR servers using round-robin distribution.
// For chunk 0: picks servers[0], servers[1]
// For chunk 1: picks servers[1], servers[2] (or wraps around)
// This ensures the load is evenly spread across all chunk servers.
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

// Finds a healthy server that does NOT already hold a replica
// of a particular chunk, so re-replication goes to a genuinely
// new location rather than duplicating an existing replica.
std::string pickNewServer(const std::vector<std::string>& existingServers) {
    std::lock_guard<std::mutex> lock(csMutex);
    for (auto& srv : chunkServers) {
        bool alreadyHasIt = std::find(existingServers.begin(),
                                       existingServers.end(),
                                       srv) != existingServers.end();
        if (!alreadyHasIt) return srv;
    }
    return "";  // no suitable server available
}

// -------------------------------------------------------
// Monitor thread — wakes every second, checks all known
// servers against the heartbeat timeout, and declares
// dead any server that has gone silent too long.
// -------------------------------------------------------
void monitorLoop() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        time_t now = std::time(nullptr);
        std::vector<std::string> deadServers;

        // Collect dead servers inside a short locked section.
        // We release the locks BEFORE calling replicateOrphanedChunks
        // because re-replication makes outbound network connections
        // that can take seconds — holding locks that long would block
        // every other thread trying to access chunk server state.
        {
            std::lock_guard<std::mutex> hbLock(hbMutex);
            std::lock_guard<std::mutex> csLock(csMutex);

            for (auto& [server, lastSeen] : lastHeartbeat) {
                double silence = difftime(now, lastSeen);
                bool isActive  = std::find(chunkServers.begin(),
                                           chunkServers.end(),
                                           server) != chunkServers.end();
                if (isActive && silence > HEARTBEAT_TIMEOUT) {
                    std::cout << "\n[Master] FAILURE DETECTED: " << server
                              << " (silent for " << (int)silence << "s)\n";
                    chunkServers.erase(
                        std::remove(chunkServers.begin(), chunkServers.end(), server),
                        chunkServers.end()
                    );
                    deadServers.push_back(server);
                }
            }
        }
        // Locks released — now safe to do slow network operations

        for (auto& deadServer : deadServers)
            replicateOrphanedChunks(deadServer);
    }
}

// -------------------------------------------------------
// Re-replication — restores the replication factor after a
// server failure by copying orphaned chunks to new servers.
// Uses peer-to-peer transfer: the Master tells a healthy
// chunk server to push the chunk directly to a new server,
// so the Master never touches the actual file data.
// -------------------------------------------------------
void replicateOrphanedChunks(const std::string& deadServer) {
    std::cout << "[Master] Starting re-replication for chunks lost on: "
              << deadServer << "\n";

    // Work on a snapshot so we don't hold the metadata lock
    // during the potentially slow network transfer operations
    std::vector<FileMetadata> allFiles = metaStore.getAllFiles();
    int restored = 0;

    for (auto& fileMeta : allFiles) {
        for (auto& chunk : fileMeta.chunks) {

            // Check if this chunk had a replica on the dead server
            bool affected = false;
            std::string sourceServer = "";

            for (auto& srv : chunk.server_addresses) {
                if (srv == deadServer) {
                    affected = true;
                } else {
                    sourceServer = srv;  // a healthy replica we can copy from
                }
            }

            if (!affected) continue;

            if (sourceServer.empty()) {
                // The dead server held the only replica — permanent data loss.
                // Real systems use replication factor 3 to guard against this.
                std::cerr << "[Master] DATA LOSS: No surviving replica for "
                          << chunk.chunk_id << "\n";
                continue;
            }

            // Find a healthy server that doesn't already have this chunk
            std::string targetServer = pickNewServer(chunk.server_addresses);
            if (targetServer.empty()) {
                std::cout << "[Master] No available server to re-replicate "
                          << chunk.chunk_id << "\n";
                continue;
            }

            auto [sourceHost, sourcePort] = parseAddress(sourceServer);
            auto [targetHost, targetPort] = parseAddress(targetServer);

            std::cout << "[Master] Re-replicating " << chunk.chunk_id
                      << ": " << sourceServer << " → " << targetServer << "\n";

            // Tell the source server to push this chunk directly to the target
            int sourceSock = connectTo(sourceHost, sourcePort);
            if (sourceSock < 0) {
                std::cerr << "[Master] Cannot reach source " << sourceServer << "\n";
                continue;
            }

            sendMsg(sourceSock, "REPLICATE " + chunk.chunk_id + " " +
                    targetHost + " " + std::to_string(targetPort) + "\n");
            std::string response = recvLine(sourceSock);
            close(sourceSock);

            if (response == "OK") {
                // Update metadata: remove dead server, add new target
                std::vector<std::string> newServers;
                for (auto& srv : chunk.server_addresses)
                    if (srv != deadServer) newServers.push_back(srv);
                newServers.push_back(targetServer);

                metaStore.updateChunkReplicas(fileMeta.filename,
                                              chunk.chunk_id, newServers);
                metaStore.saveToDisk("master_meta", fileMeta.filename);

                std::cout << "[Master] Restored: " << chunk.chunk_id << "\n";
                restored++;
            } else {
                std::cerr << "[Master] Re-replication FAILED for "
                          << chunk.chunk_id << "\n";
            }
        }
    }

    std::cout << "[Master] Re-replication complete. "
              << restored << " chunk(s) restored.\n\n";
}

// -------------------------------------------------------
// Handle one client connection — each connection is handled
// in its own thread so the Master can serve many clients
// simultaneously without any one blocking the others.
// -------------------------------------------------------
void handleClient(int clientSock) {
    std::string line = recvLine(clientSock);
    std::cout << "[Master] Received: " << line << "\n";

    std::istringstream iss(line);
    std::string command;
    iss >> command;

    if (command == "REGISTER") {
        // A chunk server announcing itself on startup.
        // We add it to the active list and initialise its heartbeat timestamp.
        std::string host, port;
        iss >> host >> port;
        std::string address = host + ":" + port;

        {
            std::lock_guard<std::mutex> lock(csMutex);
            if (std::find(chunkServers.begin(), chunkServers.end(), address)
                == chunkServers.end()) {
                chunkServers.push_back(address);
                std::cout << "[Master] Registered: " << address << "\n";
            } else {
                std::cout << "[Master] Re-registered (back online): " << address << "\n";
            }
        }
        {
            std::lock_guard<std::mutex> lock(hbMutex);
            lastHeartbeat[address] = std::time(nullptr);
        }
        sendMsg(clientSock, "OK\n");
    }

    else if (command == "HEARTBEAT") {
        // A chunk server saying "I'm still alive."
        // We just update its last-seen timestamp — no other work needed.
        std::string host, port;
        iss >> host >> port;
        std::string address = host + ":" + port;
        {
            std::lock_guard<std::mutex> lock(hbMutex);
            lastHeartbeat[address] = std::time(nullptr);
        }
        sendMsg(clientSock, "OK\n");
    }

    else if (command == "STORE") {
        // Client wants to store a file. We create an empty metadata
        // entry and tell the client which chunk servers are available.
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
        // Client reports a chunk was successfully stored on N servers.
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
        sendMsg(clientSock, "OK\n");
    }

    else if (command == "FETCH") {
        // Client wants to retrieve a file. We return the ordered
        // chunk list with all replica locations for each chunk.
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
            // Format: chunk_id size index server1 server2 ...
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

// -------------------------------------------------------
// Main — starts the monitor thread then enters the accept
// loop, spawning a new handler thread per connection.
// -------------------------------------------------------
int main() {
    fs::create_directories("master_meta");

    int serverSock = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(serverSock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(MASTER_PORT);

    if (bind(serverSock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Bind failed on port " << MASTER_PORT << "\n";
        return 1;
    }

    listen(serverSock, 10);
    std::cout << "[Master] Running on port " << MASTER_PORT << "\n";
    std::cout << "[Master] Heartbeat timeout: " << HEARTBEAT_TIMEOUT << "s\n\n";

    // Launch the monitor thread — it runs forever independently,
    // checking for dead servers once per second
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