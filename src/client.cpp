#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <filesystem>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "chunker.h"

namespace fs = std::filesystem;

const int MASTER_PORT = 8080;
const std::string MASTER_HOST = "127.0.0.1";
const int REPLICATION_FACTOR = 2;

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

int connectTo(const std::string& host, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;  // return -1 on failure instead of throwing
    }
    return sock;
}

std::pair<std::string, int> parseAddress(const std::string& addr) {
    size_t colon = addr.find(':');
    return { addr.substr(0, colon), std::stoi(addr.substr(colon + 1)) };
}

// Send one chunk to one chunk server. Returns true on success.
bool sendChunkToServer(const std::string& serverAddr,
                       const std::string& chunk_id,
                       const std::vector<char>& data) {
    auto [host, port] = parseAddress(serverAddr);
    int sock = connectTo(host, port);
    if (sock < 0) {
        std::cerr << "  [WARN] Cannot connect to " << serverAddr << "\n";
        return false;
    }

    sendMsg(sock, "PUT " + chunk_id + " " + std::to_string(data.size()) + "\n");
    send(sock, data.data(), data.size(), 0);
    std::string response = recvLine(sock);
    close(sock);
    return response == "OK";
}

void storeFile(const std::string& filepath) {
    std::string filename = fs::path(filepath).filename().string();

    fs::create_directories("temp_chunks");
    FileMetadata meta = splitFile(filepath, "temp_chunks");
    std::cout << "\nFile split into " << meta.total_chunks << " chunks.\n";

    // Ask Master for available chunk servers
    int masterSock = connectTo(MASTER_HOST, MASTER_PORT);
    sendMsg(masterSock, "STORE " + filename + " " +
            std::to_string(meta.total_size) + "\n");

    std::vector<std::string> servers;
    std::string line;
    while ((line = recvLine(masterSock)) != "END") {
        if (line.substr(0, 3) == "CS ")
            servers.push_back(line.substr(3));
    }
    close(masterSock);

    if (servers.size() < 2) {
        std::cerr << "Need at least 2 chunk servers for replication. Found: "
                  << servers.size() << "\n";
        return;
    }
    std::cout << "Chunk servers available: " << servers.size()
              << " | Replication factor: " << REPLICATION_FACTOR << "\n\n";

    for (auto& chunkInfo : meta.chunks) {
        // Load chunk data from local temp storage
        std::string chunkPath = "temp_chunks/" + chunkInfo.chunk_id;
        std::ifstream chunkFile(chunkPath, std::ios::binary);
        std::vector<char> data(chunkInfo.size);
        chunkFile.read(data.data(), chunkInfo.size);
        chunkFile.close();

        // Pick REPLICATION_FACTOR servers for this chunk using round-robin.
        // For chunk index i with N servers: use server[i%N] and server[(i+1)%N]
        // This guarantees load is balanced — no single server gets all primary chunks.
        std::vector<std::string> replicas;
        for (int r = 0; r < REPLICATION_FACTOR; r++) {
            replicas.push_back(servers[(chunkInfo.index + r) % servers.size()]);
        }

        // Send chunk to ALL replica servers
        std::vector<std::string> successfulReplicas;
        for (auto& srv : replicas) {
            std::cout << "  Sending chunk " << chunkInfo.index
                      << " → " << srv << " ... ";
            if (sendChunkToServer(srv, chunkInfo.chunk_id, data)) {
                std::cout << "OK\n";
                successfulReplicas.push_back(srv);
            } else {
                std::cout << "FAILED\n";
            }
        }

        if (successfulReplicas.empty()) {
            std::cerr << "CRITICAL: Chunk " << chunkInfo.index
                      << " could not be stored on any server!\n";
            continue;
        }

        // Report ALL successful replica locations to Master
        // Format: CHUNK_DONE chunk_id <num_replicas> server1 server2 index size
        int reportSock = connectTo(MASTER_HOST, MASTER_PORT);
        std::string report = "CHUNK_DONE " + chunkInfo.chunk_id + " " +
                             std::to_string(successfulReplicas.size());
        for (auto& srv : successfulReplicas)
            report += " " + srv;
        report += " " + std::to_string(chunkInfo.index) +
                  " " + std::to_string(chunkInfo.size) + "\n";
        sendMsg(reportSock, report);
        recvLine(reportSock);
        close(reportSock);
    }

    fs::remove_all("temp_chunks");
    std::cout << "\nStore complete: " << filename << "\n";
}

void fetchFile(const std::string& filename) {
    int masterSock = connectTo(MASTER_HOST, MASTER_PORT);
    sendMsg(masterSock, "FETCH " + filename + "\n");

    std::string firstLine = recvLine(masterSock);
    if (firstLine.substr(0, 2) != "OK") {
        std::cerr << "Master error: " << firstLine << "\n";
        close(masterSock);
        return;
    }

    int totalChunks = std::stoi(firstLine.substr(3));

    struct ChunkLocation {
        std::string chunk_id;
        size_t size;
        int index;
        std::vector<std::string> servers;  // all replicas
    };

    std::vector<ChunkLocation> chunks;
    std::string line;
    while ((line = recvLine(masterSock)) != "END") {
        // Format: chunk_id size index server1 server2 ...
        std::istringstream iss(line);
        ChunkLocation loc;
        iss >> loc.chunk_id >> loc.size >> loc.index;
        std::string srv;
        while (iss >> srv)
            loc.servers.push_back(srv);
        chunks.push_back(loc);
    }
    close(masterSock);

    std::sort(chunks.begin(), chunks.end(),
              [](const ChunkLocation& a, const ChunkLocation& b) {
                  return a.index < b.index;
              });

    fs::create_directories("temp_chunks");

    for (auto& loc : chunks) {
        bool fetched = false;

        // Try each replica in order — this is the fault tolerance in action.
        // If server 1 is dead, we automatically fall back to server 2.
        for (auto& serverAddr : loc.servers) {
            auto [host, port] = parseAddress(serverAddr);
            int csSock = connectTo(host, port);

            if (csSock < 0) {
                std::cout << "  [WARN] Chunk " << loc.index
                          << ": " << serverAddr << " unreachable, trying next replica...\n";
                continue;  // try next replica
            }

            sendMsg(csSock, "GET " + loc.chunk_id + "\n");
            std::string response = recvLine(csSock);

            if (response.substr(0, 5) != "SIZE ") {
                std::cout << "  [WARN] Bad response from " << serverAddr
                          << ", trying next replica...\n";
                close(csSock);
                continue;
            }

            size_t size = std::stoull(response.substr(5));
            std::vector<char> data(size);
            size_t received = 0;
            while (received < size) {
                ssize_t n = recv(csSock, data.data() + received, size - received, 0);
                if (n <= 0) break;
                received += n;
            }
            close(csSock);

            std::ofstream out("temp_chunks/" + loc.chunk_id, std::ios::binary);
            out.write(data.data(), received);
            out.close();

            std::cout << "  Fetched chunk " << loc.index
                      << " ← " << serverAddr << " ✓\n";
            fetched = true;
            break;  // success — no need to try other replicas
        }

        if (!fetched) {
            std::cerr << "CRITICAL: Could not fetch chunk " << loc.index
                      << " from any replica!\n";
        }
    }

    // Reassemble
    std::string outputPath = "restored_" + filename;
    std::ofstream outFile(outputPath, std::ios::binary);
    for (auto& loc : chunks) {
        std::ifstream in("temp_chunks/" + loc.chunk_id, std::ios::binary);
        std::vector<char> buf(loc.size);
        in.read(buf.data(), loc.size);
        outFile.write(buf.data(), in.gcount());
    }
    outFile.close();

    fs::remove_all("temp_chunks");
    std::cout << "\nFile restored → " << outputPath << "\n";
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cout << "Usage:\n  ./client store <filepath>\n  ./client fetch <filename>\n";
        return 1;
    }
    std::string command = argv[1];
    if      (command == "store") storeFile(argv[2]);
    else if (command == "fetch") fetchFile(argv[2]);
    else std::cout << "Unknown command.\n";
    return 0;
}