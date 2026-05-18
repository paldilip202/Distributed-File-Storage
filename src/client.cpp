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

// -------------------------------------------------------
// Low-level socket helpers
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

// Open a TCP connection to host:port, return socket fd
int connectTo(const std::string& host, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        throw std::runtime_error("Cannot connect to " + host + ":" + std::to_string(port));
    }
    return sock;
}

// Parse "host:port" string into separate host and port
std::pair<std::string, int> parseAddress(const std::string& addr) {
    size_t colon = addr.find(':');
    std::string host = addr.substr(0, colon);
    int port = std::stoi(addr.substr(colon + 1));
    return {host, port};
}

// -------------------------------------------------------
// Store: split file locally, negotiate with master,
// then send each chunk to its assigned chunk server
// -------------------------------------------------------
void storeFile(const std::string& filepath) {
    std::string filename = fs::path(filepath).filename().string();

    // Step 1: Split file into chunks in a temp local folder
    fs::create_directories("temp_chunks");
    FileMetadata meta = splitFile(filepath, "temp_chunks");
    std::cout << "\nFile split into " << meta.total_chunks << " chunks.\n";

    // Step 2: Ask Master which chunk servers to use
    int masterSock = connectTo(MASTER_HOST, MASTER_PORT);
    sendMsg(masterSock, "STORE " + filename + " " + std::to_string(meta.total_size) + "\n");

    // Collect list of available chunk servers from Master response
    std::vector<std::string> servers;
    std::string line;
    while ((line = recvLine(masterSock)) != "END") {
        if (line.substr(0, 3) == "CS ") {
            servers.push_back(line.substr(3));  // strip "CS " prefix
        }
    }
    close(masterSock);

    if (servers.empty()) {
        std::cerr << "No chunk servers available!\n";
        return;
    }
    std::cout << "Available chunk servers: " << servers.size() << "\n\n";

    // Step 3: Send each chunk to a chunk server (round-robin)
    for (auto& chunkInfo : meta.chunks) {
        // Pick server via round-robin
        std::string serverAddr = servers[chunkInfo.index % servers.size()];
        auto [host, port] = parseAddress(serverAddr);

        // Read chunk from local temp storage
        std::string chunkPath = "temp_chunks/" + chunkInfo.chunk_id;
        std::ifstream chunkFile(chunkPath, std::ios::binary);
        if (!chunkFile.is_open()) {
            std::cerr << "Cannot read chunk: " << chunkPath << "\n";
            continue;
        }

        std::vector<char> data(chunkInfo.size);
        chunkFile.read(data.data(), chunkInfo.size);
        chunkFile.close();

        // Connect to chunk server and send the chunk
        int csSock = connectTo(host, port);
        sendMsg(csSock, "PUT " + chunkInfo.chunk_id + " " +
                std::to_string(chunkInfo.size) + "\n");

        // Send raw binary data immediately after the command
        send(csSock, data.data(), chunkInfo.size, 0);

        std::string response = recvLine(csSock);
        close(csSock);

        if (response != "OK") {
            std::cerr << "Chunk server error for " << chunkInfo.chunk_id << "\n";
            continue;
        }

        std::cout << "Stored chunk " << chunkInfo.index
                  << " → " << serverAddr << "\n";

        // Step 4: Report to Master that this chunk is stored
        int reportSock = connectTo(MASTER_HOST, MASTER_PORT);
        sendMsg(reportSock, "CHUNK_DONE " + chunkInfo.chunk_id + " " +
                serverAddr + " " + std::to_string(chunkInfo.index) +
                " " + std::to_string(chunkInfo.size) + "\n");
        recvLine(reportSock);  // wait for "OK"
        close(reportSock);
    }

    // Clean up temp chunks
    fs::remove_all("temp_chunks");
    std::cout << "\nStore complete: " << filename << "\n";
}

// -------------------------------------------------------
// Fetch: ask Master for chunk locations,
// download each chunk, reassemble the file
// -------------------------------------------------------
void fetchFile(const std::string& filename) {
    // Step 1: Ask Master for chunk list
    int masterSock = connectTo(MASTER_HOST, MASTER_PORT);
    sendMsg(masterSock, "FETCH " + filename + "\n");

    std::string firstLine = recvLine(masterSock);
    if (firstLine.substr(0, 2) != "OK") {
        std::cerr << "Master error: " << firstLine << "\n";
        close(masterSock);
        return;
    }

    int totalChunks = std::stoi(firstLine.substr(3));

    // Collect chunk info from Master
    struct ChunkLocation {
        std::string chunk_id;
        std::string server;
        size_t size;
        int index;
    };

    std::vector<ChunkLocation> chunks;
    std::string line;
    while ((line = recvLine(masterSock)) != "END") {
        std::istringstream iss(line);
        ChunkLocation loc;
        iss >> loc.chunk_id >> loc.server >> loc.size >> loc.index;
        chunks.push_back(loc);
    }
    close(masterSock);

    // Sort by index to ensure correct assembly order
    std::sort(chunks.begin(), chunks.end(),
              [](const ChunkLocation& a, const ChunkLocation& b) {
                  return a.index < b.index;
              });

    // Step 2: Fetch each chunk from its chunk server
    fs::create_directories("temp_chunks");

    for (auto& loc : chunks) {
        auto [host, port] = parseAddress(loc.server);
        int csSock = connectTo(host, port);
        sendMsg(csSock, "GET " + loc.chunk_id + "\n");

        std::string response = recvLine(csSock);
        if (response.substr(0, 5) != "SIZE ") {
            std::cerr << "Error fetching " << loc.chunk_id << "\n";
            close(csSock);
            continue;
        }

        size_t size = std::stoull(response.substr(5));

        // Read exactly 'size' bytes from the socket
        std::vector<char> data(size);
        size_t received = 0;
        while (received < size) {
            ssize_t n = recv(csSock, data.data() + received, size - received, 0);
            if (n <= 0) break;
            received += n;
        }
        close(csSock);

        // Save to temp folder for reassembly
        std::ofstream out("temp_chunks/" + loc.chunk_id, std::ios::binary);
        out.write(data.data(), received);
        out.close();

        std::cout << "Fetched chunk " << loc.index << " ← " << loc.server << "\n";
    }

    // Step 3: Reassemble into final file
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
        std::cout << "Usage:\n";
        std::cout << "  ./client store <filepath>\n";
        std::cout << "  ./client fetch <filename>\n";
        return 1;
    }

    std::string command = argv[1];
    if (command == "store") storeFile(argv[2]);
    else if (command == "fetch") fetchFile(argv[2]);
    else std::cout << "Unknown command: " << command << "\n";

    return 0;
}