#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <thread>
#include <filesystem>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace fs = std::filesystem;

std::string storageDir;  // where this server saves its chunks

// -------------------------------------------------------
// Helpers (same as master)
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

// -------------------------------------------------------
// Handle one client (could be another chunk server or client)
// -------------------------------------------------------
void handleClient(int clientSock) {
    std::string line = recvLine(clientSock);
    std::cout << "[ChunkServer] Received: " << line << "\n";

    std::istringstream iss(line);
    std::string command;
    iss >> command;

    if (command == "PUT") {
        // Client stores a chunk: "PUT chunk_id size"
        // Followed immediately by 'size' bytes of raw binary data
        std::string chunk_id;
        size_t size;
        iss >> chunk_id >> size;

        std::string chunk_path = storageDir + "/" + chunk_id;
        std::ofstream out(chunk_path, std::ios::binary);
        if (!out.is_open()) {
            sendMsg(clientSock, "ERROR Cannot write chunk\n");
            close(clientSock);
            return;
        }

        // Read exactly 'size' bytes from the socket
        // We cannot just call recv once — TCP may deliver data in pieces
        // so we loop until we have all bytes
        size_t received = 0;
        std::vector<char> buffer(4096);  // read in 4KB chunks
        while (received < size) {
            size_t toRead = std::min(buffer.size(), size - received);
            ssize_t n = recv(clientSock, buffer.data(), toRead, 0);
            if (n <= 0) break;
            out.write(buffer.data(), n);
            received += n;
        }
        out.close();

        std::cout << "[ChunkServer] Stored: " << chunk_id << " (" << received << " bytes)\n";
        sendMsg(clientSock, "OK\n");
    }

    else if (command == "GET") {
        // Client requests a chunk: "GET chunk_id"
        std::string chunk_id;
        iss >> chunk_id;

        std::string chunk_path = storageDir + "/" + chunk_id;
        std::ifstream in(chunk_path, std::ios::binary);
        if (!in.is_open()) {
            sendMsg(clientSock, "ERROR Chunk not found\n");
            close(clientSock);
            return;
        }

        // Get file size
        in.seekg(0, std::ios::end);
        size_t fileSize = in.tellg();
        in.seekg(0, std::ios::beg);

        // Send size first so client knows how many bytes to read
        sendMsg(clientSock, "SIZE " + std::to_string(fileSize) + "\n");

        // Stream the chunk data directly from disk to socket
        std::vector<char> buffer(4096);
        while (!in.eof()) {
            in.read(buffer.data(), buffer.size());
            ssize_t n = in.gcount();
            if (n <= 0) break;
            send(clientSock, buffer.data(), n, 0);
        }
        in.close();

        std::cout << "[ChunkServer] Sent: " << chunk_id << "\n";
    }

    close(clientSock);
}

// -------------------------------------------------------
// Register this chunk server with the Master Node
// -------------------------------------------------------
void registerWithMaster(const std::string& masterHost, int masterPort,
                        const std::string& myHost, int myPort) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(masterPort);
    inet_pton(AF_INET, masterHost.c_str(), &addr.sin_addr);

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "[ChunkServer] Cannot connect to Master!\n";
        return;
    }

    std::string msg = "REGISTER " + myHost + " " + std::to_string(myPort) + "\n";
    send(sock, msg.c_str(), msg.size(), 0);

    // Read the "OK" response
    char buf[64] = {};
    recv(sock, buf, sizeof(buf), 0);
    std::cout << "[ChunkServer] Registered with Master. Response: " << buf;
    close(sock);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage: ./chunkserver <port>\n";
        return 1;
    }

    int port = std::stoi(argv[1]);
    storageDir = "chunks_" + std::to_string(port);
    fs::create_directories(storageDir);

    // Each chunk server stores its chunks in its own folder
    // chunks_9001/ for the server on port 9001
    // chunks_9002/ for the server on port 9002
    std::cout << "[ChunkServer] Storage: " << storageDir << "\n";

    // Announce ourselves to the Master
    registerWithMaster("127.0.0.1", 8080, "localhost", port);

    // Start TCP server
    int serverSock = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(serverSock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    bind(serverSock, (sockaddr*)&addr, sizeof(addr));
    listen(serverSock, 10);

    std::cout << "[ChunkServer] Running on port " << port << "\n";

    while (true) {
        sockaddr_in clientAddr{};
        socklen_t len = sizeof(clientAddr);
        int clientSock = accept(serverSock, (sockaddr*)&clientAddr, &len);
        if (clientSock < 0) continue;
        std::thread(handleClient, clientSock).detach();
    }

    return 0;
}