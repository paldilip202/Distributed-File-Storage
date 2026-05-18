#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <thread>
#include <chrono>
#include <filesystem>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace fs = std::filesystem;

std::string storageDir;
int myPort;
std::string myHost = "localhost";

const int MASTER_PORT      = 8080;
const std::string MASTER_HOST = "127.0.0.1";
const int HEARTBEAT_INTERVAL  = 3;  // seconds between heartbeats

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
// Handle one incoming request — could be from the client
// (PUT/GET) or from another chunk server (REPLICATE).
// -------------------------------------------------------
void handleClient(int clientSock) {
    std::string line = recvLine(clientSock);
    std::istringstream iss(line);
    std::string command;
    iss >> command;

    if (command == "PUT") {
        // Client is storing a chunk here.
        // After the command line, raw binary data follows immediately.
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

        // TCP does not guarantee all bytes arrive in one recv() call,
        // so we loop until we have received the full declared size.
        size_t received = 0;
        std::vector<char> buffer(4096);
        while (received < size) {
            size_t toRead = std::min(buffer.size(), size - received);
            ssize_t n = recv(clientSock, buffer.data(), toRead, 0);
            if (n <= 0) break;
            out.write(buffer.data(), n);
            received += n;
        }
        out.close();

        std::cout << "[CS:" << myPort << "] Stored: " << chunk_id
                  << " (" << received << " bytes)\n";
        sendMsg(clientSock, "OK\n");
    }

    else if (command == "GET") {
        // Client is requesting a chunk. We read it from disk
        // and stream it directly over the socket.
        std::string chunk_id;
        iss >> chunk_id;

        std::string chunk_path = storageDir + "/" + chunk_id;
        std::ifstream in(chunk_path, std::ios::binary);
        if (!in.is_open()) {
            sendMsg(clientSock, "ERROR Chunk not found\n");
            close(clientSock);
            return;
        }

        in.seekg(0, std::ios::end);
        size_t fileSize = in.tellg();
        in.seekg(0, std::ios::beg);

        // Send the size first so the receiver knows how many bytes to expect
        sendMsg(clientSock, "SIZE " + std::to_string(fileSize) + "\n");

        std::vector<char> buffer(4096);
        while (!in.eof()) {
            in.read(buffer.data(), buffer.size());
            ssize_t n = in.gcount();
            if (n <= 0) break;
            send(clientSock, buffer.data(), n, 0);
        }
        in.close();

        std::cout << "[CS:" << myPort << "] Sent: " << chunk_id << "\n";
    }

    else if (command == "REPLICATE") {
        // Master is asking this server to copy a chunk to another server.
        // This is peer-to-peer transfer — we act as a client to the target,
        // using the same PUT protocol the target already understands.
        std::string chunk_id, target_host;
        int target_port;
        iss >> chunk_id >> target_host >> target_port;

        std::string chunk_path = storageDir + "/" + chunk_id;
        std::ifstream in(chunk_path, std::ios::binary);
        if (!in.is_open()) {
            std::cerr << "[CS:" << myPort << "] Cannot read chunk for replication: "
                      << chunk_id << "\n";
            sendMsg(clientSock, "ERROR\n");
            close(clientSock);
            return;
        }

        in.seekg(0, std::ios::end);
        size_t fileSize = in.tellg();
        in.seekg(0, std::ios::beg);
        std::vector<char> data(fileSize);
        in.read(data.data(), fileSize);
        in.close();

        // Connect directly to the target chunk server
        int targetSock = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in targetAddr{};
        targetAddr.sin_family = AF_INET;
        targetAddr.sin_port   = htons(target_port);
        inet_pton(AF_INET, target_host.c_str(), &targetAddr.sin_addr);

        if (connect(targetSock, (sockaddr*)&targetAddr, sizeof(targetAddr)) < 0) {
            std::cerr << "[CS:" << myPort << "] Cannot connect to target "
                      << target_host << ":" << target_port << "\n";
            sendMsg(clientSock, "ERROR\n");
            close(targetSock);
            close(clientSock);
            return;
        }

        // Send PUT command then raw bytes — exactly like a normal store
        std::string putCmd = "PUT " + chunk_id + " " +
                             std::to_string(fileSize) + "\n";
        send(targetSock, putCmd.c_str(), putCmd.size(), 0);
        send(targetSock, data.data(), fileSize, 0);

        std::string response = recvLine(targetSock);
        close(targetSock);

        if (response == "OK") {
            std::cout << "[CS:" << myPort << "] Re-replicated " << chunk_id
                      << " → " << target_host << ":" << target_port << "\n";
            sendMsg(clientSock, "OK\n");
        } else {
            std::cerr << "[CS:" << myPort << "] Replication failed for "
                      << chunk_id << "\n";
            sendMsg(clientSock, "ERROR\n");
        }
    }

    close(clientSock);
}

// -------------------------------------------------------
// Announces this server to the Master on startup.
// -------------------------------------------------------
void registerWithMaster() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(MASTER_PORT);
    inet_pton(AF_INET, MASTER_HOST.c_str(), &addr.sin_addr);

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "[CS:" << myPort << "] Cannot connect to Master!\n";
        return;
    }

    std::string msg = "REGISTER " + myHost + " " +
                      std::to_string(myPort) + "\n";
    send(sock, msg.c_str(), msg.size(), 0);

    char buf[64] = {};
    recv(sock, buf, sizeof(buf), 0);
    std::cout << "[CS:" << myPort << "] Registered with Master.\n";
    close(sock);
}

// -------------------------------------------------------
// Heartbeat thread — runs forever, waking every
// HEARTBEAT_INTERVAL seconds to send "I'm alive" to Master.
// A fresh connection per heartbeat keeps things simple and
// avoids long-lived idle connections being dropped silently.
// -------------------------------------------------------
void heartbeatLoop() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(HEARTBEAT_INTERVAL));

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(MASTER_PORT);
        inet_pton(AF_INET, MASTER_HOST.c_str(), &addr.sin_addr);

        if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "[CS:" << myPort << "] Heartbeat failed — Master unreachable\n";
            close(sock);
            continue;
        }

        std::string msg = "HEARTBEAT " + myHost + " " +
                          std::to_string(myPort) + "\n";
        send(sock, msg.c_str(), msg.size(), 0);
        recvLine(sock);  // wait for "OK"
        close(sock);

        std::cout << "[CS:" << myPort << "] Heartbeat sent\n";
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage: ./chunkserver <port>\n";
        return 1;
    }

    myPort    = std::stoi(argv[1]);
    storageDir = "chunks_" + std::to_string(myPort);
    fs::create_directories(storageDir);

    std::cout << "[CS:" << myPort << "] Storage dir: " << storageDir << "\n";

    registerWithMaster();

    // Launch heartbeat as a detached background thread so it
    // runs independently of the main connection-handling loop
    std::thread(heartbeatLoop).detach();

    int serverSock = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(serverSock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(myPort);

    if (bind(serverSock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Bind failed on port " << myPort << "\n";
        return 1;
    }

    listen(serverSock, 10);
    std::cout << "[CS:" << myPort << "] Running on port " << myPort << "\n";

    while (true) {
        sockaddr_in clientAddr{};
        socklen_t len = sizeof(clientAddr);
        int clientSock = accept(serverSock, (sockaddr*)&clientAddr, &len);
        if (clientSock < 0) continue;
        std::thread(handleClient, clientSock).detach();
    }

    return 0;
}