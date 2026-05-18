#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <thread>
#include <chrono>       // for std::this_thread::sleep_for
#include <filesystem>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace fs = std::filesystem;

std::string storageDir;
int myPort;
std::string myHost = "localhost";

const int MASTER_PORT = 8080;
const std::string MASTER_HOST = "127.0.0.1";
const int HEARTBEAT_INTERVAL = 3;  // send a heartbeat every 3 seconds

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

void handleClient(int clientSock) {
    std::string line = recvLine(clientSock);
    std::istringstream iss(line);
    std::string command;
    iss >> command;

    if (command == "PUT") {
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

        // Read raw bytes in a loop — TCP does not guarantee all bytes
        // arrive in a single recv() call, so we keep reading until
        // we have received exactly 'size' bytes.
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

    close(clientSock);
}

void registerWithMaster() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(MASTER_PORT);
    inet_pton(AF_INET, MASTER_HOST.c_str(), &addr.sin_addr);

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "[CS:" << myPort << "] Cannot connect to Master!\n";
        return;
    }

    std::string msg = "REGISTER " + myHost + " " + std::to_string(myPort) + "\n";
    send(sock, msg.c_str(), msg.size(), 0);

    char buf[64] = {};
    recv(sock, buf, sizeof(buf), 0);
    std::cout << "[CS:" << myPort << "] Registered with Master.\n";
    close(sock);
}

// This function runs in its own background thread.
// It wakes up every HEARTBEAT_INTERVAL seconds, opens a fresh
// TCP connection to the Master, sends "HEARTBEAT host port",
// waits for "OK", and then closes the connection and sleeps again.
// Using a fresh connection each time keeps the implementation simple
// and avoids issues with long-lived idle connections being dropped
// by firewalls or OS-level TCP keepalive timeouts.
void heartbeatLoop() {
    while (true) {
        // Sleep first, then send — gives Master time to start up
        std::this_thread::sleep_for(std::chrono::seconds(HEARTBEAT_INTERVAL));

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(MASTER_PORT);
        inet_pton(AF_INET, MASTER_HOST.c_str(), &addr.sin_addr);

        if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
            // Master might be temporarily unreachable — just warn and retry next cycle
            std::cerr << "[CS:" << myPort << "] Heartbeat failed — cannot reach Master\n";
            close(sock);
            continue;
        }

        std::string msg = "HEARTBEAT " + myHost + " " + std::to_string(myPort) + "\n";
        send(sock, msg.c_str(), msg.size(), 0);
        recvLine(sock);  // wait for "OK"
        close(sock);

        std::cout << "[CS:" << myPort << "] Heartbeat sent ♥\n";
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage: ./chunkserver <port>\n";
        return 1;
    }

    myPort = std::stoi(argv[1]);
    storageDir = "chunks_" + std::to_string(myPort);
    fs::create_directories(storageDir);

    std::cout << "[CS:" << myPort << "] Storage: " << storageDir << "\n";

    registerWithMaster();

    // Launch heartbeat in a background thread.
    // std::thread::detach() means we don't need to join it later —
    // it runs independently until the process exits.
    std::thread(heartbeatLoop).detach();

    // Main thread handles incoming chunk requests as before
    int serverSock = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(serverSock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(myPort);

    bind(serverSock, (sockaddr*)&addr, sizeof(addr));
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