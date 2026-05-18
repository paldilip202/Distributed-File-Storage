#include "metadata.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>

void MetadataStore::addFile(const std::string& filename, const FileMetadata& meta) {
    std::lock_guard<std::mutex> lock(mutex_);
    store_[filename] = meta;
}

// Adds a single chunk incrementally — called by the Master each time
// a client reports "CHUNK_DONE" after storing one chunk successfully.
void MetadataStore::addChunk(const std::string& filename, const ChunkInfo& chunk) {
    std::lock_guard<std::mutex> lock(mutex_);
    store_[filename].filename = filename;
    store_[filename].chunks.push_back(chunk);
    store_[filename].total_chunks = store_[filename].chunks.size();
}

FileMetadata MetadataStore::getFile(const std::string& filename) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (store_.find(filename) == store_.end())
        throw std::runtime_error("File not found: " + filename);
    return store_[filename];
}

bool MetadataStore::exists(const std::string& filename) {
    std::lock_guard<std::mutex> lock(mutex_);
    return store_.find(filename) != store_.end();
}

void MetadataStore::printMetadata(const std::string& filename) {
    FileMetadata meta = getFile(filename);
    std::cout << "\n=== Metadata: " << filename << " ===\n";
    std::cout << "Total size   : " << meta.total_size << " bytes\n";
    std::cout << "Total chunks : " << meta.total_chunks << "\n";
    for (auto& c : meta.chunks) {
        std::cout << "  Chunk " << c.index << " | " << c.chunk_id
                  << " | " << c.size << " bytes | Replicas: ";
        for (auto& s : c.server_addresses) std::cout << s << " ";
        std::cout << "\n";
    }
}

// Serializes metadata to a plain-text file on disk.
// Format per chunk line: chunk:<id>:<size>:<index>:<server1;server2>
// The semicolon separates multiple server addresses within one field,
// because colons are already used to separate the fields themselves.
void MetadataStore::saveToDisk(const std::string& dir, const std::string& filename) {
    FileMetadata meta = getFile(filename);
    std::string path = dir + "/" + filename + ".meta";
    std::ofstream out(path);
    if (!out.is_open()) throw std::runtime_error("Cannot write: " + path);

    out << "filename:" << meta.filename << "\n";
    out << "total_size:" << meta.total_size << "\n";
    out << "total_chunks:" << meta.total_chunks << "\n";
    for (auto& c : meta.chunks) {
        out << "chunk:" << c.chunk_id << ":" << c.size << ":" << c.index << ":";
        for (size_t i = 0; i < c.server_addresses.size(); i++) {
            if (i > 0) out << ";";
            out << c.server_addresses[i];
        }
        out << "\n";
    }
    out.close();
}

void MetadataStore::loadFromDisk(const std::string& dir, const std::string& filename) {
    std::string path = dir + "/" + filename + ".meta";
    std::ifstream in(path);
    if (!in.is_open())
        throw std::runtime_error("Metadata not found: " + path);

    FileMetadata meta;
    std::string line;
    while (std::getline(in, line)) {
        std::vector<std::string> tokens;
        std::stringstream ss(line);
        std::string token;
        while (std::getline(ss, token, ':')) tokens.push_back(token);
        if (tokens.empty()) continue;

        if      (tokens[0] == "filename")     meta.filename = tokens[1];
        else if (tokens[0] == "total_size")   meta.total_size = std::stoull(tokens[1]);
        else if (tokens[0] == "total_chunks") meta.total_chunks = std::stoi(tokens[1]);
        else if (tokens[0] == "chunk") {
            ChunkInfo c;
            c.chunk_id = tokens[1];
            c.size     = std::stoull(tokens[2]);
            c.index    = std::stoi(tokens[3]);
            // tokens[4] contains semicolon-separated server addresses
            std::stringstream srv(tokens[4]);
            std::string s;
            while (std::getline(srv, s, ';'))
                c.server_addresses.push_back(s);
            meta.chunks.push_back(c);
        }
    }
    std::lock_guard<std::mutex> lock(mutex_);
    store_[meta.filename] = meta;
}

// Returns a full copy of all stored metadata — used by the monitor
// thread to scan for orphaned chunks after a server failure.
// We return copies, not references, to avoid data races between
// the monitor thread iterating and other threads modifying the store.
std::vector<FileMetadata> MetadataStore::getAllFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<FileMetadata> result;
    for (auto& pair : store_)
        result.push_back(pair.second);
    return result;
}

void MetadataStore::updateChunkReplicas(const std::string& filename,
                                         const std::string& chunk_id,
                                         const std::vector<std::string>& newServers) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (store_.find(filename) == store_.end()) return;
    for (auto& chunk : store_[filename].chunks) {
        if (chunk.chunk_id == chunk_id) {
            chunk.server_addresses = newServers;
            return;
        }
    }
}