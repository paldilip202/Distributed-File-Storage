#include "metadata.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>

void MetadataStore::addFile(const std::string& filename, const FileMetadata& meta) {
    std::lock_guard<std::mutex> lock(mutex_);
    store_[filename] = meta;
}

// Called by Master when client reports "CHUNK_DONE"
// Adds chunk info incrementally, chunk by chunk
void MetadataStore::addChunk(const std::string& filename, const ChunkInfo& chunk) {
    std::lock_guard<std::mutex> lock(mutex_);
    store_[filename].filename = filename;
    store_[filename].chunks.push_back(chunk);
    store_[filename].total_chunks = store_[filename].chunks.size();
}

FileMetadata MetadataStore::getFile(const std::string& filename) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (store_.find(filename) == store_.end()) {
        throw std::runtime_error("File not found: " + filename);
    }
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
        std::cout << "  Chunk " << c.index
                  << " | " << c.chunk_id
                  << " | " << c.size << " bytes"
                  << " | " << c.server_address << "\n";
    }
}

void MetadataStore::saveToDisk(const std::string& chunk_dir, const std::string& filename) {
    FileMetadata meta = getFile(filename);
    std::string path = chunk_dir + "/" + filename + ".meta";
    std::ofstream out(path);
    if (!out.is_open()) throw std::runtime_error("Cannot write: " + path);

    out << "filename:" << meta.filename << "\n";
    out << "total_size:" << meta.total_size << "\n";
    out << "total_chunks:" << meta.total_chunks << "\n";
    for (auto& c : meta.chunks) {
        // format: chunk:<id>:<size>:<index>:<server_address>
        out << "chunk:" << c.chunk_id << ":" << c.size << ":"
            << c.index << ":" << c.server_address << "\n";
    }
    out.close();
}

void MetadataStore::loadFromDisk(const std::string& chunk_dir, const std::string& filename) {
    std::string path = chunk_dir + "/" + filename + ".meta";
    std::ifstream in(path);
    if (!in.is_open()) throw std::runtime_error("Metadata not found: " + path);

    FileMetadata meta;
    std::string line;
    while (std::getline(in, line)) {
        std::vector<std::string> tokens;
        std::stringstream ss(line);
        std::string token;
        while (std::getline(ss, token, ':')) tokens.push_back(token);
        if (tokens.empty()) continue;

        if (tokens[0] == "filename")      meta.filename = tokens[1];
        else if (tokens[0] == "total_size")   meta.total_size = std::stoull(tokens[1]);
        else if (tokens[0] == "total_chunks") meta.total_chunks = std::stoi(tokens[1]);
        else if (tokens[0] == "chunk") {
            ChunkInfo c;
            c.chunk_id      = tokens[1];
            c.size          = std::stoull(tokens[2]);
            c.index         = std::stoi(tokens[3]);
            c.server_address = tokens[4];
            meta.chunks.push_back(c);
        }
    }
    std::lock_guard<std::mutex> lock(mutex_);
    store_[meta.filename] = meta;
}