#include "metadata.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>

void MetadataStore::addFile(const std::string& filename, const FileMetadata& meta) {
    store_[filename] = meta;
}

FileMetadata MetadataStore::getFile(const std::string& filename) {
    if (!exists(filename)) {
        throw std::runtime_error("File not found in metadata: " + filename);
    }
    return store_[filename];
}

bool MetadataStore::exists(const std::string& filename) {
    return store_.find(filename) != store_.end();
}

void MetadataStore::printMetadata(const std::string& filename) {
    FileMetadata meta = getFile(filename);
    std::cout << "\n=== Metadata for: " << filename << " ===\n";
    std::cout << "Total size   : " << meta.total_size << " bytes\n";
    std::cout << "Total chunks : " << meta.total_chunks << "\n";
    for (auto& chunk : meta.chunks) {
        std::cout << "  Chunk " << chunk.index
                  << " | ID: " << chunk.chunk_id
                  << " | Size: " << chunk.size << " bytes\n";
    }
}

// Serialize FileMetadata to a .meta file on disk
void MetadataStore::saveToDisk(const std::string& chunk_dir, const std::string& filename) {
    FileMetadata meta = getFile(filename);

    // Save as: chunks/testfile.bin.meta
    std::string meta_path = chunk_dir + "/" + filename + ".meta";
    std::ofstream outfile(meta_path);

    if (!outfile.is_open()) {
        throw std::runtime_error("Cannot write metadata file: " + meta_path);
    }

    // Write header info
    outfile << "filename:" << meta.filename << "\n";
    outfile << "total_size:" << meta.total_size << "\n";
    outfile << "total_chunks:" << meta.total_chunks << "\n";

    // Write one line per chunk
    for (const auto& chunk : meta.chunks) {
        outfile << "chunk:" << chunk.chunk_id << ":" << chunk.size << ":" << chunk.index << "\n";
    }

    outfile.close();
    std::cout << "Metadata saved → " << meta_path << "\n";
}

// Deserialize FileMetadata back from a .meta file on disk
void MetadataStore::loadFromDisk(const std::string& chunk_dir, const std::string& filename) {
    std::string meta_path = chunk_dir + "/" + filename + ".meta";
    std::ifstream infile(meta_path);

    if (!infile.is_open()) {
        throw std::runtime_error("Metadata file not found: " + meta_path + 
                                 "\nHave you stored this file yet?");
    }

    FileMetadata meta;
    std::string line;

    while (std::getline(infile, line)) {
        // Split each line by ':' into tokens
        std::vector<std::string> tokens;
        std::stringstream ss(line);
        std::string token;
        while (std::getline(ss, token, ':')) {
            tokens.push_back(token);
        }

        if (tokens.empty()) continue;

        if (tokens[0] == "filename") {
            meta.filename = tokens[1];
        } else if (tokens[0] == "total_size") {
            meta.total_size = std::stoull(tokens[1]);
        } else if (tokens[0] == "total_chunks") {
            meta.total_chunks = std::stoi(tokens[1]);
        } else if (tokens[0] == "chunk") {
            // format: chunk:<chunk_id>:<size>:<index>
            ChunkInfo info;
            info.chunk_id = tokens[1];
            info.size = std::stoull(tokens[2]);
            info.index = std::stoi(tokens[3]);
            meta.chunks.push_back(info);
        }
    }

    infile.close();
    store_[meta.filename] = meta; // load into in-memory store
    std::cout << "Metadata loaded ← " << meta_path << "\n";
}