#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>  // needed for thread safety in Phase 2

struct ChunkInfo {
    std::string chunk_id;
    size_t size;
    int index;
    std::string server_address;  // NEW: e.g. "localhost:9001"
};

struct FileMetadata {
    std::string filename;
    size_t total_size;
    int total_chunks;
    std::vector<ChunkInfo> chunks;
};

class MetadataStore {
public:
    void addFile(const std::string& filename, const FileMetadata& meta);
    void addChunk(const std::string& filename, const ChunkInfo& chunk);  // NEW
    FileMetadata getFile(const std::string& filename);
    bool exists(const std::string& filename);
    void printMetadata(const std::string& filename);
    void saveToDisk(const std::string& chunk_dir, const std::string& filename);
    void loadFromDisk(const std::string& chunk_dir, const std::string& filename);

private:
    std::unordered_map<std::string, FileMetadata> store_;
    std::mutex mutex_;  // prevents two threads writing metadata at the same time
};