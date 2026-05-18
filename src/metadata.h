#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>

struct ChunkInfo {
    std::string chunk_id;
    size_t size;
    int index;
    // Phase 3: now stores ALL replica locations, not just one
    std::vector<std::string> server_addresses;
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
    void addChunk(const std::string& filename, const ChunkInfo& chunk);
    FileMetadata getFile(const std::string& filename);
    bool exists(const std::string& filename);
    void printMetadata(const std::string& filename);
    void saveToDisk(const std::string& dir, const std::string& filename);
    void loadFromDisk(const std::string& dir, const std::string& filename);

private:
    std::unordered_map<std::string, FileMetadata> store_;
    std::mutex mutex_;
};