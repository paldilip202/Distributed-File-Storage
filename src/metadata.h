#pragma once
#include <string>
#include <vector>
#include <unordered_map>

struct ChunkInfo {
    std::string chunk_id;
    size_t size;
    int index;
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
    FileMetadata getFile(const std::string& filename);
    bool exists(const std::string& filename);
    void printMetadata(const std::string& filename);

    // NEW: save metadata to disk and load it back
    void saveToDisk(const std::string& chunk_dir, const std::string& filename);
    void loadFromDisk(const std::string& chunk_dir, const std::string& filename);

private:
    std::unordered_map<std::string, FileMetadata> store_;
};