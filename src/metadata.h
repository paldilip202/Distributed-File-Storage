#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>

// Holds information about one chunk of a file.
// server_addresses stores ALL replica locations (e.g. "localhost:9001")
// so the Master knows which servers hold each piece of data.
struct ChunkInfo {
    std::string chunk_id;
    size_t size;
    int index;
    std::vector<std::string> server_addresses;
};

// Holds information about one complete file stored in the system.
struct FileMetadata {
    std::string filename;
    size_t total_size;
    int total_chunks;
    std::vector<ChunkInfo> chunks;
};

// The MetadataStore is the Master's in-memory database.
// It maps filenames to their complete metadata and supports
// saving/loading from disk so metadata survives restarts.
class MetadataStore {
public:
    void addFile(const std::string& filename, const FileMetadata& meta);
    void addChunk(const std::string& filename, const ChunkInfo& chunk);
    FileMetadata getFile(const std::string& filename);
    bool exists(const std::string& filename);
    void printMetadata(const std::string& filename);
    void saveToDisk(const std::string& dir, const std::string& filename);
    void loadFromDisk(const std::string& dir, const std::string& filename);

    // Returns a snapshot of all stored files so the monitor thread
    // can scan for chunks that need re-replication after a failure.
    std::vector<FileMetadata> getAllFiles();

    // Replaces the replica list for one specific chunk.
    // Called after successful re-replication to update which
    // servers now hold a copy of the chunk.
    void updateChunkReplicas(const std::string& filename,
                             const std::string& chunk_id,
                             const std::vector<std::string>& newServers);

private:
    std::unordered_map<std::string, FileMetadata> store_;
    std::mutex mutex_;
};