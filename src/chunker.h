#pragma once
#include "metadata.h"
#include <string>

// Default chunk size: 1MB (for testing)
// In production this would be 64MB
const size_t CHUNK_SIZE = 1 * 1024 * 1024; // 1MB

// Splits a file into chunks, saves them to disk, returns metadata
FileMetadata splitFile(const std::string& filepath, 
                       const std::string& chunk_dir);

// Reassembles chunks back into original file
void assembleFile(const std::string& filename,
                  const FileMetadata& meta,
                  const std::string& chunk_dir,
                  const std::string& output_path);