#pragma once
#include "metadata.h"
#include <string>

// 1MB chunk size for testing. In a production system like GFS
// this would be 64MB — large enough to reduce metadata overhead
// while keeping individual transfers manageable.
const size_t CHUNK_SIZE = 1 * 1024 * 1024;

// Splits a file into fixed-size chunks, saves each chunk as a
// separate file in chunk_dir, and returns the complete metadata.
FileMetadata splitFile(const std::string& filepath,
                       const std::string& chunk_dir);

// Reassembles chunks back into the original file by reading
// each chunk in index order and writing them sequentially.
void assembleFile(const std::string& filename,
                  const FileMetadata& meta,
                  const std::string& chunk_dir,
                  const std::string& output_path);