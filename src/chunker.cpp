#include "chunker.h"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <stdexcept>

namespace fs = std::filesystem;

FileMetadata splitFile(const std::string& filepath, const std::string& chunk_dir) {
    std::ifstream infile(filepath, std::ios::binary);
    if (!infile.is_open())
        throw std::runtime_error("Cannot open file: " + filepath);

    fs::create_directories(chunk_dir);

    // Extract just the filename from the full path
    // e.g. "/home/dilip/movie.mp4" becomes "movie.mp4"
    std::string filename = fs::path(filepath).filename().string();

    // Determine file size by seeking to the end
    infile.seekg(0, std::ios::end);
    size_t total_size = infile.tellg();
    infile.seekg(0, std::ios::beg);

    FileMetadata meta;
    meta.filename    = filename;
    meta.total_size  = total_size;
    meta.total_chunks = 0;

    std::vector<char> buffer(CHUNK_SIZE);
    int chunk_index = 0;

    while (!infile.eof()) {
        infile.read(buffer.data(), CHUNK_SIZE);
        size_t bytes_read = infile.gcount();
        if (bytes_read == 0) break;

        std::string chunk_id   = filename + "_chunk_" + std::to_string(chunk_index);
        std::string chunk_path = chunk_dir + "/" + chunk_id;

        std::ofstream chunk_file(chunk_path, std::ios::binary);
        if (!chunk_file.is_open())
            throw std::runtime_error("Cannot write chunk: " + chunk_path);
        chunk_file.write(buffer.data(), bytes_read);
        chunk_file.close();

        ChunkInfo info;
        info.chunk_id = chunk_id;
        info.size     = bytes_read;
        info.index    = chunk_index;
        meta.chunks.push_back(info);

        std::cout << "Written chunk " << chunk_index
                  << " → " << bytes_read << " bytes\n";
        chunk_index++;
    }

    meta.total_chunks = chunk_index;
    infile.close();
    return meta;
}

void assembleFile(const std::string& filename,
                  const FileMetadata& meta,
                  const std::string& chunk_dir,
                  const std::string& output_path) {
    std::ofstream outfile(output_path, std::ios::binary);
    if (!outfile.is_open())
        throw std::runtime_error("Cannot create output file: " + output_path);

    // Process chunks in index order — order matters for correct reassembly
    for (const auto& chunk : meta.chunks) {
        std::string chunk_path = chunk_dir + "/" + chunk.chunk_id;
        std::ifstream chunk_file(chunk_path, std::ios::binary);
        if (!chunk_file.is_open())
            throw std::runtime_error("Cannot read chunk: " + chunk_path);

        std::vector<char> buffer(chunk.size);
        chunk_file.read(buffer.data(), chunk.size);
        outfile.write(buffer.data(), chunk.size);
        chunk_file.close();

        std::cout << "Assembled chunk " << chunk.index << "\n";
    }

    outfile.close();
    std::cout << "File assembled → " << output_path << "\n";
}