#include <iostream>
#include "chunker.h"
#include "metadata.h"

int main(int argc, char* argv[]) {

    if (argc < 2) {
        std::cout << "Usage:\n";
        std::cout << "  ./dfs store <filepath>\n";
        std::cout << "  ./dfs fetch <filename>\n";
        return 1;
    }

    std::string command = argv[1];
    MetadataStore metaStore;

    if (command == "store" && argc == 3) {
        std::string filepath = argv[2];

        std::cout << "Storing file: " << filepath << "\n\n";
        FileMetadata meta = splitFile(filepath, "chunks");
        metaStore.addFile(meta.filename, meta);
        metaStore.printMetadata(meta.filename);

        // Save metadata to disk so fetch can find it later
        metaStore.saveToDisk("chunks", meta.filename);

        std::cout << "\nStore complete.\n";
    }
    else if (command == "fetch" && argc == 3) {
        std::string filename = argv[2];

        // Load metadata from disk — this is how we remember across runs
        metaStore.loadFromDisk("chunks", filename);

        FileMetadata meta = metaStore.getFile(filename);
        assembleFile(filename, meta, "chunks", "restored_" + filename);
    }
    else {
        std::cout << "Invalid command.\n";
        return 1;
    }

    return 0;
}