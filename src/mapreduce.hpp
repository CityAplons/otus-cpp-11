#pragma once

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <list>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

class PrefixFindRunner {
  public:
    // TODO: remove it by presenting serializers between steps
    using mapper_chunk = std::pair<std::string, unsigned>;
    using mapper_out = std::list<mapper_chunk>;
    using mapper_func_type = std::function<mapper_out(const std::string &)>;
    using reducer_func_type = std::function<bool(const mapper_chunk &)>;

    explicit PrefixFindRunner(int mpc, int rdc)
        : mappers_count(mpc), reducers_count(rdc) {}

    void set_mapper(const mapper_func_type &func) { mapper = std::move(func); }

    void set_reducer(const reducer_func_type &func) {
        reducer = std::move(func);
    }

    bool run(const std::filesystem::path &input_file,
             const std::filesystem::path &output_directory);

  private:
    int mappers_count;
    mapper_func_type mapper;

    int reducers_count;
    reducer_func_type reducer;

    const char *mapper_subdir = "mapper/";
    const char *reducer_subdir = "reducer/";
    const char *shuffle_file = "sorted.txt";

    struct Block {
        size_t from;
        size_t to;
    };

    template <bool reverse = false> struct Compare {
        bool operator()(const mapper_chunk &a, const mapper_chunk &b) {
            if (reverse) {
                return a.first >= b.first;
            }
            return a.first < b.first;
        };
    };

    void mapper_task(const std::filesystem::path input, Block offsets,
                     const std::filesystem::path output);

    void reducer_task(const std::filesystem::path input, Block offsets,
                      const std::filesystem::path output);

    std::vector<Block> split_file(const std::filesystem::path &file,
                                  size_t blocks_count);

    std::vector<Block> shuffle(const std::vector<std::filesystem::path> &mapped,
                               const std::filesystem::path &tmp_file,
                               size_t block_count);

    std::vector<Block> align_blocks(const std::vector<Block> &blocks);
};
