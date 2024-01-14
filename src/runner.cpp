#include "mapreduce.hpp"

bool
PrefixFindRunner::run(const std::filesystem::path &input_file,
                      const std::filesystem::path &output_directory) {
    // Prepare
    auto blocks = split_file(input_file, mappers_count);

    // Map
    std::vector<std::filesystem::path> mapper_output_files;
    {
        std::vector<std::thread> map_workers;
        unsigned iter = 1;
        for (auto &&block : blocks) {
            using namespace std::literals;
            const auto actual_out = output_directory / mapper_subdir /
                                    ("map."s + std::to_string(iter) + ".txt");
            std::thread t(&PrefixFindRunner::mapper_task, this, input_file,
                          block, actual_out);
            map_workers.emplace_back(std::move(t));
            mapper_output_files.emplace_back(actual_out);
            ++iter;
        }

        for (auto &&worker : map_workers) {
            worker.join();
        }
    }

    // Shuffle
    auto shuffle_blocks = shuffle(
        mapper_output_files, output_directory / shuffle_file, reducers_count);
    shuffle_blocks = align_blocks(shuffle_blocks);

    // Reduce
    std::vector<std::filesystem::path> reducer_output_files;
    {
        unsigned iter = 1;
        std::vector<std::thread> reduce_workers;
        for (auto &&block : shuffle_blocks) {
            using namespace std::literals;
            const auto actual_out =
                output_directory / reducer_subdir /
                ("reduce."s + std::to_string(iter) + ".txt");
            std::thread t(&PrefixFindRunner::reducer_task, this,
                          output_directory / shuffle_file, block, actual_out);
            reduce_workers.emplace_back(std::move(t));
            reducer_output_files.emplace_back(actual_out);
            ++iter;
        }

        for (auto &&worker : reduce_workers) {
            worker.join();
        }

        std::filesystem::remove(output_directory / shuffle_file);
    }

    // Aggregate
    bool result = true;
    {
        std::ofstream result_file(output_directory / "result.txt");
        assert(result_file.is_open());

        for (auto &&path : reducer_output_files) {
            int status;
            std::ifstream file(path);
            assert(file.is_open());

            file >> status;
            result &= status > 0;
            file.close();
        }

        result_file << result << std::endl;
        result_file.close();
    }

    return result;
}