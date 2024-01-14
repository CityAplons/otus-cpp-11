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

    void run(const std::filesystem::path &input_file,
             const std::filesystem::path &output_directory) {
        std::vector<std::thread> map_workers;

        // Prepare
        auto blocks = split_file(input_file, mappers_count);

        // Map
        std::vector<std::filesystem::path> mapper_output_files;
        {
            unsigned iter = 1;
            for (auto &&block : blocks) {
                using namespace std::literals;
                const auto actual_out =
                    output_directory / mapper_subdir /
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
        auto shuffle_output_files =
            shuffle(mapper_output_files, output_directory / reducer_subdir,
                    output_directory / shuffle_subdir, reducers_count);

        // Создаём reducers_count новых файлов
        // Из mappers_count файлов читаем данные (результат фазы map) и
        // перекладываем в reducers_count (вход фазы reduce) Перекладываем так,
        // чтобы:
        //     * данные были отсортированы
        //     * одинаковые ключи оказывались в одном файле, чтобы одинаковые
        //     ключи попали на один редьюсер
        //     * файлы примерно одинакового размера, чтобы редьюсеры были
        //     загружены примерно равномерно
        //
        // Гуглить: алгоритмы во внешней памяти, external sorting, многопутевое
        // слияние
        //
        // Для упрощения задачи делаем это в один поток
        // Но все данные в память одновременно не загружаем, читаем построчно и
        // пишем
        //
        // Создаём reducers_count потоков
        // В каждом потоке читаем свой файл (выход предыдущей фазы)
        // Применяем к строкам функцию reducer
        // Результат сохраняется в файловую систему
        //             (во многих задачах выход редьюсера - большие данные, хотя
        //             в нашей задаче можно написать функцию reduce так, чтобы
        //             выход не был большим)
    }

  private:
    using MinHeap = std::priority_queue<ipair, pairvector, Compare>;

    int mappers_count;
    mapper_func_type mapper;

    int reducers_count;
    reducer_func_type reducer;

    const char *mapper_subdir = "mapper/";
    const char *reducer_subdir = "reducer/";
    const char *shuffle_subdir = "shuffle/";

    struct Block {
        size_t from;
        size_t to;
    };

    std::vector<Block> split_file(const std::filesystem::path &file,
                                  size_t blocks_count) {
        auto size = std::filesystem::file_size(file);
        auto chunk = size / blocks_count;

        std::ifstream fobj(file);
        assert(fobj.is_open());

        std::vector<Block> out;
        size_t offset = 0;
        for (size_t i = 0; i < blocks_count; ++i) {
            std::string unused;
            auto start = offset;

            offset += chunk;
            fobj.seekg(offset, std::ios::beg);
            std::getline(fobj, unused);

            offset = fobj.tellg();
            out.push_back({start, offset});
        }

        fobj.close();
        return out;
    }

    void mapper_task(const std::filesystem::path input, Block offsets,
                     const std::filesystem::path output) {
        PrefixFindRunner::mapper_out out;
        auto id = std::this_thread::get_id();
        {
            const auto comparator =
                [](const PrefixFindRunner::mapper_chunk &a,
                   const PrefixFindRunner::mapper_chunk &b) {
                    return a.first < b.first;
                };
            std::ifstream fobj(input);
            if (!fobj.is_open()) {
                std::cout << '[' << id << "][ERROR][" << input
                          << "]Failed to open input\n";
                return;
            }

            fobj.seekg(offsets.from, std::ios::beg);
            while (static_cast<size_t>(fobj.tellg()) < offsets.to) {
                std::string line;
                std::getline(fobj, line);

                auto temp = mapper(line);
                temp.sort(comparator);
                out.merge(temp, comparator);
            }

            fobj.close();
        }

        {
            auto dir = output.parent_path();
            boost::filesystem::create_directories(dir.c_str());
            std::ofstream out_fobj(output);
            if (!out_fobj.is_open()) {
                std::cout << '[' << id << "][ERROR][" << output
                          << "]Failed to write result\n";
                return;
            }

            out_fobj.seekp(0);
            for (auto &&pair : out) {
                out_fobj << pair.first << ' ' << pair.second << '\n';
            }
            out_fobj.close();
        }

        return;
    }

    std::vector<std::filesystem::path>
    shuffle(const std::vector<std::filesystem::path> &mapped,
            const std::filesystem::path &tmp_dir,
            const std::filesystem::path &out_dir, size_t block_count) {
        std::vector<std::filesystem::path> shuffled;
        boost::filesystem::create_directories(out_dir.c_str());
        constexpr size_t readline_max = 3;

        std::vector<std::ifstream> mapped_files;
        for (auto &&path : mapped) {
            std::ifstream file(path);
            assert(file.is_open());
            mapped_files.push_back(std::move(file));
        }

        for (auto &&file : mapped_files) {
            file.close();
        }

        return std::vector<std::filesystem::path>{};
    }

    // std::string mergeFiles(size_t chunks, const std::string &merge_file) {
    //     std::ofstream ofs(merge_file.c_str());
    //     MinHeap minHeap;

    //     // array of ifstreams
    //     std::ifstream *ifs_tempfiles = new std::ifstream[chunks];

    //     for (size_t i = 1; i <= chunks; i++) {
    //         int topval = 0;

    //         // generate a unique name for temp file (temp_out_1.txt ,
    //         // temp_out_2.txt ..)
    //         std::string sorted_file =
    //             (tmp_prefix + std::to_string(i) + tmp_suffix);

    //         // open an input file stream object for each name
    //         ifs_tempfiles[i - 1].open(
    //             sorted_file.c_str());   // bind to tmp_out_{i}.txt

    //         // get val from temp file
    //         if (ifs_tempfiles[i - 1].is_open()) {
    //             ifs_tempfiles[i - 1] >>
    //                 topval;   // first value in the file (min)

    //             ipair top(topval, (i - 1));   // 2nd value is tempfile number

    //             minHeap.push(top);   //  minHeap autosorts
    //         }
    //     }
    };
