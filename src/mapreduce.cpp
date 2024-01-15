#include "mapreduce.hpp"
#include "log.hpp"

namespace otus {

void
PrefixFindRunner::mapper_task(const std::filesystem::path input,
                              PrefixFindRunner::Block offsets,
                              const std::filesystem::path output) {
    PrefixFindRunner::mapper_out out;
    auto id = std::this_thread::get_id();

    std::stringstream id_stream;
    id_stream << "[MAP@" << std::hex << id << std::dec << ']';
    std::string thread_name;
    id_stream >> thread_name;

    {
        std::ifstream fobj(input);
        if (!fobj.is_open()) {
            Log::Get().Error("[{}] Failed to open input '{}'", thread_name,
                             input.c_str());
            return;
        }

        fobj.seekg(offsets.from, std::ios::beg);
        while (static_cast<size_t>(fobj.tellg()) < offsets.to) {
            std::string line;
            std::getline(fobj, line);

            auto temp = mapper(line);
            temp.sort(Compare<>());
            out.merge(temp, Compare<>());
        }

        fobj.close();
    }

    {
        auto dir = output.parent_path();
        boost::filesystem::create_directories(dir);
        std::ofstream out_fobj(output);
        if (!out_fobj.is_open()) {
            Log::Get().Error("[{}] Failed to write result '{}'", thread_name,
                             output.c_str());
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

void
PrefixFindRunner::reducer_task(const std::filesystem::path input,
                               PrefixFindRunner::Block offsets,
                               const std::filesystem::path output) {
    auto id = std::this_thread::get_id();
    std::stringstream id_stream;
    id_stream << "[RED@" << std::hex << id << std::dec << ']';
    std::string thread_name;
    id_stream >> thread_name;

    bool global_result = true;
    {
        std::ifstream fobj(input);
        if (!fobj.is_open()) {
            Log::Get().Error("[{}] Failed to open input '{}'", thread_name,
                             input.c_str());
            return;
        }

        fobj.seekg(offsets.from, std::ios::beg);
        while (static_cast<size_t>(fobj.tellg()) < offsets.to) {
            std::string line;
            std::getline(fobj, line);

            auto separator = line.find(' ');
            std::string token = line.substr(0, separator);
            int repeats =
                std::atoi(line.substr(separator + 1, line.find('\n')).c_str());
            global_result &= reducer({token, repeats});
        }

        fobj.close();
    }

    {
        auto dir = output.parent_path();
        boost::filesystem::create_directories(dir);
        std::ofstream out_fobj(output);
        if (!out_fobj.is_open()) {
            Log::Get().Error("[{}] Failed to write result '{}'", thread_name,
                             output.c_str());
            return;
        }

        out_fobj.seekp(0);
        out_fobj << global_result << '\n';
        out_fobj.close();
    }

    return;
}

std::vector<PrefixFindRunner::Block>
PrefixFindRunner::split_file(const std::filesystem::path &file,
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

        auto point = fobj.tellg();
        offset = point < 0 ? offset : static_cast<size_t>(point);
        out.push_back({start, offset});
    }

    fobj.close();
    return out;
}

std::vector<PrefixFindRunner::Block>
PrefixFindRunner::shuffle(const std::vector<std::filesystem::path> &mapped,
                          const std::filesystem::path &tmp_file,
                          size_t block_count) {
    std::vector<std::ifstream> mapped_files;
    for (auto &&path : mapped) {
        std::ifstream file(path);
        assert(file.is_open());
        mapped_files.push_back(std::move(file));
    }

    // Actual external merge
    {
        std::priority_queue<mapper_chunk, std::vector<mapper_chunk>,
                            Compare<true>>
            heap;
        std::ofstream out_fobj(tmp_file);
        assert(out_fobj.is_open());

        for (size_t i = 0; i < mapped_files.size(); ++i) {
            std::string top_value;
            std::getline(mapped_files[i], top_value);
            // top_value = top_value.substr(0, top_value.find(' '));

            mapper_chunk top{top_value, i};
            heap.push(top);
        }

        out_fobj.seekp(0);
        while (heap.size() > 0) {
            std::string next_value;

            auto min = heap.top();
            heap.pop();

            out_fobj << min.first << std::endl;
            if (std::getline(mapped_files[min.second], next_value)) {
                mapper_chunk next{next_value, min.second};
                heap.push(next);
            }
        }

        out_fobj.close();
    }

    for (auto &&file : mapped_files) {
        file.close();
    }

    return split_file(tmp_file, block_count);
}

std::vector<PrefixFindRunner::Block>
PrefixFindRunner::align_blocks(const std::filesystem::path &path,
                               const std::vector<Block> &blocks) {

    std::ifstream fobj(path);
    assert(fobj.is_open());

    std::vector<Block> out;
    size_t new_start_offset = blocks[0].from;
    size_t new_end_offset = blocks[0].to;
    for (size_t i = 0; i < blocks.size(); ++i) {
        bool is_unique = false;
        std::string previous;
        new_end_offset = blocks[i].to;

        fobj.clear();
        fobj.seekg(new_end_offset, std::ios::beg);
        Log::Get().Debug("Old tail 0x{:X}", new_end_offset);

        auto step_back = [](std::ifstream &stream) {
            char rewind_char = '\n';
            stream.seekg(-1, std::ios::cur);
            stream.get(rewind_char);
            stream.seekg(-1, std::ios::cur);
            return rewind_char;
        };

        char cur_char = '_';
        step_back(fobj);
        step_back(fobj);   // Skip the last \n
        while (cur_char != '\n') {
            if (fobj.eof() || fobj.tellg() == 0) {
                break;
            }
            cur_char = step_back(fobj);
            // std::cout << cur_char;
        }

        auto cur_pos = fobj.tellg();
        if (cur_pos > 0) {
            new_end_offset = static_cast<size_t>(cur_pos);
            ++new_end_offset;
        }

        fobj.clear();
        fobj.seekg(new_end_offset, std::ios::beg);
        Log::Get().Debug("Aligned tail 0x{:X}", new_end_offset);

        std::getline(fobj, previous);
        while (!is_unique && !fobj.eof()) {
            std::string current;
            new_end_offset = fobj.tellg();
            std::getline(fobj, current);

            if (current.compare(previous) != 0) {
                is_unique = true;
                break;
            }

            previous = current;
        }

        out.push_back({new_start_offset, new_end_offset});
        Log::Get().Debug("Aligned lines {}...{}", new_start_offset / 4,
                         new_end_offset / 4);
        new_start_offset = new_end_offset;
    }

    fobj.close();
    return out;
}

}   // namespace otus
