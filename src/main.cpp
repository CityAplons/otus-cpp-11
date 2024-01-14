#include <format>
#include <iostream>

#include <boost/program_options.hpp>

#include "mapreduce.hpp"
#include "project.h"

int
main(int argc, char const *argv[]) {
    struct ProjectInfo info = {};
    std::cout << info.nameString << "\t" << info.versionString << '\n';

    namespace po = boost::program_options;
    po::options_description desc(
        "Find prefixes from lines of file using mapreduce technique");
    desc.add_options()("help", "Produce this help message")(
        "input,i", po::value<std::string>(), "Input filename to process")(
        "mappers,m", po::value<int>(), "Amount of the mappers threads")(
        "reducers,r", po::value<int>(), "Amount of the reducers threads");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 1;
    }

    std::string file;
    if (!vm.count("input")) {
        std::cout << "No input was provided!\n" << desc << "\n";
        return 2;
    }
    file = vm["input"].as<std::string>();

    int mp_count = 3, rd_count = 2;
    if (vm.count("mappers") && vm.count("reducers")) {
        mp_count = vm["mappers"].as<int>();
        rd_count = vm["reducers"].as<int>();
    }

    std::cout << std::format("Using {} mappers, {} reducers with \"{}\" file",
                             mp_count, rd_count, file)
              << "\n";
    PrefixFindRunner mr(mp_count, rd_count);

    bool is_unique_found = false;
    unsigned result = 1;
    while (!is_unique_found) {
        mr.set_mapper([result](const std::string &line) {
            PrefixFindRunner::mapper_out out;
            std::string sub_string{};

            for (size_t i = 0; i < line.size() && i < result; ++i) {
                sub_string += line[i];
                out.push_back({sub_string, 1});
            }

            const auto comparator =
                [](const PrefixFindRunner::mapper_chunk &a,
                   const PrefixFindRunner::mapper_chunk &b) {
                    return a.first < b.first;
                };
            out.sort(comparator);
            return out;
        });
        mr.set_reducer([](const PrefixFindRunner::mapper_chunk &chunk) {
            static PrefixFindRunner::mapper_chunk previous;
            if (!chunk.first.compare(previous.first) || chunk.second > 1) {
                return false;
            }

            return true;
        });

        mr.run(file, std::format("out/iter{}", result));
        result++;
    }

    return 0;
}
