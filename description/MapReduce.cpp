#include <iostream>
#include <vector>
#include <algorithm>
#include <cassert>
#include <functional>
#include <random>
#include <fstream>
#include <cctype>
#include <sstream>
#include <iterator>
#include <map>
#include <numeric>
#include <locale>


namespace base {

  // Подсчитаем сумму квадратов чисел в парадигме MapReduce
  void map_reduce() {
    std::vector<int> input{ 1, 6, 3, 7, 2, 15, 10, 1, 3, 2 };

    // Map
    std::vector<int> map_output;
    const auto mapper = [](auto num) {
      return num * num;
    };
    std::transform(input.begin(), input.end(), std::back_inserter(map_output), mapper);

    // Reduce
    const auto reducer = [](auto res, auto num) {
      return res + num;
    };
    const auto reducer_output = std::accumulate(map_output.begin(), map_output.end(), 0, reducer);

    std::cout << "Sum of squares = " << reducer_output << std::endl;
  }


  // Не для всех задач такой подход удобен. 
  // Пусть стоит задача подсчитать количество уникальных по модулю чисел в массиве.


  void map_shuffle_reduce() {
    std::vector<int> input{ 1, 6, 3, -7, 2, 15, -10, -1, 3, 2, -3, 7, -2, 15, 10, };

    // Map
    std::vector<int> map_output;
    const auto mapper = [](auto num) {
      return std::abs(num);
    };
    std::transform(input.begin(), input.end(), std::back_inserter(map_output), mapper);

    // Shuffle
    std::sort(map_output.begin(), map_output.end());

    // Reduce
    int last_num = std::numeric_limits<int>::max();
    const auto reducer = [&last_num](auto res, auto num) {
      if (last_num == num) {
        return res;
      }
      last_num = num;
      return res + 1;
    };
    const auto reducer_output = std::accumulate(map_output.begin(), map_output.end(), 0, reducer);

    std::cout << "Number of unique by abs = " << reducer_output << std::endl;
  }

}

















namespace word_count {
  std::string input("A MapReduce program is composed of a map procedure, which performs filtering and sorting (such as sorting students by first name into queues, one queue for each name), and a reduce method, which performs a summary operation (such as counting the number of students in each queue, yielding name frequencies). The MapReduce System (also called infrastructure or framework) orchestrates the processing by marshalling the distributed servers, running the various tasks in parallel, managing all communications and data transfers between the various parts of the system, and providing for redundancy and fault tolerance. ");

  std::string prepare(const std::string& input) {
    std::string lower;
    std::transform(input.begin(), input.end(), std::back_inserter(lower), [](unsigned char c) {return std::tolower(c); });
    std::string alpha_lower;
    std::copy_if(lower.begin(), lower.end(), std::back_inserter(alpha_lower), [](unsigned char c) {return std::isalpha(c) || std::isspace(c); });
    return alpha_lower;
  }

  std::vector<std::string> split(const std::string& input) {
    std::istringstream iss(input);
    return { std::istream_iterator<std::string>(iss), {} };
  }


  
  void classical() {
    std::cout << "Classical word count" << std::endl;
    
    auto words = split(prepare(input));

    std::map<std::string, int> result;
    for(const auto& word: words) {
      if (result.find(word) == result.end()) {
        result[word] = 0;
      }
      result[word] += 1;
    }
    
    for (auto [word, count]: result) {
      std::cout << word << " " << count << std::endl;
    }
  }


  void almost_big_data() {
    std::cout << "Big data word count" << std::endl;
    auto words = split(prepare(input));

    // Map
    std::vector<std::pair<std::string /* key (word) */, int /* value (count) */>> map_output;
    const auto mapper = [](auto word) {
      return std::pair{word, 1};
    };
    std::transform(words.begin(), words.end(), std::back_inserter(map_output), mapper);

    // Shuffle
    std::sort(map_output.begin(), map_output.end());

    // Reduce
    const auto reducer = [](auto res, auto pair) {
      if (res.first == pair.first) {
        res.second += 1;
        return res;
      }
      if (res.second > 0) std::cout << res.first << " " << res.second << std::endl;
      return pair;
    };
    const auto last_word = std::accumulate(map_output.begin(), map_output.end(), std::pair<std::string, int>{"", 0}, reducer);
    std::cout << last_word.first << " " << last_word.second << std::endl;
  }

}



int main(int argc, char* argv[]) {
  base::map_reduce();
  std::cout << std::endl << std::endl;
  base::map_shuffle_reduce();
  std::cout << std::endl << std::endl;

  word_count::classical();
  std::cout << std::endl << std::endl;
  word_count::almost_big_data();
  std::cout << std::endl << std::endl;

  return 0;
}