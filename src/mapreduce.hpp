#pragma once

#include <filesystem>
#include <list>
#include <utility>

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
        auto blocks = split_file(input_file, mappers_count);

        // Создаём mappers_count потоков
        // В каждом потоке читаем свой блок данных
        // Применяем к строкам данных функцию mapper
        // Сортируем результат каждого потока
        // Результат сохраняется в файловую систему (представляем, что это
        // большие данные) Каждый поток сохраняет результат в свой файл
        // (представляем, что потоки выполняются на разных узлах)

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
        // Задание творческое!
        // Я не уверен, что все вышеперечисленные требования выполнимы
        // одновременно Возможно, придётся идти на компромисс, упрощая какие-то
        // детали реализации Но это то, к чему нужно стремиться Проектирование
        // ПО часто требует идти на компромиссы Это как оптимизация функции
        // многих переменных с доп. ограничениями

        // Создаём reducers_count потоков
        // В каждом потоке читаем свой файл (выход предыдущей фазы)
        // Применяем к строкам функцию reducer
        // Результат сохраняется в файловую систему
        //             (во многих задачах выход редьюсера - большие данные, хотя
        //             в нашей задаче можно написать функцию reduce так, чтобы
        //             выход не был большим)
    }

  private:
    int mappers_count;
    mapper_func_type mapper;

    int reducers_count;
    reducer_func_type reducer;

    struct Block {
        size_t from;
        size_t to;
    };

    std::vector<Block> split_file(const std::filesystem::path &file,
                                  int blocks_count) {
        /**
         * Эта функция не читает весь файл.
         *
         * Определяем размер файла в байтах.
         * Делим размер на количество блоков - получаем границы блоков.
         * Читаем данные только вблизи границ.
         * Выравниваем границы блоков по границам строк.
         */
    }
};
