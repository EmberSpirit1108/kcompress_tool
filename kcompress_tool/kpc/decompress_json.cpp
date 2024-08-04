#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <zstd.h>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <unistd.h>

#define DEFAULT_BUFFER_SIZE 4 * 1024 * 1024  // 预分配4MB缓冲区

// 计算百分位数的函数
double percentile(const std::vector<double>& data, double perc) {
    size_t n = data.size();
    double rank = perc * (n - 1);
    size_t lower_index = static_cast<size_t>(rank);
    size_t upper_index = std::min(lower_index + 1, n - 1);
    double weight = rank - lower_index;
    return data[lower_index] * (1 - weight) + data[upper_index] * weight;
}

// 获取当前内存使用情况的函数
long get_memory_usage() {
    std::ifstream statm_file("/proc/self/statm");
    long size = 0;
    if (statm_file.is_open()) {
        statm_file >> size;
        statm_file.close();
    }
    return size * sysconf(_SC_PAGESIZE); // 返回字节数
}

int decompress_file(const std::string& input_file, const std::string& output_file, const std::string& dict_file) {
    // 获取初始内存使用情况
    long initial_mem_usage = get_memory_usage();
    
    std::ifstream ifs(input_file, std::ios::binary);
    std::ofstream ofs(output_file, std::ios::binary);

    if (!ifs.is_open() || !ofs.is_open()) {
        std::cerr << "Error opening input or output file." << std::endl;
        return -1;
    }

    // 加载字典
    std::ifstream dict_fp(dict_file, std::ios::binary);
    if (!dict_fp.is_open()) {
        std::cerr << "Error opening dictionary file." << std::endl;
        return -1;
    }

    dict_fp.seekg(0, std::ios::end);
    size_t dict_size = dict_fp.tellg();
    dict_fp.seekg(0, std::ios::beg);

    std::vector<char> dict_buffer(dict_size);
    dict_fp.read(dict_buffer.data(), dict_size);
    dict_fp.close();

    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    ZSTD_DDict* ddict = ZSTD_createDDict(dict_buffer.data(), dict_size);

    if (ddict == nullptr || dctx == nullptr) {
        std::cerr << "Failed to create ZSTD context or dictionary." << std::endl;
        if (ddict) ZSTD_freeDDict(ddict);
        if (dctx) ZSTD_freeDCtx(dctx);
        return -1;
    }

    // 预分配4MB缓冲区
    std::vector<char> decompressed_data(DEFAULT_BUFFER_SIZE);

    std::vector<double> decompression_times;
    size_t line_count = 0;


    while (ifs.peek() != EOF) {
        char flag;
        ifs.read(&flag, sizeof(flag));

        auto start = std::chrono::high_resolution_clock::now();

        if (flag == 0) {
            // 处理未压缩的数据
            size_t line_len;
            ifs.read(reinterpret_cast<char*>(&line_len), sizeof(line_len));

            std::vector<char> uncompressed_data(line_len);
            ifs.read(uncompressed_data.data(), line_len);

            ofs.write(uncompressed_data.data(), line_len);
            ofs.write("\n", 1);  // 添加换行符
        } else if (flag == 1) {
            // 处理压缩的数据
            size_t compressed_size;
            ifs.read(reinterpret_cast<char*>(&compressed_size), sizeof(compressed_size));

            std::vector<char> compressed_data(compressed_size);
            ifs.read(compressed_data.data(), compressed_size);

            size_t actual_decompressed_size = ZSTD_decompress_usingDDict(dctx, decompressed_data.data(), decompressed_data.size(), compressed_data.data(), compressed_size, ddict);
            if (ZSTD_isError(actual_decompressed_size)) {
                std::cerr << "Decompression error: " << ZSTD_getErrorName(actual_decompressed_size) << std::endl;
                ZSTD_freeDDict(ddict);
                ZSTD_freeDCtx(dctx);
                return -1;
            }

            ofs.write(decompressed_data.data(), actual_decompressed_size);
            ofs.write("\n", 1);  // 添加换行符
        } else {
            std::cerr << "Unknown data flag: " << static_cast<int>(flag) << std::endl;
            ZSTD_freeDDict(ddict);
            ZSTD_freeDCtx(dctx);
            return -1;
        }

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;
        decompression_times.push_back(duration.count());
        line_count++;
    }

    ZSTD_freeDDict(ddict);
    ZSTD_freeDCtx(dctx);

    ifs.close();
    ofs.close();

    // 获取最终内存使用情况
    long final_mem_usage = get_memory_usage();
    long memory_usage = final_mem_usage - initial_mem_usage;

    double total_time = std::accumulate(decompression_times.begin(), decompression_times.end(), 0.0);
    double average_time = total_time / line_count;

    std::sort(decompression_times.begin(), decompression_times.end());

    std::cout << "Total lines processed: " << line_count << std::endl;
    std::cout << "Total decompression time (ms): " << total_time << std::endl;
    std::cout << "Average decompression time (ms): " << average_time << std::endl;
    std::cout << "Decompression time - Min: " << decompression_times.front() << " ms" << std::endl;
    std::cout << "Decompression time - P25: " << percentile(decompression_times, 0.25) << " ms" << std::endl;
    std::cout << "Decompression time - P50: " << percentile(decompression_times, 0.5) << " ms" << std::endl;
    std::cout << "Decompression time - P75: " << percentile(decompression_times, 0.75) << " ms" << std::endl;
    std::cout << "Decompression time - P95: " << percentile(decompression_times, 0.95) << " ms" << std::endl;
    std::cout << "Decompression time - Max: " << decompression_times.back() << " ms" << std::endl;
    std::cout << "Memory usage (approx.): " << memory_usage << " bytes" << std::endl;

    return 0;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_file> <output_file>" << std::endl;
        return -1;
    }

    std::string input_file = argv[1];
    std::string output_file = argv[2];
    std::string dict_file = "kpc/kpc_dictionary.dict";  // 使用相对路径

    auto overall_start = std::chrono::high_resolution_clock::now(); // 开始计时

    int result = decompress_file(input_file, output_file, dict_file);

    auto overall_end = std::chrono::high_resolution_clock::now(); // 结束计时
    std::chrono::duration<double, std::milli> overall_duration = overall_end - overall_start;
    std::cout << "Overall decompression time (ms): " << overall_duration.count() << std::endl;

    if (result == 0) {
        std::cout << "File successfully decompressed to " << output_file << std::endl;
    } else {
        std::cout << "File decompression failed." << std::endl;
    }

    return result;
}
