#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <zstd.h>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <unistd.h>

#define CHUNK_SIZE 10 * 1024 * 1024  // 每次读取的最大字节数
#define LEVEL1_THRESHOLD 1024        // 小于1KB不压缩
#define LEVEL2_THRESHOLD 2048        // 1KB-2KB使用压缩等级2
#define LEVEL3_THRESHOLD 10240       // 2KB-10KB使用压缩等级3
#define LEVEL5_THRESHOLD 10240       // 超过10KB使用压缩等级5

double calculate_total(const std::vector<double>& data) {
    return std::accumulate(data.begin(), data.end(), 0.0);
}

double percentile(const std::vector<double>& data, double perc) {
    size_t n = data.size();
    double rank = perc * (n - 1);
    size_t lower_index = static_cast<size_t>(rank);
    size_t upper_index = std::min(lower_index + 1, n - 1);
    double weight = rank - lower_index;
    return data[lower_index] * (1 - weight) + data[upper_index] * weight;
}

long get_memory_usage() {
    std::ifstream statm_file("/proc/self/statm");
    long size = 0;
    if (statm_file.is_open()) {
        statm_file >> size;
        statm_file.close();
    }
    return size * sysconf(_SC_PAGESIZE); // 返回字节数
}

int compress_file(const std::string& input_file, const std::string& output_file, const std::string& dict_file) {
    std::ifstream input_fp(input_file);
    std::ofstream output_fp(output_file, std::ios::binary);
    if (!input_fp.is_open() || !output_fp.is_open()) {
        std::cerr << "Error opening input or output file." << std::endl;
        return 1;
    }

    // 加载字典
    std::ifstream dict_fp(dict_file, std::ios::binary);
    if (!dict_fp.is_open()) {
        std::cerr << "Error opening dictionary file." << std::endl;
        return 1;
    }

    dict_fp.seekg(0, std::ios::end);
    size_t dict_size = dict_fp.tellg();
    dict_fp.seekg(0, std::ios::beg);

    std::vector<char> dict_buffer(dict_size);
    dict_fp.read(dict_buffer.data(), dict_size);
    dict_fp.close();

    ZSTD_CCtx* cctx = ZSTD_createCCtx();
    ZSTD_CDict* cdict = ZSTD_createCDict(dict_buffer.data(), dict_size, 2); // 预先加载字典

    if (cdict == nullptr || cctx == nullptr) {
        std::cerr << "Failed to create ZSTD context or load dictionary." << std::endl;
        if (cdict) ZSTD_freeCDict(cdict);
        if (cctx) ZSTD_freeCCtx(cctx);
        return 1;
    }

    // 预分配一个较大的压缩缓冲区
    size_t max_compress_size = ZSTD_compressBound(CHUNK_SIZE);
    std::vector<char> compress_buffer(max_compress_size);

    std::vector<double> compression_times;
    std::vector<double> compression_ratios;
    size_t total_input_size = 0;
    size_t total_compressed_size = 0;
    size_t line_count = 0;

    // 获取初始内存使用情况
    long initial_mem_usage = get_memory_usage();

    std::string line;
    while (std::getline(input_fp, line)) {
        line_count++;
        size_t line_len = line.size();

        if (line_len <= LEVEL1_THRESHOLD) {
            // 如果JSON数据太小，不进行压缩，直接标记并输出
            char flag = 0;  // 未压缩数据的标记
            output_fp.write(&flag, sizeof(flag));
            output_fp.write(reinterpret_cast<char*>(&line_len), sizeof(line_len));
            output_fp.write(line.data(), line_len);
            continue;
        }

        int compression_level = 1;  // 默认压缩等级1
        if (line_len >= LEVEL1_THRESHOLD && line_len < LEVEL2_THRESHOLD) {
            compression_level = 2;
        } else if (line_len >= LEVEL2_THRESHOLD && line_len < LEVEL3_THRESHOLD) {
            compression_level = 3;
        } else if (line_len >= LEVEL3_THRESHOLD) {
            compression_level = 5;
        }

        auto start = std::chrono::high_resolution_clock::now();

        ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, compression_level);
        size_t compressed_size = ZSTD_compress_usingCDict(cctx, compress_buffer.data(), compress_buffer.size(), line.data(), line_len, cdict);

        if (ZSTD_isError(compressed_size)) {
            std::cerr << "Compression error: " << ZSTD_getErrorName(compressed_size) << std::endl;
            continue;
        }

        auto end = std::chrono::high_resolution_clock::now();
        double duration = std::chrono::duration<double, std::milli>(end - start).count();
        compression_times.push_back(duration);

        double compression_ratio = static_cast<double>(compressed_size) / line_len;
        compression_ratios.push_back(compression_ratio);

        total_input_size += line_len;
        total_compressed_size += compressed_size;

        // 压缩数据块标记
        char flag = 1;  // 压缩数据的标记
        output_fp.write(&flag, sizeof(flag));
        output_fp.write(reinterpret_cast<char*>(&compressed_size), sizeof(compressed_size));  // 写入压缩块大小
        output_fp.write(compress_buffer.data(), compressed_size);  // 写入压缩数据
    }

    ZSTD_freeCDict(cdict);
    ZSTD_freeCCtx(cctx);

    input_fp.close();
    output_fp.close();

    // 获取最终内存使用情况
    long final_mem_usage = get_memory_usage();
    long memory_usage = final_mem_usage - initial_mem_usage;

    // 输出统计信息
    double total_time = calculate_total(compression_times);
    double average_time = total_time / line_count;
    double average_compression_ratio = static_cast<double>(total_compressed_size) / total_input_size;

    std::sort(compression_times.begin(), compression_times.end());
    std::sort(compression_ratios.begin(), compression_ratios.end());

    std::cout << "Total lines processed: " << line_count << std::endl;
    std::cout << "Total compression time (ms): " << total_time << std::endl;
    std::cout << "Average compression time (ms): " << average_time << std::endl;
    std::cout << "Compression time - Min: " << compression_times.front() << " ms" << std::endl;
    std::cout << "Compression time - P25: " << percentile(compression_times, 0.25) << " ms" << std::endl;
    std::cout << "Compression time - P50: " << percentile(compression_times, 0.5) << " ms" << std::endl;
    std::cout << "Compression time - P75: " << percentile(compression_times, 0.75) << " ms" << std::endl;
    std::cout << "Compression time - P95: " << percentile(compression_times, 0.95) << " ms" << std::endl;
    std::cout << "Compression time - Max: " << compression_times.back() << " ms" << std::endl;
    std::cout << "Average compression ratio: " << average_compression_ratio << std::endl;
    std::cout << "Compression ratio - Min: " << compression_ratios.front() << std::endl;
    std::cout << "Compression ratio - P25: " << percentile(compression_ratios, 0.25) << std::endl;
    std::cout << "Compression ratio - P50: " << percentile(compression_ratios, 0.5) << std::endl;
    std::cout << "Compression ratio - P75: " << percentile(compression_ratios, 0.75) << std::endl;
    std::cout << "Compression ratio - P95: " << percentile(compression_ratios, 0.95) << std::endl;
    std::cout << "Compression ratio - Max: " << compression_ratios.back() << std::endl;
    std::cout << "Memory usage: " << memory_usage << " bytes" << std::endl;

    return 0;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_file> <output_file>" << std::endl;
        return 1;
    }

    std::string input_file = argv[1];
    std::string output_file = argv[2];
    std::string dict_file = "kpc/kpc_dictionary.dict";  // 使用相对路径加载字典

    int result = compress_file(input_file, output_file, dict_file);
    if (result == 0) {
        std::cout << "File successfully compressed to " << output_file << std::endl;
    } else {
        std::cout << "File compression failed." << std::endl;
    }

    return result;
}
