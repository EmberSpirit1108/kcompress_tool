#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>

void print_difference_with_context(const std::string& line1, const std::string& line2, int line_number, size_t char_position) {
    const int context_range = 10;
    size_t start = (char_position > context_range) ? char_position - context_range - 1 : 0;
    size_t end1 = std::min(char_position + context_range, line1.length());
    size_t end2 = std::min(char_position + context_range, line2.length());

    std::string context1 = line1.substr(start, end1 - start);
    std::string context2 = line2.substr(start, end2 - start);

    std::cout << "Difference found at line " << line_number 
              << ", character position " << char_position << ":\n";
    std::cout << "File 1 context: \"" << context1 << "\"\n";
    std::cout << "File 2 context: \"" << context2 << "\"\n\n";
}

bool compare_files(const std::string& file1, const std::string& file2) {
    std::ifstream ifs1(file1, std::ios::binary);
    std::ifstream ifs2(file2, std::ios::binary);

    if (!ifs1.is_open() || !ifs2.is_open()) {
        std::cerr << "Error opening one of the files." << std::endl;
        return false;
    }

    std::string line1, line2;
    int line_number = 0;
    int differences_found = 0;

    while (std::getline(ifs1, line1) && std::getline(ifs2, line2)) {
        line_number++;
        // Remove potential \r for Windows-style line endings
        if (!line1.empty() && line1.back() == '\r') line1.pop_back();
        if (!line2.empty() && line2.back() == '\r') line2.pop_back();

        size_t len = std::min(line1.length(), line2.length());
        for (size_t i = 0; i < len; ++i) {
            if (line1[i] != line2[i]) {
                print_difference_with_context(line1, line2, line_number, i + 1);
                differences_found++;
                if (differences_found == 2) {
                    return false; // Stop after finding two differences
                }
            }
        }

        // Handle case where one line is longer than the other
        if (line1.length() != line2.length() && differences_found < 2) {
            size_t longer_len = std::max(line1.length(), line2.length());
            for (size_t i = len; i < longer_len; ++i) {
                char char1 = (i < line1.length()) ? line1[i] : '\0';
                char char2 = (i < line2.length()) ? line2[i] : '\0';
                print_difference_with_context(line1, line2, line_number, i + 1);
                differences_found++;
                if (differences_found == 2) {
                    return false; // Stop after finding two differences
                }
            }
        }
    }

    // Check if one file has extra lines
    while (std::getline(ifs1, line1) && differences_found < 2) {
        line_number++;
        for (size_t i = 0; i < line1.length(); ++i) {
            print_difference_with_context(line1, "", line_number, i + 1);
            differences_found++;
            if (differences_found == 2) {
                return false; // Stop after finding two differences
            }
        }
    }

    while (std::getline(ifs2, line2) && differences_found < 2) {
        line_number++;
        for (size_t i = 0; i < line2.length(); ++i) {
            print_difference_with_context("", line2, line_number, i + 1);
            differences_found++;
            if (differences_found == 2) {
                return false; // Stop after finding two differences
            }
        }
    }

    return differences_found == 0; // Return true if no differences were found
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <file1> <file2>" << std::endl;
        return 1;
    }

    std::string file1 = argv[1];
    std::string file2 = argv[2];

    if (compare_files(file1, file2)) {
        std::cout << "Files are identical." << std::endl;
    } else {
        std::cout << "Files are not identical." << std::endl;
    }

    return 0;
}
