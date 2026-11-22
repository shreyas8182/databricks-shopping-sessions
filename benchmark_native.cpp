#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "usage: benchmark_native RUN DATA OUTPUT_ROOT RUNS\n";
        return 2;
    }
    try {
        const int runs = std::stoi(argv[4]);
        if (runs < 1) throw std::runtime_error("RUNS must be positive");
        const fs::path root = argv[3];
        if (!fs::create_directories(root))
            throw std::runtime_error("choose a new output root for each benchmark");
        for (int trial = 1; trial <= runs; ++trial) {
            const auto output = (root / ("run-" + std::to_string(trial))).string();
            const auto start = std::chrono::steady_clock::now();
            const pid_t child = fork();
            if (child < 0) throw std::runtime_error("could not start benchmark");
            if (child == 0) {
                execl(argv[1], argv[1], argv[2], output.c_str(), static_cast<char*>(nullptr));
                _exit(127);
            }
            int status;
            if (waitpid(child, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0)
                throw std::runtime_error("native pipeline failed on run " + std::to_string(trial));
            const auto elapsed = std::chrono::steady_clock::now() - start;
            std::cout << trial << ',' << std::chrono::duration<double>(elapsed).count() << '\n';
        }
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
