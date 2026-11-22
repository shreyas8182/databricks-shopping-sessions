#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

static std::string env(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value ? value : fallback;
}

static std::string window_field(const std::string& manifest, const char* name) {
    // The track manifest supplies two top-level ASCII timestamp strings.
    auto position = manifest.find(std::string("\"") + name + "\"");
    if (position != std::string::npos) {
        position = manifest.find_first_not_of(" \t\r\n", position + std::char_traits<char>::length(name) + 2);
        if (position != std::string::npos && manifest[position] == ':') {
            position = manifest.find_first_not_of(" \t\r\n", position + 1);
            if (position != std::string::npos && manifest[position] == '"') {
                auto end = manifest.find('"', position + 1);
                if (end != std::string::npos) return manifest.substr(position + 1, end - position - 1);
            }
        }
    }
    throw std::runtime_error(std::string("missing manifest string: ") + name);
}

static void launch(std::vector<std::string> args) {
    std::vector<char*> argv;
    for (auto& arg : args) argv.push_back(arg.data());
    argv.push_back(nullptr);
    execv(argv[0], argv.data());
    throw std::runtime_error("could not launch " + args[0]);
}

int main(int argc, char** argv) {
    try {
#if defined(__linux__)
        const auto bin = fs::canonical("/proc/self/exe").parent_path();
#else
        const auto bin = fs::canonical(argv[0]).parent_path();
#endif
        fs::path executable = bin / "native_sweep";
#if defined(__linux__) && defined(__x86_64__)
        const auto avx = bin / "native_sweep_avx2";
        if (__builtin_cpu_supports("avx2") && __builtin_cpu_supports("bmi") &&
            __builtin_cpu_supports("bmi2") && __builtin_cpu_supports("popcnt") &&
            access(avx.c_str(), X_OK) == 0) executable = avx;
#endif
        std::vector<std::string> paths;
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            // Accepted for callers of the earlier CLI. The C++ engine
            // publishes temporary Parquet files in the output directory.
            if (arg == "--scratch" && i + 1 < argc) { ++i; continue; }
            if (arg.rfind("--scratch=", 0) == 0) continue;
            if (arg == "--help" || arg == "-h") {
                std::cout << "Usage: run [DATA_DIR [OUTPUT_DIR]] [--scratch DIR]\n";
                return 0;
            }
            if (paths.size() == 2 || (!arg.empty() && arg.front() == '-'))
                throw std::runtime_error("usage: run [DATA_DIR [OUTPUT_DIR]] [--scratch DIR]");
            paths.push_back(arg);
        }
        auto data = paths.empty() ? env("DATA_DIR", "/data") : paths[0];
        auto output = paths.size() < 2 ? env("OUTPUT_DIR", "/output") : paths[1];
        std::ifstream stream(fs::path(data) / "manifest.json");
        if (!stream) throw std::runtime_error("could not open manifest.json");
        std::string manifest((std::istreambuf_iterator<char>(stream)), {});
        launch({executable.string(), data, output, window_field(manifest, "window_start"),
                window_field(manifest, "window_end"), env("NCPU", "8")});
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
