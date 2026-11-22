#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

void run(std::vector<std::string> args) {
    std::vector<char*> command;
    for (auto& arg : args) command.push_back(arg.data());
    command.push_back(nullptr);
    const pid_t child = fork();
    if (child < 0) throw std::runtime_error("fork failed");
    if (child == 0) {
        execvp(command.front(), command.data());
        _exit(127);
    }
    int status;
    pid_t result;
    do { result = waitpid(child, &status, 0); } while (result < 0 && errno == EINTR);
    if (result < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0)
        throw std::runtime_error(args.front() + " failed");
}

std::string literal(const fs::path& path) {
    std::string value = "'";
    for (char c : path.string()) {
        value += c;
        if (c == '\'') value += c;
    }
    return value + "'";
}

void compare(const fs::path& output, const fs::path& expected) {
    const char* cli = std::getenv("DUCKDB");
    std::string query;
    for (const std::string table : {"user_daily", "campaign_daily"}) {
        const std::string key = table == "user_daily" ? "user_id" : "campaign_id";
        query += "CREATE OR REPLACE VIEW actual AS SELECT * FROM read_parquet("
              + literal(output / table / "*.parquet") + ");"
                "CREATE OR REPLACE VIEW expected AS SELECT * FROM read_csv("
              + literal(expected / (table + ".csv")) + ", header=true, all_varchar=true);";
        query += "SELECT CASE WHEN EXISTS (SELECT " + key + ", day FROM actual GROUP BY ALL HAVING count(*) > 1)"
                 " OR EXISTS (SELECT * FROM (SELECT COLUMNS(*)::VARCHAR FROM actual EXCEPT ALL SELECT * FROM expected))"
                 " OR EXISTS (SELECT * FROM (SELECT * FROM expected EXCEPT ALL SELECT COLUMNS(*)::VARCHAR FROM actual))"
                 " THEN error('" + table + " differs') ELSE '" + table + " matches' END;";
    }
    run({cli ? cli : "duckdb", "-batch", "-bail", "-noheader", "-list", "-c", query});
}

int main(int argc, char** argv) {
    try {
        if (argc != 4) throw std::runtime_error("usage: check_native compare OUTPUT EXPECTED | fixtures FIXTURES RUNNER");
        if (std::string(argv[1]) == "compare") {
            compare(argv[2], argv[3]);
        } else if (std::string(argv[1]) == "fixtures") {
            unsigned count = 0;
            for (const auto& entry : fs::directory_iterator(argv[2])) {
                if (!entry.is_directory()) continue;
                const auto case_path = fs::absolute(entry.path());
                const auto input = fs::is_directory(case_path / "data") ? case_path / "data" : case_path / "input";
                std::string pattern = (fs::temp_directory_path() / "shopping-check-XXXXXX").string();
                if (!mkdtemp(pattern.data())) throw std::runtime_error("could not create fixture output");
                try {
                    run({argv[3], input.string(), pattern});
                    compare(pattern, case_path / "expected");
                } catch (...) {
                    fs::remove_all(pattern);
                    std::cerr << "Fixture failed: " << case_path.filename() << '\n';
                    throw;
                }
                fs::remove_all(pattern);
                ++count;
            }
            if (!count) throw std::runtime_error("no fixtures found");
            std::cout << count << " fixtures passed\n";
        } else throw std::runtime_error("expected compare or fixtures mode");
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
