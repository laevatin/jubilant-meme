// test_framework.h — tiny assertion-based test runner.
// Register tests with TEST(name){...}; end the file with RUN_ALL_TESTS().
#pragma once

#include <cstdio>
#include <exception>
#include <functional>
#include <string>
#include <vector>

namespace tf {

struct Case { std::string name; std::function<void()> fn; };
inline std::vector<Case>& registry() { static std::vector<Case> r; return r; }
inline int& failures() { static int f = 0; return f; }

struct Registrar {
    Registrar(const char* name, std::function<void()> fn) { registry().push_back({name, std::move(fn)}); }
};

struct AssertFail : std::exception { std::string msg; explicit AssertFail(std::string m): msg(std::move(m)){} };

inline int run_all() {
    int failed = 0;
    for (auto& c : registry()) {
        try {
            c.fn();
            std::printf("  [ OK ] %s\n", c.name.c_str());
        } catch (const AssertFail& e) {
            std::printf("  [FAIL] %s\n         %s\n", c.name.c_str(), e.msg.c_str());
            ++failed;
        } catch (const std::exception& e) {
            std::printf("  [FAIL] %s\n         unexpected exception: %s\n", c.name.c_str(), e.what());
            ++failed;
        }
    }
    std::printf("%d/%zu passed\n", (int)registry().size() - failed, registry().size());
    return failed;
}

}  // namespace tf

#define TF_CAT2(a, b) a##b
#define TF_CAT(a, b) TF_CAT2(a, b)
#define TEST(name)                                                            \
    static void TF_CAT(tf_case_, __LINE__)();                                 \
    static ::tf::Registrar TF_CAT(tf_reg_, __LINE__)(name, &TF_CAT(tf_case_, __LINE__)); \
    static void TF_CAT(tf_case_, __LINE__)()

#define CHECK(cond)                                                           \
    do {                                                                      \
        if (!(cond))                                                          \
            throw ::tf::AssertFail(std::string(__FILE__ ":") +                \
                std::to_string(__LINE__) + "  CHECK(" #cond ")");             \
    } while (0)

#define CHECK_EQ(a, b)                                                        \
    do {                                                                      \
        auto _va = (a); auto _vb = (b);                                       \
        if (!(_va == _vb))                                                    \
            throw ::tf::AssertFail(std::string(__FILE__ ":") +                \
                std::to_string(__LINE__) + "  CHECK_EQ(" #a ", " #b ")");     \
    } while (0)

#define CHECK_THROWS(expr)                                                    \
    do {                                                                      \
        bool _threw = false;                                                  \
        try { (void)(expr); } catch (...) { _threw = true; }                  \
        if (!_threw)                                                          \
            throw ::tf::AssertFail(std::string(__FILE__ ":") +                \
                std::to_string(__LINE__) + "  CHECK_THROWS(" #expr ")");      \
    } while (0)

#define RUN_ALL_TESTS() int main() { return ::tf::run_all() == 0 ? 0 : 1; }
