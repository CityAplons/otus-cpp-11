#pragma once

#include <format>
#include <string>
#include <iostream>

#include <unistd.h>

namespace otus
{

class Log
{
public:
  enum Severity
  {
    ERROR,
    WARN,
    INFO,
    DEBUG
  };

  static Log& Get()
  {
    static Log instance;
    return instance;
  }

  void SetSeverity(Severity level)
  {
    level_ = level;
  }

  template <typename... Args>
  static std::string UnfoldFormat(std::string_view fmt, Args&&... args)
  {
    return std::vformat(fmt, std::make_format_args(args...));
  }

  template <typename... Args>
  void Debug(std::string_view fmt, Args&&... args) noexcept
  {
    if (level_ < Severity::DEBUG)
      return;

    std::cout << "[DEBUG]" << UnfoldFormat(fmt, args...) << "\n";
  }

  template <typename... Args>
  void Info(std::string_view fmt, Args&&... args) noexcept
  {
    if (level_ < Severity::INFO)
      return;

    std::cout << "[INFO]" << UnfoldFormat(fmt, args...) << "\n";
  }

  template <typename... Args>
  void Warn(std::string_view fmt, Args&&... args) noexcept
  {
    if (level_ < Severity::WARN)
      return;

    std::cout << "[WARN]" << UnfoldFormat(fmt, args...) << "\n";
  }

  template <typename... Args>
  void Error(std::string_view fmt, Args&&... args) noexcept
  {
    std::cout << "[ERROR]" << UnfoldFormat(fmt, args...) << "\n";
  }

  bool SetSeverityFromArgs(int argc, char* const argv[])
  {
    int opt, parsed;
    while ((opt = getopt(argc, argv, "d:")) != -1)
    {
      switch (opt)
      {
        case 'd':
          parsed = std::atoi(optarg);
          break;

        default:
          break;
      }
    }

    if (parsed <= otus::Log::Severity::DEBUG && parsed >= otus::Log::Severity::ERROR)
    {
      level_ = static_cast<otus::Log::Severity>(parsed);
      return true;
    }

    return false;
  }

private:
  Severity level_ = Severity::WARN;

  Log() = default;
  ~Log() = default;

  Log(const Log& root) = delete;
  Log& operator=(const Log&) = delete;
  Log(Log&& root) = delete;
  Log& operator=(Log&&) = delete;
};

}  // namespace otus
