#include <iostream>

#include "project.h"

int main(int argc, char const *argv[]) {
  struct ProjectInfo info = {};
  std::cout << info.nameString << "\t" << info.versionString << '\n';

  (void)argc;
  (void)argv;
  return 0;
}
