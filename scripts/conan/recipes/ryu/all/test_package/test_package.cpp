#include <ryu/ryu.h>
#include <iostream>

int main() {
  std::cout << "Testing ryu library..." << std::endl;
  double value = 12345.6789;
  char buffer[32];
  int length = d2s_buffered_n(value, buffer);
  buffer[length] = '\0'; // Null-terminate the string
  std::cout << "Double value: " << value << " -> String: " << buffer
            << std::endl;
  return 0;
}