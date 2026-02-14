## Do CMake Right

Please write CMake scripts in a modern style, and follow the best practices in [Effective Modern CMake](https://gist.github.com/mbinna/c61dbb39bca0e4fb7d1f73b0d66a4fd1).

## Adding a new CMake target in Bolt

### Adding a CMake target into Bolt engine,
Use the `bolt_add_library` command to add CMake target. It is a wrapper of the `add_library` command(see [here](https://github.com/bytedance/bolt/issues/139).
It will turn this target into an `OBJECT` target, and categorize and assemble it into the correct Bolt's sub-components.

Take this CMake target `bolt_config` for example:
```
# Step 1: turn bolt_config into an OBJECT target.
# Step 2: add bolt_config into bolt_engine sub-component according to its directory hierarchy.
bolt_add_library(bolt_config Config.cpp Context.cpp)

target_link_libraries(bolt_config PUBLIC Folly::folly)
```

## References
- [Effective Modern CMake](https://gist.github.com/mbinna/c61dbb39bca0e4fb7d1f73b0d66a4fd1)
- [Advanced Dependencies Model in Conan 2.0](https://www.youtube.com/watch?v=kKGglzm5ous)
- [CMake: Public VS Private VS Interface](https://leimao.github.io/blog/CMake-Public-Private-Interface/)
