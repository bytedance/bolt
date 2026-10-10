// Copyright (c) ByteDance Ltd. and/or its affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeSet;
import org.scalatest.DoNotDiscover;
import org.scalatest.Suite;
import org.scalatest.WrapWith;

/** Discover suites without constructing them or starting their Spark sessions. */
class DiscoverGlutenSuites {
  public static void main(String[] args) throws Exception {
    TreeSet<String> names = new TreeSet<>();
    for (int i = 1; i < args.length; i++) {
      discover(Path.of(args[i]), names);
    }
    Files.write(Path.of(args[0]), names, StandardCharsets.UTF_8);
  }

  private static void discover(Path root, TreeSet<String> names) throws Exception {
    try (var files = Files.walk(root)) {
      for (Path file : files.filter(p -> p.toString().endsWith(".class")).toList()) {
        String name = root.relativize(file).toString().replace('/', '.');
        name = name.substring(0, name.length() - ".class".length());
        Class<?> type = Class.forName(name, false, DiscoverGlutenSuites.class.getClassLoader());
        if (type.isAnnotationPresent(DoNotDiscover.class)) {
          continue;
        }
        if (isAccessibleSuite(type) || isRunnable(type)) {
          names.add(name);
        }
      }
    }
  }

  private static boolean isAccessibleSuite(Class<?> type) {
    int modifiers = type.getModifiers();
    if (!Suite.class.isAssignableFrom(type) || !Modifier.isPublic(modifiers)
        || Modifier.isAbstract(modifiers)) {
      return false;
    }
    try {
      type.getConstructor();
      return true;
    } catch (NoSuchMethodException ignored) {
      return false;
    }
  }

  private static boolean isRunnable(Class<?> type) {
    WrapWith wrapper = type.getAnnotation(WrapWith.class);
    if (wrapper == null) {
      return false;
    }
    for (var constructor : wrapper.value().getDeclaredConstructors()) {
      Class<?>[] parameters = constructor.getParameterTypes();
      if (parameters.length == 1 && parameters[0] == Class.class) {
        return true;
      }
    }
    return false;
  }
}
