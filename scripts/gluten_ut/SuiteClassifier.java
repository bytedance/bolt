/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Discover runnable test classes under a test-classes directory, the same way
 * scalatest / surefire would, using the module's test classpath (pass it via
 * -cp; this file is meant to be run as a single-file source program:
 * `java -cp <test classpath> SuiteClassifier.java <module> <test-classes dir>`).
 *
 * Output, tab separated, one record per line:
 *   scalatest <module> <fqcn> <ntests>   concrete org.scalatest.Suite with a public
 *                                         no-arg ctor (ntests = -1 if it could not
 *                                         be instantiated to list its tests)
 *   test      <module> <fqcn> <name>     one line per test of a scalatest suite
 *   junit     <module> <fqcn>            concrete class with @org.junit.Test methods
 */
public class SuiteClassifier {
  public static void main(String[] args) throws IOException {
    String module = args[0];
    Path root = Paths.get(args[1]);
    ClassLoader loader = SuiteClassifier.class.getClassLoader();
    Class<?> suiteClass = load(loader, "org.scalatest.Suite");
    Class<?> doNotDiscover = load(loader, "org.scalatest.DoNotDiscover");
    Class<?> junitTest = load(loader, "org.junit.Test");
    Class<?> junitTestCase = load(loader, "junit.framework.TestCase");

    List<String> names;
    try (Stream<Path> files = Files.walk(root)) {
      names =
          files
              .filter(p -> p.toString().endsWith(".class"))
              .map(p -> root.relativize(p).toString())
              .filter(s -> !s.contains("$"))
              .map(s -> s.substring(0, s.length() - ".class".length()).replace('/', '.'))
              .sorted()
              .collect(Collectors.toList());
    }

    StringBuilder out = new StringBuilder();
    for (String name : names) {
      Class<?> cls;
      try {
        cls = Class.forName(name, false, loader);
      } catch (Throwable t) {
        continue; // unloadable (missing optional dependency etc.): not runnable either
      }
      int mod = cls.getModifiers();
      if (cls.isInterface()
          || Modifier.isAbstract(mod)
          || !Modifier.isPublic(mod)
          || !hasPublicNoArgCtor(cls)) {
        continue;
      }
      if (suiteClass != null && suiteClass.isAssignableFrom(cls)) {
        if (doNotDiscover != null && hasAnnotation(cls, doNotDiscover)) {
          continue;
        }
        List<String> tests = testNames(cls);
        out.append("scalatest\t")
            .append(module)
            .append('\t')
            .append(name)
            .append('\t')
            .append(tests == null ? -1 : tests.size())
            .append('\n');
        if (tests != null) {
          for (String t : tests) {
            out.append("test\t").append(module).append('\t').append(name).append('\t').append(t).append('\n');
          }
        }
      } else if (isJUnit(cls, junitTest, junitTestCase)) {
        out.append("junit\t").append(module).append('\t').append(name).append('\n');
      }
    }
    System.out.print(out);
  }

  private static Class<?> load(ClassLoader loader, String name) {
    try {
      return Class.forName(name, false, loader);
    } catch (Throwable t) {
      return null;
    }
  }

  private static boolean hasPublicNoArgCtor(Class<?> cls) {
    try {
      return Modifier.isPublic(cls.getConstructor().getModifiers());
    } catch (NoSuchMethodException | SecurityException e) {
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  private static boolean hasAnnotation(Class<?> cls, Class<?> annotation) {
    return cls.isAnnotationPresent((Class<? extends Annotation>) annotation);
  }

  @SuppressWarnings("unchecked")
  private static boolean isJUnit(Class<?> cls, Class<?> junitTest, Class<?> junitTestCase) {
    if (junitTestCase != null && junitTestCase.isAssignableFrom(cls)) {
      return true;
    }
    if (junitTest == null) {
      return false;
    }
    for (Class<?> c = cls; c != null && c != Object.class; c = c.getSuperclass()) {
      for (Method m : c.getDeclaredMethods()) {
        if (m.isAnnotationPresent((Class<? extends Annotation>) junitTest)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Instantiate the suite and list its test names; null if that is not possible. */
  private static List<String> testNames(Class<?> cls) {
    try {
      Object suite = cls.getConstructor().newInstance();
      Object set = cls.getMethod("testNames").invoke(suite);
      Object iterator = set.getClass().getMethod("iterator").invoke(set);
      Method hasNext = iterator.getClass().getMethod("hasNext");
      Method next = iterator.getClass().getMethod("next");
      hasNext.setAccessible(true);
      next.setAccessible(true);
      List<String> names = new ArrayList<>();
      while ((Boolean) hasNext.invoke(iterator)) {
        names.add(String.valueOf(next.invoke(iterator)));
      }
      return names;
    } catch (Throwable t) {
      return null;
    }
  }
}
