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

// JShell normally exits 0 after snippet errors; keep failures nonzero.
int status = 1;
import java.nio.file.*;
import java.lang.reflect.Modifier;
import java.util.Arrays;
try {
    var module = System.getenv("UT_MODULE");
    var root = Paths.get(System.getenv("UT_CLASSES"));
    var loader = org.scalatest.Suite.class.getClassLoader();
    var paths = scala.collection.JavaConverters.asScalaBuffer(java.util.List.of(root.toString())).toList();
    // ScalaTest 3.2.16's internal discovery API; recheck on Gluten upgrades.
    var suites = org.scalatest.tools.SuiteDiscoveryHelper.discoverSuiteNames(paths, loader, scala.Option.empty()).iterator();
    while (suites.hasNext()) System.out.println("scalatest\t" + module + "\t" + suites.next());
    // Plain JUnit 3/4 classes are not included in ScalaTest's discovery.
    var junitClasses = new java.util.TreeSet<String>();
    try (var files = Files.walk(root)) {
        var it = files.filter(p -> p.toString().endsWith(".class")).iterator();
        while (it.hasNext()) {
            var name = root.relativize(it.next()).toString().replace('/', '.');
            name = name.substring(0, name.length() - ".class".length());
            if (name.contains("$")) continue;
            var cls = Class.forName(name, false, loader);
            if (org.scalatest.Suite.class.isAssignableFrom(cls) ||
                !Modifier.isPublic(cls.getModifiers()) || Modifier.isAbstract(cls.getModifiers())) continue;
            try { cls.getConstructor(); } catch (NoSuchMethodException e) { continue; }
            boolean junit = junit.framework.TestCase.class.isAssignableFrom(cls);
            for (Class<?> type = cls; !junit && type != null; type = type.getSuperclass())
                junit = Arrays.stream(type.getDeclaredMethods()).anyMatch(m -> m.isAnnotationPresent(org.junit.Test.class));
            if (junit) junitClasses.add(name);
        }
    }
    if (!junitClasses.isEmpty()) System.out.println("junit\t" + module + "\t" + String.join(",", junitClasses));
    status = 0;
} catch (Throwable error) { error.printStackTrace(); }
/exit status
