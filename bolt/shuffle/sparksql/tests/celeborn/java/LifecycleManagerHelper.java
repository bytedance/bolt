/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;

/**
 * Standalone helper that starts a Celeborn {@link LifecycleManager} and keeps
 * it alive until an external signal (stop file) is received.
 *
 * <p>The LifecycleManager coordinates shuffle metadata between the Celeborn
 * master and the C++ shuffle tests.  It is normally launched by
 * {@code run_e2e.sh} as a background Java process.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>Connect to the Celeborn master at {@code masterEndpoints}.</li>
 *   <li>Write the LifecycleManager's own RPC endpoint ({@code host:port})
 *       to {@code endpointFile} so the C++ test processes can discover it.</li>
 *   <li>Poll for the existence of {@code stopFile} once per second.</li>
 *   <li>On stop-file creation (or JVM shutdown), gracefully stop the
 *       LifecycleManager and exit.</li>
 * </ol>
 *
 * <p>Usage:
 * <pre>
 *   java -cp &lt;classpath&gt; LifecycleManagerHelper \
 *       &lt;masterEndpoints&gt; &lt;appId&gt; &lt;endpointFile&gt; &lt;stopFile&gt;
 * </pre>
 */
public final class LifecycleManagerHelper {
  private LifecycleManagerHelper() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println(
          "Usage: LifecycleManagerHelper <masterEndpoints> <appId> <endpointFile> <stopFile>");
      System.exit(2);
    }

    final String masterEndpoints = args[0];  // e.g. "127.0.0.1:19097"
    final String appId = args[1];            // unique application ID for this test run
    final Path endpointFile = Path.of(args[2]); // output: LM endpoint for test processes
    final Path stopFile = Path.of(args[3]);     // input:  touch this file to trigger shutdown

    // Configure a minimal CelebornConf — only master endpoints and no
    // replication (single-host test setup).
    final CelebornConf conf = new CelebornConf();
    conf.set("celeborn.master.endpoints", masterEndpoints);
    conf.set("celeborn.client.push.replicate.enabled", "false");

    // Start the LifecycleManager and register a shutdown hook so it is
    // cleaned up even on unexpected JVM termination (e.g. SIGTERM).
    final LifecycleManager lifecycleManager = new LifecycleManager(appId, conf);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    lifecycleManager.stop();
                  } catch (Exception ignored) {
                  }
                }));

    // Publish the RPC endpoint so that C++ test processes can connect.
    final String endpoint = lifecycleManager.getHost() + ":" + lifecycleManager.getPort();
    Files.createDirectories(endpointFile.getParent());
    Files.writeString(endpointFile, endpoint, StandardCharsets.UTF_8);

    // Block until the controlling script (run_e2e.sh) creates the stop file.
    while (!Files.exists(stopFile)) {
      Thread.sleep(1000L);
    }

    lifecycleManager.stop();
  }
}
