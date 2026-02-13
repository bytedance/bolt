import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;

public final class LifecycleManagerHelper {
  private LifecycleManagerHelper() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println(
          "Usage: LifecycleManagerHelper <masterEndpoints> <appId> <endpointFile> <stopFile>");
      System.exit(2);
    }

    final String masterEndpoints = args[0];
    final String appId = args[1];
    final Path endpointFile = Path.of(args[2]);
    final Path stopFile = Path.of(args[3]);

    final CelebornConf conf = new CelebornConf();
    conf.set("celeborn.master.endpoints", masterEndpoints);
    conf.set("celeborn.client.push.replicate.enabled", "false");

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

    final String endpoint = lifecycleManager.getHost() + ":" + lifecycleManager.getPort();
    Files.createDirectories(endpointFile.getParent());
    Files.writeString(endpointFile, endpoint, StandardCharsets.UTF_8);

    while (!Files.exists(stopFile)) {
      Thread.sleep(1000L);
    }

    lifecycleManager.stop();
  }
}
