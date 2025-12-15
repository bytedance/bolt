#include <iostream>
#include "celeborn/conf/CelebornConf.h"
#include "celeborn/client/writer/PushState.h"

using namespace celeborn;
using namespace celeborn::client;

int main() {
  conf::CelebornConf conf;
  conf.registerProperty(
      conf::CelebornConf::kClientPushLimitInFlightTimeoutMs, std::to_string(0));
  conf.registerProperty(
      conf::CelebornConf::kClientPushLimitInFlightSleepDeltaMs,
      std::to_string(0));
  conf.registerProperty(
      conf::CelebornConf::kClientPushMaxReqsInFlightTotal, std::to_string(0));
  conf.registerProperty(
      conf::CelebornConf::kClientPushMaxReqsInFlightPerWorker,
      std::to_string(0));

  std::unique_ptr<PushState> pushState = std::make_unique<PushState>(conf);
}
