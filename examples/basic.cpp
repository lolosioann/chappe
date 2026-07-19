// examples/basic.cpp
// demonstrates the full stack: broker daemon + client nodes + async dispatch.
// The daemon runs in-process here (it's just a BrokerServer); in a real system
// it would be the separate broker_daemon binary and the nodes separate processes.
#include "broker_server.hpp"
#include "node.hpp"
#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

// ---- Messages --------------------------------------------------------------

struct IMUReading {
  float ax, ay, az;
};
struct LaneDetected {
  float curvature;
  int lane_id;
};
struct MotorCommand {
  float throttle;
  float steering;
};

MAKE_TOPIC(IMUReading, "imu/reading");
MAKE_TOPIC(LaneDetected, "lane/detected");
MAKE_TOPIC(MotorCommand, "motor/command");

// ---- Main ------------------------------------------------------------------

int main() {
  ipc::BrokerServer broker; // in-process here; real use: run broker_daemon

  Node sensor("sensor");                 // publishes IMU readings
  Node vision("vision", 2);              // heavy processing on 2 worker threads
  Node control("control");              // latency-sensitive, synchronous
  Node actuator("actuator");            // drives motor commands
  for (Node *n : {&sensor, &vision, &control, &actuator})
    n->connect(); // defaults to the well-known broker address

  // vision subscribes to IMU, publishes lane estimates asynchronously
  vision.subscribe([&vision](const IMUReading &msg) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2)); // simulate work
    std::cout << "[vision] processed IMU ax=" << msg.ax << "\n";
    vision.publish(LaneDetected{.curvature = msg.ax * 0.1f, .lane_id = 1});
  });

  // control subscribes to lane estimates, issues motor commands
  control.subscribe([&control](const LaneDetected &msg) {
    std::cout << "[control] lane " << msg.lane_id
              << " curvature=" << msg.curvature << "\n";
    control.publish(MotorCommand{.throttle = 0.5f, .steering = msg.curvature});
  });

  // actuator subscribes to motor commands
  std::atomic<int> commands{0};
  actuator.subscribe([&commands](const MotorCommand &cmd) {
    std::cout << "[actuator] throttle=" << cmd.throttle
              << " steering=" << cmd.steering << "\n";
    commands.fetch_add(1);
  });

  // Make sure every subscription is live at the daemon before publishing (v1
  // has no resubscribe, so a publish before its subscriber registers is lost).
  vision.sync();
  control.sync();
  actuator.sync();

  std::cout << "-- publishing 3 IMU readings --\n\n";
  for (int i = 0; i < 3; i++) {
    sensor.publish(IMUReading{.ax = 0.1f * i, .ay = 0.0f, .az = 9.8f});
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // wait for the async sensor->vision->control->actuator chain to flush
  for (int i = 0; i < 200 && commands.load() < 3; i++)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

  std::cout << "\ndone (" << commands.load() << " motor commands).\n";
}
