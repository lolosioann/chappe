// examples/basic.cpp
// demonstrates the full stack: broker + nodes + async dispatch
#include "broker.hpp"
#include "node.hpp"
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

MAKE_TOPIC(IMUReading, "imu.reading");
MAKE_TOPIC(LaneDetected, "lane.detected");
MAKE_TOPIC(MotorCommand, "motor.command");

// ---- Main ------------------------------------------------------------------

int main() {
  Broker<IMUReading, LaneDetected, MotorCommand> broker;

  // sensor node — publishes IMU readings synchronously
  Node<IMUReading, LaneDetected, MotorCommand> sensor("sensor", broker);

  // vision node — heavy processing, runs on 2 worker threads
  Node<IMUReading, LaneDetected, MotorCommand> vision("vision", broker, 2);

  // control node — synchronous, latency sensitive
  Node<IMUReading, LaneDetected, MotorCommand> control("control", broker);

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
  auto actuator_guard = broker.subscribe([](const MotorCommand &cmd) {
    std::cout << "[actuator] throttle=" << cmd.throttle
              << " steering=" << cmd.steering << "\n";
  });

  std::cout << "-- publishing 3 IMU readings --\n\n";

  for (int i = 0; i < 3; i++) {
    sensor.publish(IMUReading{.ax = 0.1f * i, .ay = 0.0f, .az = 9.8f});
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  vision.drain(); // wait for async vision handlers to finish
  std::cout << "\ndone.\n";
}
