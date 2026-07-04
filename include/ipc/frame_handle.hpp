#pragma once

#include <cstdint>
#include <type_traits>

namespace ipc {

// Metadata for one published frame. The pixel/sensor bytes live in the shm ring
// slot; this small POD rides through the broker as the message. Each frame
// topic is its own broker message type — derive a tag from FrameHandle and give
// it a topic name:
//
//     struct FrontCam : ipc::FrameHandle {};
//     MAKE_TOPIC(FrontCam, "cam.front");
//
// The topic name identifies the backing ring (one ring per topic); slot_index
// is deliberately absent — consumers read via retain_latest(), so an index in
// the message would be dead weight. Stays trivially copyable so it (and any
// derived tag) can cross the socket bridge later as raw bytes, no serialization.
struct FrameHandle {
  uint64_t timestamp_ns;
  uint32_t width;
  uint32_t height;
  uint32_t stride;
};

static_assert(std::is_trivially_copyable_v<FrameHandle>,
              "FrameHandle must stay POD for zero-copy socket transport");

} // namespace ipc
