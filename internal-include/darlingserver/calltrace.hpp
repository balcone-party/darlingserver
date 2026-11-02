// iosc: in-memory ring of call events (no file writes: they change timing and hide races).
// Printed in the dump (dump.cpp). Events: R call received, P made pending, i deferred
// interrupt, A became active, Q thread queued for work, W doWork, d/r/t doWork left (deferred /
// already running / terminating), S reply sent, x call dropped, s S2C reply, p push_reply.
#pragma once
#include <atomic>
#include <cstdint>
#include <ctime>

namespace DarlingServer::CallTrace {
	struct Entry { uint64_t ns; char ev; int32_t pid, tid; uint32_t num; uint64_t extra; };
	constexpr uint64_t kSize = 1 << 18;  // 8 MB: rare events (kqchan, executors) cover minutes
	inline Entry ring[kSize];
	inline std::atomic<uint64_t> next{0};

	inline void add(char ev, int32_t pid, int32_t tid, uint32_t num, uint64_t extra = 0) {
		uint64_t i = next.fetch_add(1, std::memory_order_relaxed);
		struct timespec ts;
		clock_gettime(CLOCK_MONOTONIC, &ts);
		ring[i % kSize] = Entry{ (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec, ev, pid, tid, num, extra };
	}
};
