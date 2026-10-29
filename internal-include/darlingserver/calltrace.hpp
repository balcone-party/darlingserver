// iosc: кольцо событий вызовов в памяти (без записи в файл — запись меняет тайминг и прячет
// гонки). Выводится в дамп (dump.cpp). События: R — вызов принят, P — назначен ожидающим,
// i — отложенное прерывание, A — стал активным, Q — поток в очередь работы, W — doWork,
// d/r/t — doWork вышел (отложен / уже выполняется / завершается), S — ответ отправлен,
// x — вызов выброшен, s — ответ S2C, p — push_reply.
#pragma once
#include <atomic>
#include <cstdint>
#include <ctime>

namespace DarlingServer::CallTrace {
	struct Entry { uint64_t ns; char ev; int32_t pid, tid; uint32_t num; uint64_t extra; };
	constexpr uint64_t kSize = 1 << 18;  // 8 МБ: редкие события (kqchan, исполнители) — за минуты
	inline Entry ring[kSize];
	inline std::atomic<uint64_t> next{0};

	inline void add(char ev, int32_t pid, int32_t tid, uint32_t num, uint64_t extra = 0) {
		uint64_t i = next.fetch_add(1, std::memory_order_relaxed);
		struct timespec ts;
		clock_gettime(CLOCK_MONOTONIC, &ts);
		ring[i % kSize] = Entry{ (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec, ev, pid, tid, num, extra };
	}
};
