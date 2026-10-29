/**
 * iosc: дамп микропотоков сервера по запросу — `touch <prefix>/dump.req` (inotify),
 * вывод — <prefix>/dump/dserver.txt. Для каждого потока: pid/tid (хост и пространство имён
 * контейнера), активный/ожидающий вызов, состояние микропотока, ожидание в duct-tape
 * (wait_event, wmesg) и цепочка кадров приостановленного микропотока. Микропотоки не видны
 * в стеках ОС — без этого «RPC не вернулся» не отличить от «микропоток ждёт на waitq».
 * Адреса внутри исполняемого файла печатаются как exe+0x… (для addr2line).
 */

#include <darlingserver/calltrace.hpp>
#include <darlingserver/registry.hpp>
#include <darlingserver/thread.hpp>
#include <darlingserver/process.hpp>
#include <darlingserver/call.hpp>
#include <darlingserver/duct-tape.h>
#include <darlingserver/kqchan.hpp>

#include <sys/inotify.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <vector>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <string>
#include <thread>

extern "C" char __executable_start, _end;

// Чтение своей памяти без риска SIGSEGV: поток мог умереть, и его память — освободиться.
static bool peek(const void* address, void* out, size_t size) {
	struct iovec local = { out, size }, remote = { const_cast<void*>(address), size };
	return process_vm_readv(getpid(), &local, 1, &remote, 1, 0) == (ssize_t)size;
};

static bool inImage(const char* string) {
	return string >= &__executable_start && string < &_end;
};

static void printAddress(int fd, uintptr_t address) {
	uintptr_t base = (uintptr_t)&__executable_start, end = (uintptr_t)&_end;
	if (address >= base && address < end) {
		dprintf(fd, " exe+%#lx", (unsigned long)(address - base));
	} else {
		dprintf(fd, " %#lx", (unsigned long)address);
	}
};

// iosc: kqchan Mach-портов каждого процесса — порт, флаги уведомлений, очередь в duct-tape
void DarlingServer::dumpKqchans(int fd) {
	for (auto& process: DarlingServer::processRegistry().copyEntries()) {
		if (process->_dtapeTask) {
			struct Ctx { int fd; pid_t pid, nspid; } c = { fd, process->id(), process->nsid() };
			dtape_task_describe_ports(process->_dtapeTask, [](void* ctx, const char* line) {
				auto c = static_cast<Ctx*>(ctx);
				dprintf(c->fd, "порт процесс %d (ns %d) %s\n", c->pid, c->nspid, line);
			}, &c);
		}
		std::shared_lock lock(process->_rwlock);
		for (auto& [id, kq]: process->_kqchannels) {
			auto mp = std::dynamic_pointer_cast<DarlingServer::Kqchan::MachPort>(kq);
			if (!mp) {
				continue;
			}
			char buf[256] = "";
			if (mp->_dtapeKqchan) {
				dtape_kqchan_mach_port_describe(mp->_dtapeKqchan, buf, sizeof(buf));
			}
			dprintf(fd, "kqchan %llu процесс %d (ns %d) порт %#x canSend=%d canSendNotification=%d defer=%d deferred=%d notifications=%llu %s\n",
				(unsigned long long)mp->_debugID, process->id(), process->nsid(), mp->_port, mp->_canSend,
				mp->_canSendNotification, mp->_deferNotification, mp->_deferredNotification.has_value(),
				(unsigned long long)mp->_notificationCount, buf);
		}
	}
};

void DarlingServer::dumpThreads(int fd) {
	auto threads = threadRegistry().copyEntries();
	time_t now = time(nullptr);
	char when[64];
	strftime(when, sizeof(when), "%F %T", localtime(&now));
	dprintf(fd, "=== darlingserver pid %d, %s, потоков %zu, exe=%p\n", getpid(), when, threads.size(), (void*)&__executable_start);

	for (auto& thread: threads) {
		// чужой поток может держать замок долго (он и завис) — читаем и без него
		bool locked = thread->_rwlock.try_lock_shared();

		auto process = thread->_process;
		auto active = thread->_activeCall;
		auto pending = thread->_pendingCall;
		std::string path = process ? process->executablePath() : "";
		if (process && process == Process::kernelProcess()) {
			path = "<kernel>";
		}

		dprintf(fd, "поток %d (ns %d) процесс %d (ns %d) %s%s\n", thread->_tid, thread->_nstid,
			process ? process->id() : -1, process ? process->nsid() : -1, path.c_str(), locked ? "" : " [замок занят]");
		dprintf(fd, "  вызов: активный %s, ожидающий %s\n",
			active ? Call::callNumberToString(active->number()) : "-",
			pending ? Call::callNumberToString(pending->number()) : "-");
		dprintf(fd, "  состояние:%s%s%s%s%s%s прерываний %zu (ждут %zu)\n",
			thread->_running ? " running" : "",
			thread->_suspended ? " suspended" : "",
			thread->_continuationCallback ? " continuation" : "",
			thread->_terminating ? " terminating" : "",
			thread->_dead ? " dead" : "",
			thread->_deferReplyForS2C ? " s2c-deferred" : "",
			thread->_interrupts.size(), thread->_pendingInterrupts.size());

		if (auto dtape = thread->_dtapeThread) {
			std::vector<char> copy(dtape_thread_struct_size());
			char buf[256];
			if (peek(dtape, copy.data(), copy.size())) {
				dtape_thread_describe((dtape_thread_t*)copy.data(), buf, sizeof(buf), inImage);
				dprintf(fd, "  dtape: %s\n", buf);
			} else {
				dprintf(fd, "  dtape: %p недоступен\n", (void*)dtape);
			}
		}

		// приостановлен без продолжения — стек микропотока цел, контекст из getcontext в suspend()
		if (thread->_suspended && !thread->_continuationCallback && thread->_stack.base) {
			uintptr_t lo = (uintptr_t)thread->_stack.base, hi = lo + thread->_stack.size;
			uintptr_t fp = thread->_resumeContext.uc_mcontext.regs[29];
			dprintf(fd, "  стек:");
			printAddress(fd, thread->_resumeContext.uc_mcontext.pc);
			uintptr_t frame[2];
			for (int depth = 0; depth < 32 && fp >= lo && fp + 16 <= hi && !(fp & 7) && peek((void*)fp, frame, sizeof(frame)); ++depth) {
				printAddress(fd, frame[1]);
				if (frame[0] <= fp) {
					break;
				}
				fp = frame[0];
			}
			dprintf(fd, "\n");
		}

		if (locked) {
			thread->_rwlock.unlock_shared();
		}
	}

	dumpKqchans(fd);
};

void DarlingServer::startDumpWatcher(const char* prefix) {
	std::string dir = prefix;
	// сигналы процесса — не этому потоку (сервер раздаёт их своим потокам и signalfd)
	sigset_t all, old;
	sigfillset(&all);
	pthread_sigmask(SIG_BLOCK, &all, &old);
	std::thread([dir]() {
		int in = inotify_init1(IN_CLOEXEC);
		if (in < 0 || inotify_add_watch(in, dir.c_str(), IN_CLOSE_WRITE | IN_ATTRIB) < 0) {
			fprintf(stderr, "dump watcher: inotify %s: %s\n", dir.c_str(), strerror(errno));
			return;
		}
		alignas(struct inotify_event) char buf[4096];
		for (;;) {
			ssize_t n = read(in, buf, sizeof(buf));
			if (n <= 0) {
				if (n < 0 && errno == EINTR) {
					continue;
				}
				return;
			}
			bool requested = false;
			for (char* p = buf; p < buf + n; ) {
				auto event = (struct inotify_event*)p;
				if (event->len && strcmp(event->name, "dump.req") == 0) {
					requested = true;
				}
				p += sizeof(*event) + event->len;
			}
			if (!requested) {
				continue;
			}
			std::string out = dir + "/dump";
			mkdir(out.c_str(), 0777);
			int fd = open((out + "/dserver.txt").c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
			if (fd < 0) {
				continue;
			}
			dumpThreads(fd);
			{
				// кольцо событий вызовов (calltrace.hpp), от старых к новым
				uint64_t end = CallTrace::next.load(), begin = end > CallTrace::kSize ? end - CallTrace::kSize : 0;
				dprintf(fd, "trace: %llu событий\n", (unsigned long long)end);
				for (uint64_t i = begin; i < end; i++) {
					auto& e = CallTrace::ring[i % CallTrace::kSize];
					// редкие события — из всего кольца, остальные — последние 50 тыс.
					if (end - i > 50000 && !strchr("kKnxspabcr", e.ev)) {
						continue;
					}
					dprintf(fd, "trace %llu.%09llu %c pid %d tid %d num %u x %llu\n", (unsigned long long)(e.ns / 1000000000ull),
						(unsigned long long)(e.ns % 1000000000ull), e.ev, e.pid, e.tid, e.num, (unsigned long long)e.extra);
				}
			}
			close(fd);
		}
	}).detach();
	pthread_sigmask(SIG_SETMASK, &old, NULL);
};
