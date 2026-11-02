#ifndef _DARLINGSERVER_KQCHAN_HPP_
#define _DARLINGSERVER_KQCHAN_HPP_

#include <memory>
#include <optional>
#include <deque>

#include <darlingserver/monitor.hpp>
#include <darlingserver/message.hpp>
#include <darlingserver/duct-tape.h>
#include <darlingserver/rpc-supplement.h>
#include <darlingserver/logging.hpp>

#define NOTE_EXIT        0x80000000U
#define NOTE_FORK        0x40000000U
#define NOTE_EXEC        0x20000000U
#define NOTE_REAP        0x10000000U
#define NOTE_SIGNAL      0x08000000U
#define NOTE_EXITSTATUS  0x04000000U
#define NOTE_EXIT_DETAIL 0x02000000U
#define NOTE_TRACK       0x00000001U
#define NOTE_TRACKERR    0x00000002U
#define NOTE_CHILD       0x00000004U
#define NOTE_PDATAMASK   0x000fffffU
#define NOTE_PCTRLMASK  (~(NOTE_PDATAMASK))

namespace DarlingServer {
	class Process;
	class Thread;

	void dumpThreads(int fd);
	void dumpKqchans(int fd);

	class Kqchan: public Loggable {
		friend class DarlingServer::Process;
		friend void dumpThreads(int fd);  // iosc: src/dump.cpp
		friend void dumpKqchans(int fd);

	protected:
		uint64_t _debugID;
		std::weak_ptr<DarlingServer::Process> _process;
		std::mutex _notificationMutex;
		bool _canSendNotification = true;
		uint64_t _notificationCount = 0;

		Kqchan(std::shared_ptr<DarlingServer::Process> process);

		virtual uintptr_t _idForProcess() const;

		void _sendNotification();

	public:
		virtual ~Kqchan();

		uint64_t id() const { return _idForProcess(); }

		void logToStream(Log::Stream& stream) const;

		class MachPort;
		class Process;
	};


	class Kqchan::MachPort: public Kqchan, public std::enable_shared_from_this<MachPort> {
	private:
		uint32_t _port;
		uint64_t _receiveBuffer;
		uint64_t _receiveBufferSize;
		uint64_t _savedFilterFlags;
		dtape_kqchan_mach_port_t* _dtapeKqchan = nullptr;
		friend void dumpThreads(int fd);  // iosc: src/dump.cpp
		friend void dumpKqchans(int fd);

		void _notify();
		void _notifyIfEvents();

	protected:
		virtual uintptr_t _idForProcess() const;

	public:
		MachPort(std::shared_ptr<DarlingServer::Process> process, uint32_t port, uint64_t receiveBuffer, uint64_t receiveBufferSize, uint64_t savedFilterFlags);
		~MachPort();

		MachPort(const MachPort&) = delete;
		MachPort& operator=(const MachPort&) = delete;

		MachPort(MachPort&&) = delete;
		MachPort& operator=(MachPort&&) = delete;

		void setup();
		// both run in the calling thread's microthread (an RPC)
		void modify(uint64_t receiveBuffer, uint64_t receiveBufferSize, uint64_t savedFilterFlags);
		bool read(uint64_t defaultBuffer, uint64_t defaultBufferSize, dserver_kqchan_reply_mach_port_read_t* reply);
	};

	class Kqchan::Process: public Kqchan, public std::enable_shared_from_this<Process> {
		friend class DarlingServer::Process;

	private:
		// some events can be coalesced, but ones like NOTE_FORK and NOTE_EXIT can't be sent in a single event
		struct Event {
		public:
			uint32_t events;
			int64_t data;
			std::shared_ptr<Process> newKqchan;
		};

		pid_t _nspid;
		uint32_t _flags;
		std::mutex _mutex;
		std::deque<Event> _events;
		std::weak_ptr<DarlingServer::Process> _targetProcess;
		bool _attached = false;
		bool _open = false;

		void _notify(uint32_t event, int64_t data);

	protected:
		virtual uintptr_t _idForProcess() const;

	public:
		Process(std::shared_ptr<DarlingServer::Process> process, pid_t nspid, uint32_t flags);
		~Process();

		Process(const Process&) = delete;
		Process& operator=(const Process&) = delete;

		Process(Process&&) = delete;
		Process& operator=(Process&&) = delete;

		void setup();
		bool read(uint32_t* fflags, int64_t* data);

		virtual void logToStream(Log::Stream& stream) const;
	};
};

#endif // _DARLINGSERVER_KQCHAN_HPP_
