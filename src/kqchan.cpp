#include <algorithm>
#include <cstdio>
#include <darlingserver/calltrace.hpp>
#include <darlingserver/kqchan.hpp>
#include <darlingserver/server.hpp>
#include <darlingserver/rpc-supplement.h>
#include <darlingserver/thread.hpp>
#include <darlingserver/logging.hpp>

#include <sys/socket.h>
#include <fcntl.h>
#include <atomic>

static DarlingServer::Log kqchanLog("kqchan");
static DarlingServer::Log kqchanMachPortLog("kqchan:mach_port");
static DarlingServer::Log kqchanProcLog("kqchan:proc");
static std::atomic_uint64_t kqchanDebugIDCounter = 0;

//
// base class
//

DarlingServer::Kqchan::Kqchan(std::shared_ptr<DarlingServer::Process> process):
	_debugID(kqchanDebugIDCounter++),
	_process(process)
{
	kqchanLog.debug() << *this << ": Constructing kqchan" << kqchanLog.endLog;
};

DarlingServer::Kqchan::~Kqchan() {
	kqchanLog.debug() << *this << ": Destroying kqchan" << kqchanLog.endLog;
};

uintptr_t DarlingServer::Kqchan::_idForProcess() const {
	throw std::runtime_error("must be overridden in derived class");
};

// At most one unacknowledged notification per kqchan; the client's read is the acknowledgement.
void DarlingServer::Kqchan::_sendNotification() {
	CallTrace::add('n', -1, -1, 0, (uint64_t)this);
	auto process = _process.lock();
	if (!process) {
		return;
	}
	{
		std::unique_lock lock(_notificationMutex);
		if (!_canSendNotification) {
			return;
		}
		_canSendNotification = false;
	}
	process->kqchanNotify(_idForProcess());
};

void DarlingServer::Kqchan::logToStream(Log::Stream& stream) const {
	auto proc = _process.lock();
	stream << "[KQ:" << _debugID << ":";
	if (proc) {
		stream << *proc;
	} else {
		stream << "<null>";
	}
	stream << "]";
};

//
// mach port
//

DarlingServer::Kqchan::MachPort::MachPort(std::shared_ptr<DarlingServer::Process> process, uint32_t port, uint64_t receiveBuffer, uint64_t receiveBufferSize, uint64_t savedFilterFlags):
	Kqchan(process),
	_port(port),
	_receiveBuffer(receiveBuffer),
	_receiveBufferSize(receiveBufferSize),
	_savedFilterFlags(savedFilterFlags)
{
	kqchanMachPortLog.debug() << *this << ": Constructing Mach port kqchan" << kqchanMachPortLog.endLog;
};

DarlingServer::Kqchan::MachPort::~MachPort() {
	kqchanMachPortLog.debug() << *this << ": Destroying Mach port kqchan" << kqchanMachPortLog.endLog;

	if (_dtapeKqchan) {
		auto kqchan = _dtapeKqchan;

		// disable notifications so that `this` won't be used after we're destroyed
		dtape_kqchan_mach_port_disable_notifications(kqchan);

		// and schedule the duct-taped kqchan to be destroyed on a microthread
		auto debugID = _debugID;
		Thread::kernelAsync([=]() {
			kqchanMachPortLog.debug() << "Destroying duct-taped Mach port kqchan with ID " << debugID << kqchanMachPortLog.endLog;
			dtape_kqchan_mach_port_destroy(kqchan);
		});
	}
};

uintptr_t DarlingServer::Kqchan::MachPort::_idForProcess() const {
	return reinterpret_cast<uintptr_t>(this);
};

void DarlingServer::Kqchan::MachPort::setup() {
	auto proc = _process.lock();

	if (!proc) {
		throw std::system_error(ESRCH, std::generic_category());
	}

	// NOTE: the duct-taped kqchan will never notify us after we die
	//       since we disable notifications upon destruction,
	//       so using `this` here is safe
	_dtapeKqchan = dtape_kqchan_mach_port_create(proc->_dtapeTask, _port, _receiveBuffer, _receiveBufferSize, _savedFilterFlags, [](void* context) {
		auto self = reinterpret_cast<MachPort*>(context);
		self->_notify();
	}, this);
	if (!_dtapeKqchan) {
		kqchanMachPortLog.debug() << *this << ": Failed to create duct-taped Mach port kqchan for port " << _port << kqchanMachPortLog.endLog;
		throw std::system_error(ESRCH, std::generic_category());
	}

	_notifyIfEvents();
};

void DarlingServer::Kqchan::MachPort::modify(uint64_t receiveBuffer, uint64_t receiveBufferSize, uint64_t savedFilterFlags) {
	dtape_kqchan_mach_port_modify(_dtapeKqchan, receiveBuffer, receiveBufferSize, savedFilterFlags);
	_notifyIfEvents();
};

bool DarlingServer::Kqchan::MachPort::read(uint64_t defaultBuffer, uint64_t defaultBufferSize, dserver_kqchan_reply_mach_port_read_t* reply) {
	CallTrace::add('k', -1, -1, 0, (uint64_t)this);
	{
		// set before the fill: an event right after it is notified again (worst case a stale read)
		std::unique_lock lock(_notificationMutex);
		_notificationCount++;
		_canSendNotification = true;
	}
	bool got = dtape_kqchan_mach_port_fill(_dtapeKqchan, reply, defaultBuffer, defaultBufferSize);
	CallTrace::add('K', -1, -1, got ? 0 : 0xdead, (uint64_t)this);
	_notifyIfEvents();
	return got;
};

void DarlingServer::Kqchan::MachPort::_notify() {
	_sendNotification();
};

void DarlingServer::Kqchan::MachPort::_notifyIfEvents() {
	if (dtape_kqchan_mach_port_has_events(_dtapeKqchan)) {
		_notify();
	}
};

//
// process
//
// TODO: NOTE_REAP and NOTE_SIGNAL
//

DarlingServer::Kqchan::Process::Process(std::shared_ptr<DarlingServer::Process> process, pid_t nspid, uint32_t flags):
	Kqchan(process),
	_nspid(nspid),
	_flags(flags)
{
	kqchanProcLog.debug() << *this << ": Constructing process kqchan" << kqchanProcLog.endLog;
};

DarlingServer::Kqchan::Process::~Process() {
	kqchanProcLog.debug() << *this << ": Destroying process kqchan" << kqchanProcLog.endLog;

	if (auto targetProcess = _targetProcess.lock()) {
		targetProcess->unregisterListeningKqchan(_idForProcess());
	}
};

uintptr_t DarlingServer::Kqchan::Process::_idForProcess() const {
	return reinterpret_cast<uintptr_t>(this);
};

void DarlingServer::Kqchan::Process::setup() {
	auto maybeTargetProcess = processRegistry().lookupEntryByNSID(_nspid);
	if (!maybeTargetProcess) {
		kqchanProcLog.debug() << *this << ": Failed to create process kqchan for PID " << _nspid << kqchanProcLog.endLog;
		throw std::system_error(ESRCH, std::generic_category());
	}

	auto targetProcess = *maybeTargetProcess;
	_targetProcess = targetProcess;

	std::unique_lock lock(_mutex);
	_open = true;
	if (!_attached) {
		targetProcess->registerListeningKqchan(shared_from_this());
		_attached = true;
	}
	if (!_events.empty()) {
		_sendNotification();
	}
};

// NOTE_TRACK is not supported (the client never asks for it): a child's kqchan is dropped.
bool DarlingServer::Kqchan::Process::read(uint32_t* fflags, int64_t* data) {
	{
		std::unique_lock lock(_notificationMutex);
		_notificationCount++;
		_canSendNotification = true;
	}

	std::unique_lock lock(_mutex);
	while (!_events.empty()) {
		auto event = std::move(_events.front());
		_events.pop_front();
		*fflags = event.events & _flags;
		*data = event.data;
		if (*fflags == 0) {
			continue;
		}
		if (!_events.empty()) {
			_sendNotification();
		}
		return true;
	}
	return false;
};

void DarlingServer::Kqchan::Process::_notify(uint32_t event, int64_t data) {
	Event newEvent;

	kqchanProcLog.debug() << *this << ": notified with {event=" << event << ",data=" << data << "}" << kqchanProcLog.endLog;

	newEvent.data = data;
	newEvent.events = event;
	newEvent.newKqchan = nullptr;

	if (event == NOTE_FORK) {
		// NOTE: we always setup a new kqchan regardless of whether or not the user has currently requested NOTE_TRACK or not,
		//       in case the user does have NOTE_TRACK when reading the event.

		auto listeningProcess = _process.lock();

		if (!listeningProcess) {
			return;
		}

		auto maybeChild = processRegistry().lookupEntryByNSID(data & NOTE_PDATAMASK);

		if (!maybeChild) {
			kqchanProcLog.debug() << *this << ": notified with NOTE_FORK (and wanted NOTE_TRACK), but couldn't find child" << kqchanProcLog.endLog;
		} else {
			auto child = *maybeChild;

			auto newKqchan = std::make_shared<Process>(listeningProcess, data & NOTE_PDATAMASK, _flags);

			child->registerListeningKqchan(newKqchan);
			{
				std::unique_lock newLock(newKqchan->_mutex);
				newKqchan->_targetProcess = child;
				newKqchan->_attached = true;
			}

			auto childParent = child->parentProcess();

			newKqchan->_notify(NOTE_CHILD, (childParent ? childParent->nsid() : 0) & NOTE_PDATAMASK);

			newEvent.newKqchan = newKqchan;
		}
	}

	{
		std::unique_lock lock(_mutex);
		_events.push_back(std::move(newEvent));
		if (_open) {
			_sendNotification();
		}
	}
};

void DarlingServer::Kqchan::Process::logToStream(Log::Stream& stream) const {
	auto proc = _targetProcess.lock();

	Kqchan::logToStream(stream);

	stream << ":";
	if (proc) {
		stream << *proc;
	} else {
		stream << "<null>";
	}
};
