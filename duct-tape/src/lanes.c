// SPDX-License-Identifier: GPL-3.0-or-later
// iosc: server side of Mach message lanes (include/darlingserver/lanes.h).
//
// A port with steady traffic to a guest receiver gets a lane: its receiver takes messages from
// the shared ring, senders of non-complex messages write there directly. Messages that still go
// through the server stay on the port's queue and leave a SERVER entry in the ring, so the ring
// order is the delivery order. A receive through the server on a lane port is answered with
// IOSC_RCV_IN_LANE, except the one that takes a SERVER entry's message.
//
// The region is writable by every process of the device: indices, sizes and states read from it
// are checked before use, and a wait on another process is bounded.
#include <darlingserver/duct-tape.h>
#include <darlingserver/duct-tape/task.h>
#include <darlingserver/duct-tape/log.h>
#include <darlingserver/lanes.h>

#include <ipc/ipc_port.h>
#include <ipc/ipc_kmsg.h>
#include <ipc/ipc_mqueue.h>
#include <ipc/ipc_space.h>
#include <kern/task.h>
#include <kern/thread.h>
#include <kern/waitq.h>
#include <kern/ipc_kobject.h>
#include <ipc/ipc_pset.h>

#include <darlingserver/duct-tape/lanes.h>

void iosc_futex_wake(uint32_t* addr, int count);

#define LANE_OPEN_RATE 50                    // messages per second to a port before it gets a lane
#define LANE_WAIT_NS (100ull * NSEC_PER_MSEC) // longest wait for another process's writer

static uint8_t* region;
static uint64_t region_size;
static iosc_lanes_header_t* hdr;
static iosc_lanes_proc_t* procs;
static iosc_lane_t* lanes;
static iosc_lane_token_t* tokens;
static iosc_lane_pset_t* psets;
static ipc_pset_t pset_obj[IOSC_LANES_PSETS];  // published sets (referenced)
static ipc_port_t lane_port[IOSC_LANES_COUNT];  // the port of each open lane (not referenced)
// The published set of each lane (index + 1): the region copy is only for clients, any process
// of the device can rewrite it.
static uint32_t lane_pset[IOSC_LANES_COUNT];
static uint32_t lane_owner[IOSC_LANES_COUNT];
static uint32_t pset_owner[IOSC_LANES_PSETS], pset_name[IOSC_LANES_PSETS];
static ipc_port_t token_port[IOSC_LANES_TOKENS];  // each token's reply port (referenced)

bool iosc_lanes_enabled(void) {
	return region != NULL;
}

void dtape_lanes_init(void* base, uint64_t size) {
	if (size < iosc_lanes_size()) {
		dtape_log_error("lanes: region of %llu bytes, %llu needed", (unsigned long long)size, (unsigned long long)iosc_lanes_size());
		return;
	}
	region = base;
	region_size = size;
	hdr = base;
	hdr->magic = IOSC_LANES_MAGIC;
	hdr->version = IOSC_LANES_VERSION;
	hdr->size = size;
	hdr->procs = IOSC_LANES_PROCS;
	hdr->lanes = IOSC_LANES_COUNT;
	hdr->ring = IOSC_LANES_RING;
	hdr->entry = IOSC_LANES_ENTRY;
	hdr->procs_off = IOSC_LANES_PROCS_OFF;
	hdr->lanes_off = IOSC_LANES_LANES_OFF;
	hdr->tokens = IOSC_LANES_TOKENS;
	hdr->tokens_off = IOSC_LANES_TOKENS_OFF;
	hdr->rings_off = IOSC_LANES_RINGS_OFF;
	procs = (iosc_lanes_proc_t*)(region + hdr->procs_off);
	lanes = (iosc_lane_t*)(region + hdr->lanes_off);
	tokens = (iosc_lane_token_t*)(region + hdr->tokens_off);
	hdr->psets_off = IOSC_LANES_PSETS_OFF;
	psets = (iosc_lane_pset_t*)(region + hdr->psets_off);
	for (uint32_t i = 0; i < IOSC_LANES_COUNT; i++) {
		lanes[i].ring_off = hdr->rings_off + (uint64_t)i * IOSC_LANES_RING * IOSC_LANES_ENTRY;
	}
};

// The process index (+1) is kept in the task; procs[] is what other processes see.
uint32_t dtape_lanes_task_checkin(dtape_task_t* task) {
	if (!region) {
		return UINT32_MAX;
	}
	if (task->lane_proc > IOSC_LANES_PROCS) {
		task->lane_proc = 0;
	}
	if (!task->lane_proc) {
		for (uint32_t i = 0; i < IOSC_LANES_PROCS; i++) {
			if (__atomic_load_n(&procs[i].pid, __ATOMIC_RELAXED) == 0) {
				task->lane_proc = i + 1;
				break;
			}
		}
		if (!task->lane_proc) {
			return UINT32_MAX;
		}
	}
	iosc_lanes_proc_t* p = &procs[task->lane_proc - 1];
	memcpy(p->audit, &task->xnu_task.audit_token, sizeof(p->audit));
	p->alive = 1;
	__atomic_store_n(&p->pid, task->saved_pid ? task->saved_pid : 1, __ATOMIC_RELEASE);
	return task->lane_proc - 1;
};

// After uidgid: senders copy their audit token from here.
void dtape_lanes_task_ids(dtape_task_t* task) {
	if (region && task->lane_proc && task->lane_proc <= IOSC_LANES_PROCS) {
		memcpy(procs[task->lane_proc - 1].audit, &task->xnu_task.audit_token, sizeof(procs[0].audit));
	}
};

void dtape_lanes_task_gone(dtape_task_t* task) {
	if (region && task->lane_proc && task->lane_proc <= IOSC_LANES_PROCS) {
		iosc_lanes_proc_t* p = &procs[task->lane_proc - 1];
		uint32_t me = task->lane_proc - 1;
		for (uint32_t i = 0; i < IOSC_LANES_TOKENS; i++) {
			if (tokens[i].state != iosc_token_unused && (tokens[i].receiver == me || tokens[i].client == me)) {
				__atomic_store_n(&tokens[i].state, iosc_token_dead, __ATOMIC_RELEASE);
			}
		}
		p->alive = 0;
		__atomic_store_n(&p->pid, 0, __ATOMIC_RELEASE);
		task->lane_proc = 0;
	}
};

static iosc_lane_entry_t* lane_entry(iosc_lane_t* l, uint64_t pos) {
	// from the lane's index, never from ring_off in the region
	uint64_t i = (uint64_t)(l - lanes) * IOSC_LANES_RING + (pos & (IOSC_LANES_RING - 1));
	return (iosc_lane_entry_t*)(region + IOSC_LANES_RINGS_OFF + i * IOSC_LANES_ENTRY);
}

static iosc_lane_t* port_lane(ipc_port_t port) {
	uint32_t i = port->ip_lane;
	return (i && i <= IOSC_LANES_COUNT) ? &lanes[i - 1] : NULL;
}

static uint32_t task_lane_proc(ipc_space_t space) {
	if (!space || space == ipc_space_kernel || !space->is_task) {
		return 0;
	}
	return dtape_task_for_xnu_task(space->is_task)->lane_proc;
}

static void lane_wake_all(iosc_lane_t* l) {
	__atomic_add_fetch(&l->ready, 1, __ATOMIC_SEQ_CST);
	iosc_futex_wake(&l->ready, INT32_MAX);
}

static uint32_t lane_open(ipc_port_t port, ipc_mqueue_t mqueue);

static iosc_lane_pset_t* pset_at(uint32_t s) {
	return s && s <= IOSC_LANES_PSETS && pset_obj[s - 1] ? &psets[s - 1] : NULL;
}

// A knote watches this lane (or its set): wake the owner's lane watcher (client lanes.c).
static void lane_doorbell(iosc_lane_t* l) {
	uint32_t s = lane_pset[l - lanes];
	if (!l->kq && !(s && s <= IOSC_LANES_PSETS && psets[s - 1].kq)) {
		return;
	}
	if (l->owner < IOSC_LANES_PROCS) {
		__atomic_add_fetch(&procs[l->owner].doorbell, 1, __ATOMIC_SEQ_CST);
		iosc_futex_wake(&procs[l->owner].doorbell, 1);
	}
}

static void pset_wake(uint32_t s, int count) {
	iosc_lane_pset_t* p = pset_at(s);
	if (p) {
		__atomic_add_fetch(&p->ready, 1, __ATOMIC_SEQ_CST);
		if (__atomic_load_n(&p->waiters, __ATOMIC_SEQ_CST)) {
			iosc_futex_wake(&p->ready, count);
		}
	}
}

// Under the mqueue lock, before a message is queued to the port: count, and open a lane when the
// port is hot, has no queued messages and is plain (no set, no knote, a guest receiver).
// Receivers waiting in the server are sent away (MACH_RCV_PORT_CHANGED, which their receive
// turns into IOSC_RCV_IN_LANE).
void iosc_lane_note_send(ipc_mqueue_t mqueue, ipc_kmsg_t kmsg) {
	if (!region) {
		return;
	}
	ipc_port_t port = ip_from_mq(mqueue);
	if (port->ip_lane) {
		return;
	}
	uint64_t now = mach_absolute_time();
	if (now < port->ip_lane_ban) {
		return;
	}
	if (now - port->ip_lane_t0 > NSEC_PER_SEC) {
		port->ip_lane_t0 = now;
		port->ip_lane_count = 0;
		port->ip_lane_simple = 0;
	}
	// a lane only pays off when most messages could skip the server: complex ones would come
	// through it anyway and cost a lane_take more each
	mach_msg_header_t* h = kmsg->ikm_header;
	if (!(h->msgh_bits & MACH_MSGH_BITS_COMPLEX) && h->msgh_size <= IOSC_LANES_MSG_MAX + sizeof(mach_msg_header_t) &&
	    !h->msgh_voucher_port) {
		port->ip_lane_simple++;
	}
	if (++port->ip_lane_count < LANE_OPEN_RATE || port->ip_lane_simple * 3 < port->ip_lane_count * 2) {
		return;
	}
	if (!ip_active(port) || port->ip_specialreply || ip_kotype(port) != IKOT_NONE || !port->ip_receiver_name) {
		return;
	}
	if (port->ip_in_pset) {
		// a member of a published set gets its lane after the send (the set has to be unlinked)
		uint32_t s = port->ip_lane_pset;
		if (s && s <= IOSC_LANES_PSETS && pset_obj[s - 1]) {
			port->ip_lane_want = 1;
		}
		return;
	}
	if (!ipc_kmsg_queue_empty(&mqueue->imq_messages)) {
		return;
	}
	lane_open(port, mqueue);
};

// mqueue locked; the port is plain (checked by the caller). Returns the lane index + 1 or 0.
static uint32_t lane_open(ipc_port_t port, ipc_mqueue_t mqueue) {
	uint32_t owner = task_lane_proc(port->ip_receiver);
	if (!owner) {
		return 0;
	}
	for (uint32_t i = 0; i < IOSC_LANES_COUNT; i++) {
		iosc_lane_t* l = &lanes[i];
		if (__atomic_load_n(&l->state, __ATOMIC_ACQUIRE) != iosc_lane_free || __atomic_load_n(&l->writers, __ATOMIC_ACQUIRE)) {
			continue;
		}
		for (uint64_t k = 0; k < IOSC_LANES_RING; k++) {
			__atomic_store_n(&lane_entry(l, k)->seq, k, __ATOMIC_RELAXED);
		}
		l->head = 0;
		l->tail = 0;
		l->owner = owner - 1;
		lane_owner[i] = owner - 1;
		l->name = port->ip_receiver_name;
		__atomic_add_fetch(&l->gen, 1, __ATOMIC_RELEASE);
		__atomic_store_n(&l->state, iosc_lane_open, __ATOMIC_RELEASE);
		port->ip_lane = i + 1;
		port->ip_lane_tserve = port->ip_lane_tnext;
		lane_port[i] = port;
		l->pset = 0;
		lane_pset[i] = 0;
		// a port watched by a knote (EVFILT_MACHPORT through kqchan) rings the doorbell
		l->kq = port->ip_lane_watched;
		port->ip_lane_t0 = mach_absolute_time();
		port->ip_lane_tail0 = 0;
		port->ip_lane_srv = 0;
		__atomic_add_fetch(&hdr->epoch, 1, __ATOMIC_RELEASE);
		if (!iosc_waitq_empty(&mqueue->imq_wait_queue)) {
			waitq_wakeup64_all_locked(&mqueue->imq_wait_queue, IPC_MQUEUE_RECEIVE, THREAD_RESTART, NULL,
			    WAITQ_ALL_PRIORITIES, WAITQ_KEEP_LOCKED);
		}
		dtape_log_debug("lanes: port %p (name %#x, proc %u) -> lane %u", port, port->ip_receiver_name, owner - 1, i);
		return i + 1;
	}
	return 0;
}

// A lane whose traffic is mostly messages through the server only adds a lane_take per message:
// it closes and the port stays without a lane for a while.
#define LANE_SERVER_SHARE 3      // close when more than 1/3 of the entries in a window are SERVER
#define LANE_BAN_NS (10ull * NSEC_PER_SEC)

static void lane_traffic(ipc_port_t port, iosc_lane_t* l) {
	uint64_t now = mach_absolute_time();
	uint64_t tail = __atomic_load_n(&l->tail, __ATOMIC_RELAXED);
	if (now - port->ip_lane_t0 > NSEC_PER_SEC) {
		port->ip_lane_t0 = now;
		port->ip_lane_tail0 = tail - 1;
		port->ip_lane_srv = 0;
	}
	port->ip_lane_srv++;
	uint64_t total = tail - port->ip_lane_tail0;
	bool close = port->ip_lane_srv >= 16 && (uint64_t)port->ip_lane_srv * LANE_SERVER_SHARE > total;
	if (close) {
		ip_lock(port);
		if (ip_active(port) && port->ip_lane == (uint32_t)(l - lanes) + 1) {
			iosc_lane_close(port, false);
			port->ip_lane_ban = now + LANE_BAN_NS;
		}
		ip_unlock(port);
	}
}

// After a message was queued to a lane port through the server: its SERVER entry. A full ring
// waits as a full queue would.
void iosc_lane_after_post(ipc_port_t port, uint32_t lane, uint32_t gen, uint32_t ticket) {
	iosc_lane_t* l = &lanes[lane - 1];
	for (;;) {
		if (port->ip_lane != lane || __atomic_load_n(&l->gen, __ATOMIC_ACQUIRE) != gen ||
		    __atomic_load_n(&l->state, __ATOMIC_ACQUIRE) != iosc_lane_open) {
			if ((int32_t)(ticket + 1 - port->ip_lane_tserve) > 0) {
				port->ip_lane_tserve = ticket + 1;  // never back: a newer lane may serve later tickets
			}
			return;  // closed meanwhile: the message is an ordinary queued one now
		}
		if (port->ip_lane_tserve != ticket) {
			// an earlier message still waits for room: SERVER entries keep the queue's order
			assert_wait_timeout((event_t)&l->tail, THREAD_UNINT, 1, NSEC_PER_MSEC);
			thread_block(THREAD_CONTINUE_NULL);
			continue;
		}
		uint64_t pos = __atomic_load_n(&l->tail, __ATOMIC_RELAXED);
		iosc_lane_entry_t* e = lane_entry(l, pos);
		uint64_t seq = __atomic_load_n(&e->seq, __ATOMIC_ACQUIRE);
		if (seq == pos) {
			if (__atomic_compare_exchange_n(&l->tail, &pos, pos + 1, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
				e->kind = iosc_lane_server;
				e->size = 0;
				e->writer = UINT32_MAX;
				e->tid = 0;
				__atomic_store_n(&e->seq, pos + 1, __ATOMIC_RELEASE);
				if (__atomic_add_fetch(&l->ready, 1, __ATOMIC_SEQ_CST) && __atomic_load_n(&l->waiters, __ATOMIC_SEQ_CST)) {
					iosc_futex_wake(&l->ready, 1);
				}
				pset_wake(lane_pset[l - lanes], 1);
				lane_doorbell(l);
				port->ip_lane_tserve = ticket + 1;
				lane_traffic(port, l);
				return;
			}
		} else if ((int64_t)(seq - pos) < 0) {
			// full: sleep this microthread (delay() would spin the server's only worker, and the
			// receiver needs it to drain the ring)
			assert_wait_timeout((event_t)&l->tail, THREAD_UNINT, 1, NSEC_PER_MSEC);
			thread_block(THREAD_CONTINUE_NULL);
		}
	}
};

void dtape_set_lane_hint(uint64_t addr) {
	current_thread()->iosc_lane_hint = addr;
};

static bool port_plain(ipc_port_t port) {
	return ip_active(port) && !port->ip_specialreply && ip_kotype(port) == IKOT_NONE && !port->ip_in_pset &&
	    port->ip_receiver_name;
}

#define LANE_REPLY_USES 8

// Real send-once rights to the client's reply port in the receiver's space, published as FREE
// tokens; the pool for (reply port, receiver) is topped up to IOSC_LANES_POOL.
static void make_tokens(ipc_port_t reply, uint32_t reply_name, uint32_t reply_lane, uint32_t client,
    ipc_space_t rspace, uint32_t receiver, uint32_t out[IOSC_LANES_POOL]) {
	uint32_t n = 0;
	uint32_t lane_gen = lanes[reply_lane - 1].gen;
	for (uint32_t i = 0; i < IOSC_LANES_TOKENS && n < IOSC_LANES_POOL; i++) {
		iosc_lane_token_t* t = &tokens[i];
		uint32_t st = __atomic_load_n(&t->state, __ATOMIC_ACQUIRE);
		if ((st == iosc_token_free || st == iosc_token_in_flight) && t->client == client &&
		    t->client_name == reply_name && t->receiver == receiver && t->lane == reply_lane && t->lane_gen == lane_gen) {
			out[n++] = i + 1;
		}
	}
	thread_t self = current_thread();
	struct knote* kn = self->ith_knote;
	self->ith_knote = ITH_KNOTE_NULL;
	for (uint32_t i = 0; i < IOSC_LANES_TOKENS && n < IOSC_LANES_POOL; i++) {
		iosc_lane_token_t* t = &tokens[i];
		uint32_t st = __atomic_load_n(&t->state, __ATOMIC_ACQUIRE);
		if (st != iosc_token_unused && st != iosc_token_dead) {
			continue;
		}
		ipc_port_t so = ipc_port_make_sonce(reply);
		mach_port_name_t name = MACH_PORT_NULL;
		if (!IP_VALID(so) || ipc_object_copyout(rspace, ip_to_object(so), MACH_MSG_TYPE_PORT_SEND_ONCE,
		    IPC_OBJECT_COPYOUT_FLAGS_NONE, NULL, NULL, &name) != KERN_SUCCESS) {
			if (IP_VALID(so)) {
				ipc_port_release_sonce(so);
			}
			break;
		}
		if (token_port[i]) {
			ip_release(token_port[i]);
		}
		ip_reference(reply);
		token_port[i] = reply;
		t->client = client;
		t->client_name = reply_name;
		t->receiver = receiver;
		t->name = name;
		t->lane = reply_lane;
		t->lane_gen = lane_gen;
		__atomic_store_n(&t->state, iosc_token_free, __ATOMIC_RELEASE);
		out[n++] = i + 1;
	}
	self->ith_knote = kn;
}

// After a send through the server to a lane port: tell the sender where the lane is and, when
// the reply port is the sender's own plain port, give it a lane and reply tokens in the receiver.
void iosc_lane_hint_sent(mach_port_name_t name, ipc_port_t port, ipc_port_t reply, mach_port_name_t reply_name) {
	thread_t self = current_thread();
	uint64_t addr = self->iosc_lane_hint;
	uint32_t gen;
	uint32_t lane = iosc_lane_of(port, &gen);
	if (!addr || !lane || !name) {
		return;
	}
	self->iosc_lane_hint = 0;
	iosc_lane_hint_t h = { .name = name, .lane = lane, .gen = gen };
	uint32_t client = task_lane_proc(current_space());
	if (IP_VALID(reply) && reply_name && client) {
		ip_lock(reply);
		uint32_t rlane = 0;
		// a reply port made for one call (MIG without a cached reply port) would only cost a lane,
		// tokens in the receiver and a lane_take for its reply
		if (++reply->ip_lane_replies >= LANE_REPLY_USES && port_plain(reply) && reply->ip_receiver == current_space() &&
		    reply->ip_receiver_name == reply_name) {
			rlane = reply->ip_lane;
			if (!rlane) {
				imq_lock(&reply->ip_messages);
				if (ipc_kmsg_queue_empty(&reply->ip_messages.imq_messages)) {
					rlane = lane_open(reply, &reply->ip_messages);
				}
				imq_unlock(&reply->ip_messages);
			}
		}
		ip_unlock(reply);
		ip_lock(port);
		ipc_space_t rspace = ip_active(port) ? port->ip_receiver : NULL;
		uint32_t receiver = task_lane_proc(rspace);
		if (rspace) {
			is_reference(rspace);
		}
		ip_unlock(port);
		if (rlane && receiver) {
			make_tokens(reply, reply_name, rlane, client - 1, rspace, receiver - 1, h.tokens);
			h.reply = reply_name;
		}
		if (rspace) {
			is_release(rspace);
		}
	}
	copyout(&h, addr, sizeof(h));
};

uint32_t iosc_lane_of(ipc_port_t port, uint32_t* gen) {
	iosc_lane_t* l = port_lane(port);
	if (!l) {
		return 0;
	}
	*gen = __atomic_load_n(&l->gen, __ATOMIC_ACQUIRE);
	return port->ip_lane;
}

// Writers that reserved an entry before the lane closed get LANE_WAIT_NS to commit it.
static void lane_quiesce(iosc_lane_t* l) {
	uint64_t deadline = mach_absolute_time() + LANE_WAIT_NS;
	while (__atomic_load_n(&l->writers, __ATOMIC_SEQ_CST) && mach_absolute_time() < deadline) {
	}
	if (__atomic_load_n(&l->writers, __ATOMIC_SEQ_CST)) {
		dtape_log_error("lanes: lane %ld: %u writers did not finish", (long)(l - lanes), l->writers);
		__atomic_store_n(&l->writers, 0, __ATOMIC_SEQ_CST);
	}
}

// An INLINE entry as a queued kmsg, as ipc_kmsg_get + copyin would have made it: kernel header
// layout (ports are pointers), the destination holding a send (or send-once, for a reply) right,
// a token's reply port a fresh send-once right. The port is locked.
// mach_msg_header_t as a 64-bit task sees it (ipc_kmsg.c: mach_msg_legacy_header_t)
typedef struct {
	mach_msg_bits_t msgh_bits;
	mach_msg_size_t msgh_size;
	mach_port_name_t msgh_remote_port;
	mach_port_name_t msgh_local_port;
	mach_port_name_t msgh_voucher_port;
	mach_msg_id_t msgh_id;
} user_header_t;

static ipc_kmsg_t lane_kmsg(ipc_port_t port, iosc_lane_entry_t* e) {
	uint32_t usize = e->size;
	if (usize < sizeof(user_header_t) || usize > IOSC_LANES_MSG_MAX || (usize & 3)) {
		return IKM_NULL;
	}
	const user_header_t* src = (const user_header_t*)((uint8_t*)e + IOSC_LANES_ENTRY_HDR);
	mach_msg_type_name_t dest_type = MACH_MSGH_BITS_LOCAL(src->msgh_bits);
	mach_msg_type_name_t reply_type = MACH_MSGH_BITS_REMOTE(src->msgh_bits);
	ipc_port_t reply = IP_NULL;
	uint32_t t = e->token;
	if (reply_type) {
		if (reply_type != MACH_MSG_TYPE_PORT_SEND_ONCE || !t || t > IOSC_LANES_TOKENS || !token_port[t - 1]) {
			return IKM_NULL;
		}
		__atomic_store_n(&tokens[t - 1].state, iosc_token_dead, __ATOMIC_RELEASE);
		reply = token_port[t - 1];
	}
	if (dest_type != MACH_MSG_TYPE_PORT_SEND && dest_type != MACH_MSG_TYPE_PORT_SEND_ONCE) {
		return IKM_NULL;
	}
	mach_msg_size_t size = usize + (mach_msg_size_t)(sizeof(mach_msg_header_t) - sizeof(user_header_t));
	ipc_kmsg_t kmsg = ipc_kmsg_alloc(size + MAX_TRAILER_SIZE);
	if (kmsg == IKM_NULL) {
		return IKM_NULL;
	}
	mach_msg_header_t* h = kmsg->ikm_header;
	h->msgh_size = size;
	h->msgh_bits = MACH_MSGH_BITS(dest_type, reply_type);
	h->msgh_remote_port = dest_type == MACH_MSG_TYPE_PORT_SEND ? ipc_port_make_send_locked(port) : ipc_port_make_sonce_locked(port);
	h->msgh_local_port = reply != IP_NULL ? ipc_port_make_sonce(reply) : IP_NULL;
	h->msgh_voucher_port = MACH_PORT_NULL;
	h->msgh_id = src->msgh_id;
	memcpy(h + 1, src + 1, usize - sizeof(*src));
	ikm_qos_init(kmsg);
	mach_msg_max_trailer_t* trailer = (mach_msg_max_trailer_t*)((vm_offset_t)h + size);
	bzero(trailer, sizeof(*trailer));
	memcpy(&trailer->msgh_audit, e->audit, sizeof(trailer->msgh_audit));
	trailer->msgh_sender.val[0] = e->audit[1];
	trailer->msgh_sender.val[1] = e->audit[2];
	trailer->msgh_trailer_type = MACH_MSG_TRAILER_FORMAT_0;
	trailer->msgh_trailer_size = MACH_MSG_TRAILER_MINIMUM_SIZE;
	return kmsg;
}

static void pset_detach(uint32_t s, uint32_t lane) {
	iosc_lane_pset_t* p = &psets[s - 1];
	for (uint32_t k = 0; k < IOSC_PSET_LANES; k++) {
		if (p->lanes[k] == lane) {
			__atomic_store_n(&p->lanes[k], 0, __ATOMIC_RELEASE);
		}
	}
	lanes[lane - 1].pset = 0;
	lane_pset[lane - 1] = 0;
}

// The port leaves lane mode. drop: the port is being destroyed (its messages go with it);
// otherwise the ring's entries become queued messages in ring order. The port is locked.
void iosc_lane_close(ipc_port_t port, bool drop) {
	iosc_lane_t* l = port_lane(port);
	if (!l) {
		port->ip_lane = 0;
		return;
	}
	__atomic_store_n(&l->state, iosc_lane_closing, __ATOMIC_SEQ_CST);
	lane_quiesce(l);

	ipc_mqueue_t mq = &port->ip_messages;
	if (!drop) {
		struct { uint32_t kind; ipc_kmsg_t kmsg; } got[IOSC_LANES_RING];
		uint32_t n = 0, servers = 0;
		uint64_t deadline = mach_absolute_time() + LANE_WAIT_NS;
		while (n < IOSC_LANES_RING) {
			uint64_t pos = __atomic_load_n(&l->head, __ATOMIC_RELAXED);
			if (pos == __atomic_load_n(&l->tail, __ATOMIC_ACQUIRE)) {
				break;
			}
			iosc_lane_entry_t* e = lane_entry(l, pos);
			if (__atomic_load_n(&e->seq, __ATOMIC_ACQUIRE) != pos + 1) {
				if (mach_absolute_time() > deadline) {
					break;  // a writer reserved and never committed (died): the rest is lost
				}
				continue;
			}
			if (!__atomic_compare_exchange_n(&l->head, &pos, pos + 1, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
				continue;  // a receiver took it
			}
			uint32_t kind = __atomic_load_n(&e->kind, __ATOMIC_RELAXED);  // read once: the region may change under us
			got[n].kind = kind;
			got[n].kmsg = kind == iosc_lane_inline ? lane_kmsg(port, e) : IKM_NULL;
			servers += kind == iosc_lane_server;
			if (kind == iosc_lane_server || got[n].kmsg != IKM_NULL) {
				n++;
			}
			__atomic_store_n(&e->seq, pos + IOSC_LANES_RING, __ATOMIC_RELEASE);
		}

		imq_lock(mq);
		struct ipc_kmsg_queue old;
		ipc_kmsg_queue_init(&old);
		uint32_t queued = 0;
		ipc_kmsg_t k;
		while ((k = ipc_kmsg_dequeue(&mq->imq_messages)) != IKM_NULL) {
			ipc_kmsg_enqueue(&old, k);
			queued++;
		}
		// queued messages whose SERVER entry a receiver already took go first
		uint32_t claimed = queued > servers ? queued - servers : 0;
		for (uint32_t i = 0; i < claimed; i++) {
			ipc_kmsg_enqueue(&mq->imq_messages, ipc_kmsg_dequeue(&old));
		}
		uint32_t added = 0;
		for (uint32_t i = 0; i < n; i++) {
			if (got[i].kind == iosc_lane_server) {
				if ((k = ipc_kmsg_dequeue(&old)) != IKM_NULL) {
					ipc_kmsg_enqueue(&mq->imq_messages, k);
				}
			} else {
				ipc_kmsg_enqueue(&mq->imq_messages, got[i].kmsg);
				added++;
			}
		}
		while ((k = ipc_kmsg_dequeue(&old)) != IKM_NULL) {
			ipc_kmsg_enqueue(&mq->imq_messages, k);
		}
		mq->imq_msgcount += added;
		iosc_mqueue_knote(mq);
		imq_unlock(mq);
	}

	uint32_t lane = port->ip_lane;
	uint32_t s = lane_pset[lane - 1];
	if (s) {
		pset_detach(s, lane);
		if (!drop && pset_at(s)) {
			// still a member of the set: messages from now on go through the set again
			ipc_pset_t pset = pset_obj[s - 1];
			uint64_t link = waitq_link_reserve(NULL);
			uint64_t prepost = waitq_prepost_reserve(NULL, 10, WAITQ_DONT_LOCK);
			ips_lock(pset);
			if (ips_active(pset)) {
				ipc_mqueue_add(&port->ip_messages, &pset->ips_messages, &link, &prepost);
			}
			ips_unlock(pset);
			waitq_link_release(link);
			waitq_prepost_release_reserve(prepost);
			pset_wake(s, INT32_MAX);
		}
	}
	lane_port[lane - 1] = IP_NULL;
	for (uint32_t i = 0; i < IOSC_LANES_TOKENS; i++) {
		if (tokens[i].lane == lane && tokens[i].state != iosc_token_unused) {
			__atomic_store_n(&tokens[i].state, iosc_token_dead, __ATOMIC_RELEASE);
		}
	}
	port->ip_lane = 0;
	__atomic_add_fetch(&l->gen, 1, __ATOMIC_RELEASE);
	__atomic_store_n(&l->state, iosc_lane_free, __ATOMIC_RELEASE);
	__atomic_add_fetch(&hdr->epoch, 1, __ATOMIC_RELEASE);
	lane_wake_all(l);
	lane_doorbell(l);  // a knote served from this lane goes back to the server's kqchan
	l->kq = 0;
};

// --- port sets ---
//
// A set that a task with lanes receives on is published: a receive on it through the server is
// answered with IOSC_RCV_IN_LANE, and the client looks at the rings of its lane members first.
// Lane members are unlinked from the set in the server; a message queued to any other member
// preposts the set, which bumps server_pending: the client then receives through the server.

static void pset_publish(ipc_pset_t pset, mach_port_name_t name, uint32_t owner) {
	for (uint32_t i = 0; i < IOSC_LANES_PSETS; i++) {
		iosc_lane_pset_t* p = &psets[i];
		if (pset_obj[i] || __atomic_load_n(&p->state, __ATOMIC_ACQUIRE) != iosc_lane_free) {
			continue;
		}
		memset(p->lanes, 0, sizeof(p->lanes));
		p->owner = owner - 1;
		p->name = name;
		pset_owner[i] = owner - 1;
		pset_name[i] = name;
		p->server_pending = 1;  // members may have queued messages
		p->kq = !SLIST_EMPTY(&pset->ips_messages.imq_klist);
		__atomic_add_fetch(&p->gen, 1, __ATOMIC_RELEASE);
		__atomic_store_n(&p->state, iosc_lane_open, __ATOMIC_RELEASE);
		ips_reference(pset);
		pset_obj[i] = pset;
		pset->ips_lane = i + 1;
		ipc_mqueue_t mq = &pset->ips_messages;
		imq_lock(mq);
		waitq_wakeup64_all_locked(&mq->imq_wait_queue, IPC_MQUEUE_RECEIVE, THREAD_RESTART, NULL,
		    WAITQ_ALL_PRIORITIES, WAITQ_KEEP_LOCKED);
		imq_unlock(mq);
		return;
	}
}

// The set stops being published: its lane members close (relink: the set lives on, for a knote;
// the set must not be locked) or just leave it (the set is being destroyed).
static void pset_unpublish(ipc_pset_t pset, bool relink) {
	uint32_t s = pset->ips_lane;
	if (!s || s > IOSC_LANES_PSETS || pset_obj[s - 1] != pset) {
		return;
	}
	iosc_lane_pset_t* p = &psets[s - 1];
	for (uint32_t k = 0; k < IOSC_PSET_LANES; k++) {
		uint32_t lane = p->lanes[k];
		if (!lane || lane > IOSC_LANES_COUNT) {
			continue;
		}
		ipc_port_t port = lane_port[lane - 1];
		if (relink && port) {
			ip_lock(port);
			if (port->ip_lane == lane) {
				iosc_lane_close(port, false);
			}
			ip_unlock(port);
		} else {
			pset_detach(s, lane);
		}
	}
	pset->ips_lane = 0;
	pset_obj[s - 1] = NULL;
	__atomic_add_fetch(&p->gen, 1, __ATOMIC_RELEASE);
	__atomic_store_n(&p->state, iosc_lane_free, __ATOMIC_RELEASE);
	__atomic_add_fetch(&p->ready, 1, __ATOMIC_SEQ_CST);
	iosc_futex_wake(&p->ready, INT32_MAX);
	ips_release(pset);
}

static bool pset_attach(uint32_t s, ipc_port_t port) {
	iosc_lane_pset_t* p = &psets[s - 1];
	for (uint32_t k = 0; k < IOSC_PSET_LANES; k++) {
		if (!p->lanes[k]) {
			lanes[port->ip_lane - 1].pset = s;
			lane_pset[port->ip_lane - 1] = s;
			__atomic_store_n(&p->lanes[k], port->ip_lane, __ATOMIC_RELEASE);
			port->ip_lane_pset = s;
			pset_wake(s, INT32_MAX);
			return true;
		}
	}
	return false;
}

// A receive is sent to the lane (IOSC_RCV_IN_LANE): the client looks the lane (or set) up by state,
// owner and name in the region. Put back what a process of the device may have overwritten, or
// the receive would bounce between the server and the region for good.
static void put32(uint32_t* w, uint32_t v) {
	if (__atomic_load_n(w, __ATOMIC_RELAXED) != v) {
		__atomic_store_n(w, v, __ATOMIC_RELEASE);
	}
}

static void lane_restore(uint32_t i) {
	ipc_port_t port = lane_port[i];
	if (!port) {
		return;
	}
	// another slot claiming the same owner and name would be found first
	for (uint32_t k = 0; k < IOSC_LANES_COUNT; k++) {
		if (k != i && lane_port[k] != port && lanes[k].owner == lane_owner[i] && lanes[k].name == port->ip_receiver_name &&
		    __atomic_load_n(&lanes[k].state, __ATOMIC_RELAXED) == iosc_lane_open) {
			put32(&lanes[k].state, lane_port[k] ? iosc_lane_open : iosc_lane_free);
			if (lane_port[k]) {
				put32(&lanes[k].owner, lane_owner[k]);
				put32(&lanes[k].name, lane_port[k]->ip_receiver_name);
			}
		}
	}
	iosc_lane_t* l = &lanes[i];
	put32(&l->owner, lane_owner[i]);
	put32(&l->name, port->ip_receiver_name);
	put32(&l->pset, lane_pset[i]);
	put32(&l->state, iosc_lane_open);
}

void iosc_lane_restore_port(ipc_port_t port) {
	uint32_t i = port->ip_lane;
	if (region && i && i <= IOSC_LANES_COUNT && lane_port[i - 1] == port) {
		lane_restore(i - 1);
	}
}

void iosc_lane_restore_pset(ipc_pset_t pset) {
	uint32_t s = pset->ips_lane;
	if (!region || !s || s > IOSC_LANES_PSETS || pset_obj[s - 1] != pset) {
		return;
	}
	iosc_lane_pset_t* p = &psets[s - 1];
	for (uint32_t k = 0; k < IOSC_LANES_PSETS; k++) {
		if (k != s - 1 && !pset_obj[k] && psets[k].owner == pset_owner[s - 1] && psets[k].name == pset_name[s - 1]) {
			put32(&psets[k].state, iosc_lane_free);
		}
	}
	put32(&p->owner, pset_owner[s - 1]);
	put32(&p->name, pset_name[s - 1]);
	uint32_t k = 0;
	for (uint32_t i = 0; i < IOSC_LANES_COUNT && k < IOSC_PSET_LANES; i++) {
		if (lane_pset[i] == s && lane_port[i]) {
			lane_restore(i);
			put32(&p->lanes[k++], i + 1);
		}
	}
	while (k < IOSC_PSET_LANES) {
		put32(&p->lanes[k++], 0);
	}
	put32(&p->state, iosc_lane_open);
}

void iosc_lane_pset_name(mach_port_name_t name) {
	current_thread()->iosc_pset_name = name;
}

// ipc_pset_add: both locked. Returns true when the port joined as a lane member (not linked).
bool iosc_lane_pset_add(ipc_pset_t pset, ipc_port_t port) {
	thread_t self = current_thread();
	mach_port_name_t name = self->iosc_pset_name;
	self->iosc_pset_name = MACH_PORT_NULL;
	if (!region) {
		return false;
	}
	uint32_t owner = task_lane_proc(current_space());
	if (!pset->ips_lane && name && owner && SLIST_EMPTY(&pset->ips_messages.imq_klist)) {
		pset_publish(pset, name, owner);
	}
	// only a port in no other set can be a lane member: a lane member is unlinked from its set, and
	// a second set would have to see its messages too (CFRunLoop puts its wakeup port into the set
	// of every mode)
	bool first = !port->ip_in_pset && !(port->ip_lane && lane_pset[port->ip_lane - 1]);
	if (port->ip_lane) {
		if (first && pset->ips_lane && pset_attach(pset->ips_lane, port)) {
			return true;
		}
		iosc_lane_close(port, false);  // back into the set it is a lane member of, if any
	}
	port->ip_lane_pset = first && pset->ips_lane ? pset->ips_lane : 0;
	return false;
}

// ipc_pset_remove: true when the port was a lane member of the set (nothing to unlink).
bool iosc_lane_pset_remove(ipc_pset_t pset, ipc_port_t port) {
	uint32_t s = port->ip_lane_pset;
	if (!s || s != pset->ips_lane) {
		return false;
	}
	port->ip_lane_pset = 0;
	if (port->ip_lane && lane_pset[port->ip_lane - 1] == s) {
		pset_detach(s, port->ip_lane);
		return true;
	}
	return false;
}

// ipc_pset_remove_from_all: true when the port was a lane member (of one set, now of none).
bool iosc_lane_pset_remove_all(ipc_port_t port) {
	uint32_t s = port->ip_lane_pset;
	port->ip_lane_pset = 0;
	if (s && port->ip_lane && lane_pset[port->ip_lane - 1] == s) {
		pset_detach(s, port->ip_lane);
		return true;
	}
	return false;
}

void iosc_lane_pset_gone(ipc_pset_t pset, bool relink) {
	if (region && pset->ips_lane) {
		pset_unpublish(pset, relink);
	}
}

// A port of a published set got a message while nobody waited in the server: tell the set's
// client receivers.
void iosc_lane_pset_prepost(struct waitq_set* wqset, struct waitq* wq) {
	if (!region) {
		return;
	}
	ipc_mqueue_t smq = __container_of(wqset, struct ipc_mqueue, imq_set_queue);
	ipc_pset_t pset = __container_of(smq, struct ipc_pset, ips_messages);
	for (uint32_t i = 0; i < IOSC_LANES_PSETS; i++) {
		if (pset_obj[i] != pset) {
			continue;
		}
		__atomic_add_fetch(&psets[i].server_pending, 1, __ATOMIC_SEQ_CST);
		pset_wake(i + 1, 1);
		if (psets[i].kq && psets[i].owner < IOSC_LANES_PROCS) {
			__atomic_add_fetch(&procs[psets[i].owner].doorbell, 1, __ATOMIC_SEQ_CST);
			iosc_futex_wake(&procs[psets[i].owner].doorbell, 1);
		}
		return;
	}
}

// After a send to a hot member of a published set (ip_lane_want): its lane, unlinked from the
// set; messages already queued get SERVER entries so the ring keeps their order.
void iosc_lane_member_open(ipc_port_t port) {
	ip_lock(port);
	uint32_t s = port->ip_lane_pset;
	port->ip_lane_want = 0;
	if (!region || port->ip_lane || !ip_active(port) || !port->ip_in_pset || !pset_at(s)) {
		ip_unlock(port);
		return;
	}
	ipc_pset_t pset = pset_obj[s - 1];
	iosc_lane_pset_t* p = &psets[s - 1];
	bool room = false;
	for (uint32_t k = 0; k < IOSC_PSET_LANES; k++) {
		room |= !p->lanes[k];
	}
	if (!room) {
		ip_unlock(port);
		return;
	}
	ips_lock(pset);
	if (!ips_active(pset) || ipc_mqueue_remove(&port->ip_messages, &pset->ips_messages) != KERN_SUCCESS || port->ip_in_pset) {
		// not (only) in this set any more
		ips_unlock(pset);
		ip_unlock(port);
		return;
	}
	ipc_mqueue_t mq = &port->ip_messages;
	imq_lock(mq);
	uint32_t queued = 0;
	for (ipc_kmsg_t k = ipc_kmsg_queue_first(&mq->imq_messages); k != IKM_NULL; k = ipc_kmsg_queue_next(&mq->imq_messages, k)) {
		queued++;
	}
	uint32_t lane = queued < IOSC_LANES_RING ? lane_open(port, mq) : 0;
	imq_unlock(mq);
	if (lane) {
		iosc_lane_t* l = &lanes[lane - 1];
		for (uint32_t i = 0; i < queued; i++) {
			iosc_lane_entry_t* e = lane_entry(l, i);
			e->kind = iosc_lane_server;
			e->size = 0;
			e->writer = UINT32_MAX;
			e->tid = 0;
			__atomic_store_n(&e->seq, (uint64_t)i + 1, __ATOMIC_RELEASE);
		}
		__atomic_store_n(&l->tail, queued, __ATOMIC_RELEASE);
		pset_attach(s, port);
	} else {
		// put it back
		uint64_t link = waitq_link_reserve(NULL);
		uint64_t prepost = waitq_prepost_reserve(NULL, 10, WAITQ_DONT_LOCK);
		ipc_mqueue_add(&port->ip_messages, &pset->ips_messages, &link, &prepost);
		waitq_link_release(link);
		waitq_prepost_release_reserve(prepost);
	}
	ips_unlock(pset);
	ip_unlock(port);
}

// A knote is attached to a lane port or a published set: from now on its entries ring the owner's
// doorbell, and the client serves the knote from the lane.
void iosc_lane_watch_port(ipc_port_t port) {
	port->ip_lane_watched = 1;
	iosc_lane_t* l = port_lane(port);
	if (l) {
		__atomic_store_n(&l->kq, 1, __ATOMIC_SEQ_CST);
		lane_doorbell(l);
	}
}

void iosc_lane_watch_pset(ipc_pset_t pset) {
	iosc_lane_pset_t* p = region && pset->ips_lane ? pset_at(pset->ips_lane) : NULL;
	if (p) {
		__atomic_store_n(&p->kq, 1, __ATOMIC_SEQ_CST);
		if (p->owner < IOSC_LANES_PROCS) {
			__atomic_add_fetch(&procs[p->owner].doorbell, 1, __ATOMIC_SEQ_CST);
			iosc_futex_wake(&procs[p->owner].doorbell, 1);
		}
	}
}
