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
	return (iosc_lane_entry_t*)(region + l->ring_off + (pos & (IOSC_LANES_RING - 1)) * IOSC_LANES_ENTRY);
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

// Under the mqueue lock, before a message is queued to the port: count, and open a lane when the
// port is hot, has no queued messages and is plain (no set, no knote, a guest receiver).
// Receivers waiting in the server are sent away (MACH_RCV_PORT_CHANGED, which their receive
// turns into IOSC_RCV_IN_LANE).
void iosc_lane_note_send(ipc_mqueue_t mqueue) {
	if (!region) {
		return;
	}
	ipc_port_t port = ip_from_mq(mqueue);
	if (port->ip_lane) {
		return;
	}
	uint64_t now = mach_absolute_time();
	if (now - port->ip_lane_t0 > NSEC_PER_SEC) {
		port->ip_lane_t0 = now;
		port->ip_lane_count = 0;
	}
	if (++port->ip_lane_count < LANE_OPEN_RATE) {
		return;
	}
	if (!ip_active(port) || port->ip_specialreply || ip_kotype(port) != IKOT_NONE || port->ip_in_pset ||
	    !port->ip_receiver_name || !SLIST_EMPTY(&mqueue->imq_klist) ||
	    !ipc_kmsg_queue_empty(&mqueue->imq_messages)) {
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
		l->name = port->ip_receiver_name;
		__atomic_add_fetch(&l->gen, 1, __ATOMIC_RELEASE);
		__atomic_store_n(&l->state, iosc_lane_open, __ATOMIC_RELEASE);
		port->ip_lane = i + 1;
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

// After a message was queued to a lane port through the server: its SERVER entry. A full ring
// waits as a full queue would.
void iosc_lane_after_post(ipc_port_t port, uint32_t lane, uint32_t gen) {
	iosc_lane_t* l = &lanes[lane - 1];
	for (;;) {
		if (port->ip_lane != lane || __atomic_load_n(&l->gen, __ATOMIC_ACQUIRE) != gen ||
		    __atomic_load_n(&l->state, __ATOMIC_ACQUIRE) != iosc_lane_open) {
			return;  // closed meanwhile: the message is an ordinary queued one now
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
				return;
			}
		} else if ((int64_t)(seq - pos) < 0) {
			delay(200);  // full
		}
	}
};

void dtape_set_lane_hint(uint64_t addr) {
	current_thread()->iosc_lane_hint = addr;
};

static bool port_plain(ipc_port_t port) {
	return ip_active(port) && !port->ip_specialreply && ip_kotype(port) == IKOT_NONE && !port->ip_in_pset &&
	    port->ip_receiver_name && SLIST_EMPTY(&port->ip_messages.imq_klist);
}

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
		if (port_plain(reply) && reply->ip_receiver == current_space() && reply->ip_receiver_name == reply_name) {
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

// An INLINE entry as a queued kmsg: the destination holds a send right, as after a copyin.
static ipc_kmsg_t lane_kmsg(ipc_port_t port, iosc_lane_entry_t* e) {
	uint32_t size = e->size;
	if (size < sizeof(mach_msg_header_t) || size > IOSC_LANES_MSG_MAX || (size & 3)) {
		return IKM_NULL;
	}
	mach_msg_header_t* src = (mach_msg_header_t*)((uint8_t*)e + IOSC_LANES_ENTRY_HDR);
	ipc_kmsg_t kmsg = ipc_kmsg_alloc(size + MAX_TRAILER_SIZE);
	if (kmsg == IKM_NULL) {
		return IKM_NULL;
	}
	memcpy(kmsg->ikm_header, src, size);
	ikm_qos_init(kmsg);
	kmsg->ikm_header->msgh_size = size;
	kmsg->ikm_header->msgh_bits = MACH_MSGH_BITS(MACH_MSG_TYPE_PORT_SEND, 0);
	kmsg->ikm_header->msgh_remote_port = ipc_port_make_send_locked(port);
	kmsg->ikm_header->msgh_local_port = IP_NULL;
	kmsg->ikm_header->msgh_voucher_port = MACH_PORT_NULL;
	mach_msg_max_trailer_t* trailer = (mach_msg_max_trailer_t*)((vm_offset_t)kmsg->ikm_header + size);
	bzero(trailer, sizeof(*trailer));
	memcpy(&trailer->msgh_audit, e->audit, sizeof(trailer->msgh_audit));
	trailer->msgh_sender.val[0] = e->audit[1];
	trailer->msgh_sender.val[1] = e->audit[2];
	trailer->msgh_trailer_type = MACH_MSG_TRAILER_FORMAT_0;
	trailer->msgh_trailer_size = MACH_MSG_TRAILER_MINIMUM_SIZE;
	return kmsg;
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
			got[n].kind = e->kind;
			got[n].kmsg = e->kind == iosc_lane_inline ? lane_kmsg(port, e) : IKM_NULL;
			servers += e->kind == iosc_lane_server;
			if (e->kind == iosc_lane_server || got[n].kmsg != IKM_NULL) {
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
		imq_unlock(mq);
	}

	uint32_t lane = port->ip_lane;
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
};
