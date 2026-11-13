// SPDX-License-Identifier: GPL-3.0-or-later
// iosc: Mach message lanes (docs/design/mach-fastpath.md §4 of ios-container).
//
// One shared region per server (one per device), mapped by the server and by every client
// process. A lane is a ring of messages for one receive right with steady traffic: a sender's
// loader writes a non-complex message straight into it and wakes the receiver with a futex, no
// server hop. The server stays the authority for rights; it opens and closes lanes and puts a
// SERVER entry into the ring for every message that still went through it, so that per-sender
// order holds across both paths.
//
// Everything here is offsets and indices, never pointers. Any process of the device can write
// the region: the server validates every index and size it reads from it.
#ifndef _DARLINGSERVER_LANES_H_
#define _DARLINGSERVER_LANES_H_

#include <stdint.h>

#define IOSC_LANES_MAGIC 0x4c534f49u  // "IOSL"
#define IOSC_LANES_VERSION 1u

#define IOSC_LANES_PROCS 1024u
#define IOSC_LANES_COUNT 256u
#define IOSC_LANES_RING 64u          // entries per lane, a power of two
#define IOSC_LANES_ENTRY 2048u       // bytes per entry
#define IOSC_LANES_ENTRY_HDR 64u     // entry header, the message follows
#define IOSC_LANES_MSG_MAX (IOSC_LANES_ENTRY - IOSC_LANES_ENTRY_HDR)

enum iosc_lane_state {
	iosc_lane_free = 0,
	iosc_lane_open = 1,
	iosc_lane_closing = 2,
};

enum iosc_lane_entry_kind {
	iosc_lane_inline = 1,  // the message is in the entry
	iosc_lane_server = 2,  // the next message of the port's queue in the server (take it with a receive)
};

typedef struct iosc_lanes_header {
	uint32_t magic;
	uint32_t version;
	uint64_t size;
	uint32_t procs;
	uint32_t lanes;
	uint32_t ring;
	uint32_t entry;
	uint64_t procs_off;
	uint64_t lanes_off;
	uint64_t rings_off;
	uint32_t epoch;          // bumped by the server whenever a lane opens or closes
	uint32_t reserved[3];
} iosc_lanes_header_t;

typedef struct iosc_lanes_proc {
	uint32_t pid;            // 0: free
	uint32_t alive;
	uint32_t audit[8];       // the task's audit token, as the server has it
	uint32_t reserved[6];
} iosc_lanes_proc_t;

typedef struct iosc_lane {
	uint32_t state;          // iosc_lane_state
	uint32_t gen;            // bumped on open and close
	uint32_t owner;          // process index of the receiver
	uint32_t name;           // the receive right's name in the owner's space
	uint64_t ring_off;
	uint32_t ready;          // futex word: bumped after every committed entry
	uint32_t waiters;        // receivers waiting on ready
	uint32_t writers;        // senders between their state check and their commit
	uint32_t pad0[7];
	uint64_t tail;           // producers (own cache line)
	uint64_t pad1[7];
	uint64_t head;           // consumers
	uint64_t pad2[7];
} iosc_lane_t;

typedef struct iosc_lane_entry {
	uint64_t seq;            // Vyukov bounded queue stamp
	uint32_t kind;           // iosc_lane_entry_kind
	uint32_t size;           // message size (header + body), at most IOSC_LANES_MSG_MAX
	uint32_t writer;         // process index of the sender
	uint32_t tid;
	uint32_t audit[8];       // the sender's audit token (written by the sender)
	uint32_t reserved[2];
	// followed by the message: mach_msg_header_t as the receiver sees it, then the body
} iosc_lane_entry_t;

_Static_assert(sizeof(iosc_lane_entry_t) == IOSC_LANES_ENTRY_HDR, "lane entry header");
_Static_assert(sizeof(iosc_lane_t) == 192, "lane");

static inline uint64_t iosc_lanes_size(void) {
	uint64_t procs = 4096;
	uint64_t lanes = procs + (uint64_t)IOSC_LANES_PROCS * sizeof(iosc_lanes_proc_t);
	uint64_t rings = lanes + (uint64_t)IOSC_LANES_COUNT * sizeof(iosc_lane_t);
	return rings + (uint64_t)IOSC_LANES_COUNT * IOSC_LANES_RING * IOSC_LANES_ENTRY;
}

// Written by the server to dserver_rpc_callhdr_t.lane_hint after a message to a lane port went
// through it: the sender may write later messages to that name into the lane.
typedef struct iosc_lane_hint {
	uint32_t name;           // the destination name in the sender's space, 0: no hint
	uint32_t lane;           // lane index + 1
	uint32_t gen;
	uint32_t reserved;
} iosc_lane_hint_t;

// mach_msg result: the send part (if any) is done, receive from the lane of rcv_name.
#define IOSC_RCV_IN_LANE 0x10004fffu

#endif // _DARLINGSERVER_LANES_H_
