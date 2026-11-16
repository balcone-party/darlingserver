// SPDX-License-Identifier: GPL-3.0-or-later
// iosc: hooks of the XNU IPC code into Mach message lanes (src/lanes.c)
#ifndef _DARLINGSERVER_DUCT_TAPE_LANES_H_
#define _DARLINGSERVER_DUCT_TAPE_LANES_H_

#include <stdbool.h>
#include <stdint.h>

struct ipc_mqueue;
struct ipc_port;
struct waitq;
struct waitq_set;
struct ipc_pset;

bool iosc_lanes_enabled(void);
struct ipc_kmsg;
void iosc_lane_note_send(struct ipc_mqueue* mqueue, struct ipc_kmsg* kmsg);
uint32_t iosc_lane_of(struct ipc_port* port, uint32_t* gen);
bool iosc_lane_backlog_full(struct ipc_port* port);
void iosc_lane_qlimit(struct ipc_port* port);
void iosc_lane_after_post(struct ipc_port* port, uint32_t lane, uint32_t gen, uint32_t ticket);
void iosc_lane_close(struct ipc_port* port, bool drop);
void iosc_lane_hint_sent(uint32_t name, struct ipc_port* port, struct ipc_port* reply, uint32_t reply_name);
bool iosc_waitq_empty(struct waitq* wq);
void iosc_lane_pset_name(uint32_t name);
bool iosc_lane_pset_add(struct ipc_pset* pset, struct ipc_port* port);
bool iosc_lane_pset_remove(struct ipc_pset* pset, struct ipc_port* port);
bool iosc_lane_pset_remove_all(struct ipc_port* port);
void iosc_lane_pset_gone(struct ipc_pset* pset, bool relink);
void iosc_lane_pset_prepost(struct waitq_set* wqset, struct waitq* wq);
void iosc_lane_member_open(struct ipc_port* port);
void iosc_lane_watch_port(struct ipc_port* port);
void iosc_lane_watch_pset(struct ipc_pset* pset);
void iosc_lane_restore_port(struct ipc_port* port);
void iosc_lane_restore_pset(struct ipc_pset* pset);
void iosc_mqueue_knote(struct ipc_mqueue* mqueue);

#endif // _DARLINGSERVER_DUCT_TAPE_LANES_H_
