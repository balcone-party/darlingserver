// SPDX-License-Identifier: GPL-3.0-or-later
// iosc: hooks of the XNU IPC code into Mach message lanes (src/lanes.c)
#ifndef _DARLINGSERVER_DUCT_TAPE_LANES_H_
#define _DARLINGSERVER_DUCT_TAPE_LANES_H_

#include <stdbool.h>
#include <stdint.h>

struct ipc_mqueue;
struct ipc_port;
struct waitq;

bool iosc_lanes_enabled(void);
void iosc_lane_note_send(struct ipc_mqueue* mqueue);
uint32_t iosc_lane_of(struct ipc_port* port, uint32_t* gen);
void iosc_lane_after_post(struct ipc_port* port, uint32_t lane, uint32_t gen);
void iosc_lane_close(struct ipc_port* port, bool drop);
void iosc_lane_hint_sent(uint32_t name, struct ipc_port* port);
bool iosc_waitq_empty(struct waitq* wq);

#endif // _DARLINGSERVER_DUCT_TAPE_LANES_H_
