/* GdoLib - A library for controlling garage door openers.
 * Copyright (C) 2024  Konnected Inc.
 * Copyright (C) 2025  Gelidus Research Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef GDO_PRIV_H
#define GDO_PRIV_H

#ifdef __cplusplus
extern "C"
{
#endif // __cplusplus

#include <esp_timer.h>
#include "gdo.h"
#include "esp_log.h"

#define RX_BUFFER_SIZE 160
#define GDO_PACKET_SIZE ((g_status.protocol == GDO_PROTOCOL_SEC_PLUS_V2) ? 19UL : 2UL)
#define GDO_DRY_CONTACT_DEBOUNCE_MS 50
#define GDO_DRY_CONTACT_PULSE_WIDTH_MS 250

// Task priority definitions for good real-time response
#define GDO_TASK_PRIORITY_HIGH 18        // Contact ISR tasks - highest priority for real-time response
#define GDO_TASK_PRIORITY_MEDIUM_HIGH 16 // Main task - medium-high priority for processing
#define GDO_TASK_PRIORITY_MEDIUM 14      // Sync task - medium priority for background sync
#define GDO_ISR_PRIORITY 2               // GPIO ISR service priority

// Task stack size definitions
#define GDO_TASK_STACK_SIZE 4096         // Standard stack size for all GDO tasks

// Command timing definitions
#define GDO_MIN_COMMAND_INTERVAL_MS 50   // Minimum interval between commands (matches internal default)

// V1 protocol validation
#define GDO_V1_CMD_IS_VALID(cmd) ((cmd) > V1_CMD_MIN && (cmd) < V1_CMD_MAX)


    typedef enum
    {
        GDO_CMD_UNKNOWN = 0x000,
        GDO_CMD_GET_STATUS = 0x080,
        GDO_CMD_STATUS = 0x081,
        GDO_CMD_OBST_1 = 0x084,
        GDO_CMD_OBST_2 = 0x085,
        GDO_CMD_BATTERY_STATUS = 0x09d,
        GDO_CMD_PAIR_3 = 0x0a0,
        GDO_CMD_PAIR_3_RESP = 0x0a1,
        GDO_CMD_LEARN = 0x181,
        GDO_CMD_LOCK = 0x18c,
        GDO_CMD_DOOR_ACTION = 0x280,
        GDO_CMD_LIGHT = 0x281,
        GDO_CMD_MOTOR_ON = 0x284,
        GDO_CMD_MOTION = 0x285,
        GDO_CMD_GET_PAIRED_DEVICES = 0x307,
        GDO_CMD_PAIRED_DEVICES = 0x308,
        GDO_CMD_CLEAR_PAIRED_DEVICES = 0x30D,
        GDO_CMD_LEARN_1 = 0x391,
        GDO_CMD_PING = 0x392,
        GDO_CMD_PING_RESP = 0x393,
        GDO_CMD_PAIR_2 = 0x400,
        GDO_CMD_PAIR_2_RESP = 0x401,
        GDO_CMD_SET_TTC = 0x402,
        GDO_CMD_CANCEL_TTC = 0x408,
        GDO_CMD_UPDATE_TTC = 0x40a,
        GDO_CMD_GET_OPENINGS = 0x48b,
        GDO_CMD_OPENINGS = 0x48c,
        GDO_CMD_MAX,
    } gdo_command_t;

    typedef enum
    {
        V1_CMD_MIN = 0x2F,
        V1_CMD_TOGGLE_DOOR_PRESS = 0x30,
        V1_CMD_TOGGLE_DOOR_RELEASE = 0x31,
        V1_CMD_TOGGLE_LIGHT_PRESS = 0x32,
        V1_CMD_TOGGLE_LIGHT_RELEASE = 0x33,
        V1_CMD_TOGGLE_LOCK_PRESS = 0x34,
        V1_CMD_TOGGLE_LOCK_RELEASE = 0x35,
        V1_CMD_QUERY_DOOR_STATUS_0x37 = 0x37,
        V1_CMD_QUERY_DOOR_STATUS = 0x38,
        V1_CMD_OBSTRUCTION = 0x39,
        V1_CMD_QUERY_OTHER_STATUS = 0x3A,
        V1_CMD_MAX = 0x3B,
    } gdo_v1_command_t;

    typedef enum
    {
        GDO_EVENT_TX_PENDING = UART_EVENT_MAX + 1,
        GDO_EVENT_SYNC_COMPLETE,
        GDO_EVENT_OBST,
        GDO_EVENT_DOOR_POSITION_UPDATE,
        GDO_EVENT_LIGHT_UPDATE,
        GDO_EVENT_LOCK_UPDATE,
        GDO_EVENT_MOTOR_UPDATE,
        GDO_EVENT_BUTTON_UPDATE,
        GDO_EVENT_BATTERY_UPDATE,
        GDO_EVENT_LEARN_UPDATE,
        GDO_EVENT_OPENINGS_UPDATE,
        GDO_EVENT_MOTION_UPDATE,
        GDO_EVENT_UPDATE_TTC,
        GDO_EVENT_SET_TTC,
        GDO_EVENT_PAIRED_DEVICES_UPDATE,
        GDO_EVENT_DOOR_OPEN_DURATION_MEASUREMENT,
        GDO_EVENT_DOOR_CLOSE_DURATION_MEASUREMENT,
        GDO_EVENT_TOF_TIMER,
        GDO_EVENT_MAX,
    } gdo_event_type_t;

    typedef enum
    {
        GDO_LIGHT_ACTION_OFF = 0,
        GDO_LIGHT_ACTION_ON,
        GDO_LIGHT_ACTION_TOGGLE,
        GDO_LIGHT_ACTION_MAX,
    } gdo_light_action_t;

    typedef enum
    {
        GDO_LOCK_ACTION_UNLOCK,
        GDO_LOCK_ACTION_LOCK,
        GDO_LOCK_ACTION_TOGGLE,
        GDO_LOCK_ACTION_MAX,
    } gdo_lock_action_t;

    typedef enum
    {
        GDO_DOOR_ACTION_CLOSE = 0,
        GDO_DOOR_ACTION_OPEN,
        GDO_DOOR_ACTION_TOGGLE,
        GDO_DOOR_ACTION_STOP,
        GDO_DOOR_ACTION_MAX,
    } gdo_door_action_t;

    typedef enum
    {
        GDO_LEARN_ACTION_DEACTIVATE = 0,
        GDO_LEARN_ACTION_ACTIVATE,
        GDO_LEARN_ACTION_MAX,
    } gdo_learn_action_t;

    typedef enum
    {
        GDO_CONTACT_UNKNOWN = 0,
        GDO_CONTACT_DOOR_OPEN,
        GDO_CONTACT_DOOR_CLOSE,
        GDO_CONTACT_DOOR_TOGGLE,
        GDO_CONTACT_MAX,
    } gdo_contact_type_t;

    typedef struct
    {
        gdo_contact_type_t contact;
        gpio_num_t pin;
        TaskHandle_t notifyTask;          // Handle of task that ISR should notify when fired
        esp_timer_handle_t debounceTimer; // Handle of timer used for debounce logic
        portMUX_TYPE mux;                 // Mutex to wrap critical sections in ISR and task
        volatile bool inDebounce;         // Set when we are in midst of debounce check
        volatile uint32_t level;          // Last known state of the GPIO pin
        uint64_t levelTimestamp;          // microseconds when level set (initial interrupt)
        uint32_t count;                   // Incremented when interrupt received during a debounce period
        uint64_t countTimestamp;          // microseconds when count last incremented (bounces)
    } gdo_contact_t;

    typedef struct
    {
        int64_t sleep_micros;
        uint32_t count;
        portMUX_TYPE mux;
    } gdo_obstruction_stats_t;

    typedef struct
    {
        gdo_command_t cmd;
        uint32_t sent_ms;
        uint8_t *packet;
    } gdo_tx_message_t;

    typedef struct
    {
        uint32_t cmd;
        bool door_cmd;
        uint8_t nibble;
        uint8_t byte1;
        uint8_t byte2;
        esp_timer_handle_t timer;
    } gdo_sched_cmd_args_t;

    typedef struct
    {
        gdo_event_type_t event;
        esp_timer_handle_t timer;
    } gdo_sched_evt_args_t;

    typedef enum
    {
        SERIAL_EVENT_DATA = GDO_EVENT_MAX + 1,
        SERIAL_EVENT_MAX,
    } serial_event_type_t;

    typedef struct
    {
        serial_event_type_t event;
        size_t size;
        uint8_t packet[20];
    } serial_event_t;

    typedef struct
    {
        TaskHandle_t sendingTask;
        size_t size;
        uint8_t packet[20];
    } serial_transmit_t;

    typedef union
    {
        gdo_event_type_t gdo_event;
        uart_event_t uart_event;
        serial_event_t serial_event;
    } gdo_event_t;

    const char *cmd_to_string(gdo_command_t cmd);
    const char *v1_cmd_to_string(gdo_v1_command_t cmd);
    void print_buffer(gdo_protocol_type_t protocol, uint8_t *buf, bool is_tx);

    extern const char *gdo_door_state_str[];
    extern const char *gdo_light_state_str[];
    extern const char *gdo_lock_state_str[];
    extern const char *gdo_motion_state_str[];
    extern const char *gdo_obstruction_state_str[];
    extern const char *gdo_motor_state_str[];
    extern const char *gdo_button_state_str[];
    extern const char *gdo_battery_state_str[];
    extern const char *gdo_learn_state_str[];
    extern const char *gdo_paired_device_type_str[];
    extern const char *gdo_light_action_str[];
    extern const char *gdo_lock_action_str[];
    extern const char *gdo_protocol_type_str[];

#ifdef __cplusplus
}
#endif // __cplusplus
#endif // GDO_PRIV_H
