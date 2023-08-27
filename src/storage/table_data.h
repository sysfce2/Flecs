/**
 * @file table_data.h
 * @brief Table data implementation.
 */

#ifndef FLECS_TABLE_DATA_H
#define FLECS_TABLE_DATA_H

void flecs_table_data_init(
    ecs_world_t *world,
    ecs_table_t *table,
    int32_t column_count);

int32_t flecs_table_data_append(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_entity_t entity,
    ecs_record_t *record,
    bool construct,
    bool on_add);

int32_t flecs_table_data_appendn(
    ecs_world_t *world,
    ecs_table_t *table,
    int32_t to_add,
    const ecs_entity_t *ids);

void flecs_table_data_move(
    ecs_world_t *world,
    ecs_entity_t dst_entity,
    ecs_entity_t src_entity,
    ecs_table_t *dst_table,
    int32_t dst_index,
    ecs_table_t *src_table,
    int32_t src_index,
    bool construct);

int32_t flecs_table_data_delete(
    ecs_world_t *world,
    ecs_table_t *table,
    int32_t index,
    bool destruct);

void flecs_table_data_swap(
    ecs_world_t *world,
    ecs_table_t *table,
    int32_t row_1,
    int32_t row_2);

void flecs_table_data_merge(
    ecs_world_t *world,
    ecs_table_t *dst_table,
    ecs_table_t *src_table);

bool flecs_table_data_shrink(
    ecs_world_t *world,
    ecs_table_t *table);

#endif
