/**
 * @file table.c
 * @brief Table storage implementation.
 * 
 * Tables are the data structure that store the component data. Tables have
 * columns for each component in the table, and rows for each entity stored in
 * the table. Once created, the component list for a table doesn't change, but
 * entities can move from one table to another.
 * 
 * Each table has a type, which is a vector with the (component) ids in the 
 * table. The vector is sorted by id, which ensures that there can be only one
 * table for each unique combination of components.
 * 
 * Not all ids in a table have to be components. Tags are ids that have no
 * data type associated with them, and as a result don't need to be explicitly
 * stored beyond an element in the table type. To save space and speed up table
 * creation, each table has a reference to a "storage table", which is a table
 * that only includes component ids (so excluding tags).
 * 
 * Note that the actual data is not stored on the storage table. The storage 
 * table is only used for sharing administration. A column_map member maps
 * between column indices of the table and its storage table. Tables are 
 * refcounted, which ensures that storage tables won't be deleted if other
 * tables have references to it.
 */

#include "../private_api.h"

/* Table sanity check to detect storage issues. Only enabled in SANITIZE mode as
 * this can severly slow down many ECS operations. */
#ifdef FLECS_SANITIZE
static
void flecs_table_check_sanity(ecs_table_t *table) {
    ecs_table_data_t *data = table->data;
    int32_t size = ecs_vec_size(&data->entities);
    int32_t count = ecs_vec_count(&data->entities);
    
    ecs_assert(size == ecs_vec_size(&data->records), 
        ECS_INTERNAL_ERROR, NULL);
    ecs_assert(count == ecs_vec_count(&data->records), 
        ECS_INTERNAL_ERROR, NULL);

    int32_t i;
    int32_t bs_offset = table->_ ? table->data->bs_offset : 0;
    int32_t bs_count = table->_ ? table->data->bs_count : 0;
    int32_t type_count = table->type.count;
    ecs_id_t *ids = table->type.array;

    ecs_assert((bs_count + bs_offset) <= type_count, ECS_INTERNAL_ERROR, NULL);

    int32_t column_count = table->data->column_count;
    if (column_count) {
        ecs_assert(type_count >= column_count, ECS_INTERNAL_ERROR, NULL);

        int32_t *column_map = table->column_map;
        ecs_assert(column_map != NULL, ECS_INTERNAL_ERROR, NULL);

        ecs_assert(table->data->columns != NULL, ECS_INTERNAL_ERROR, NULL);

        for (i = 0; i < column_count; i ++) {
            ecs_vec_t *column = &table->data->columns[i].data;
            ecs_assert(size == column->size, ECS_INTERNAL_ERROR, NULL);
            ecs_assert(count == column->count, ECS_INTERNAL_ERROR, NULL);
            int32_t column_map_id = column_map[i + type_count];
            ecs_assert(column_map_id >= 0, ECS_INTERNAL_ERROR, NULL);
        }
    } else {
        ecs_assert(table->column_map == NULL, ECS_INTERNAL_ERROR, NULL);
    }

    if (bs_count) {
        ecs_assert(data->bs_columns != NULL, 
            ECS_INTERNAL_ERROR, NULL);
        for (i = 0; i < bs_count; i ++) {
            ecs_bitset_t *bs = &data->bs_columns[i];
            ecs_assert(flecs_bitset_count(bs) == count,
                ECS_INTERNAL_ERROR, NULL);
            ecs_assert(ECS_HAS_ID_FLAG(ids[i + bs_offset], TOGGLE),
                ECS_INTERNAL_ERROR, NULL);
        }
    }

    ecs_assert((table->_->traversable_count == 0) || 
        (table->flags & EcsTableHasTraversable), ECS_INTERNAL_ERROR, NULL);
}
#else
#define flecs_table_check_sanity(table)
#endif

ecs_table_data_t* flecs_table_data(
    const ecs_table_t *table)
{
    ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(table->data != NULL, ECS_INTERNAL_ERROR, NULL);
    return table->data;
}

ecs_column_t* flecs_table_columns(
    const ecs_table_t *table)
{
    return flecs_table_data(table)->columns;
}

ecs_vec_t* flecs_table_entities(
    const ecs_table_t *table)
{
    return &flecs_table_data(table)->entities;
}

ecs_vec_t* flecs_table_records(
    const ecs_table_t *table)
{
    return &flecs_table_data(table)->records;
}

ecs_entity_t* flecs_table_entities_array(
    const ecs_table_t *table)
{
    return ecs_vec_first(flecs_table_entities(table));
}

ecs_record_t** flecs_table_records_array(
    const ecs_table_t *table)
{
    return ecs_vec_first(flecs_table_records(table));
}

ecs_column_t* flecs_table_column(
    const ecs_table_t *table,
    int32_t column)
{
    ecs_assert(column < table->data->column_count, ECS_INTERNAL_ERROR, NULL);
    return &flecs_table_columns(table)[column];
}

/* Initialize table flags. Table flags are used in lots of scenarios to quickly
 * check the features of a table without having to inspect the table type. Table
 * flags are typically used to early-out of potentially expensive operations. */
static
void flecs_table_init_flags(
    ecs_world_t *world,
    ecs_table_t *table)
{
    ecs_id_t *ids = table->type.array;
    int32_t count = table->type.count;

    int32_t i;
    for (i = 0; i < count; i ++) {
        ecs_id_t id = ids[i];

        if (id <= EcsLastInternalComponentId) {
            table->flags |= EcsTableHasBuiltins;
        }

        if (id == EcsModule) {
            table->flags |= EcsTableHasBuiltins;
            table->flags |= EcsTableHasModule;
        } else if (id == EcsPrefab) {
            table->flags |= EcsTableIsPrefab;
        } else if (id == EcsDisabled) {
            table->flags |= EcsTableIsDisabled;
        } else {
            if (ECS_IS_PAIR(id)) {
                ecs_entity_t r = ECS_PAIR_FIRST(id);

                table->flags |= EcsTableHasPairs;

                if (r == EcsIsA) {
                    table->flags |= EcsTableHasIsA;
                } else if (r == EcsChildOf) {
                    table->flags |= EcsTableHasChildOf;
                    ecs_entity_t obj = ecs_pair_second(world, id);
                    ecs_assert(obj != 0, ECS_INTERNAL_ERROR, NULL);

                    if (obj == EcsFlecs || obj == EcsFlecsCore || 
                        ecs_has_id(world, obj, EcsModule)) 
                    {
                        /* If table contains entities that are inside one of the 
                         * builtin modules, it contains builtin entities */
                        table->flags |= EcsTableHasBuiltins;
                        table->flags |= EcsTableHasModule;
                    }
                } else if (id == ecs_pair_t(EcsIdentifier, EcsName)) {
                    table->flags |= EcsTableHasName;
                } else if (r == ecs_id(EcsTarget)) {
                    table->flags |= EcsTableHasTarget;
                    table->_->ft_offset = flecs_ito(int16_t, i);
                } else if (r == ecs_id(EcsPoly)) {
                    table->flags |= EcsTableHasBuiltins;
                }
            } else {
                if (ECS_HAS_ID_FLAG(id, TOGGLE)) {
                    if (!(table->flags & EcsTableHasToggle)) {
                        table->_->bs_offset = flecs_ito(int16_t, i);
                    }
                    table->flags |= EcsTableHasToggle;
                }
                if (ECS_HAS_ID_FLAG(id, OVERRIDE)) {
                    table->flags |= EcsTableHasOverrides;
                }
            }
        } 
    }
}

/* Utility function that appends an element to the table record array */
static
void flecs_table_append_to_records(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_vec_t *records,
    ecs_id_t id,
    int32_t column)
{
    /* To avoid a quadratic search, use the O(1) lookup that the index
     * already provides. */
    ecs_id_record_t *idr = flecs_id_record_ensure(world, id);
    ecs_table_record_t *tr = (ecs_table_record_t*)flecs_id_record_get_table(
            idr, table);
    if (!tr) {
        tr = ecs_vec_append_t(&world->allocator, records, ecs_table_record_t);
        tr->index = flecs_ito(int16_t, column);
        tr->count = 1;

        ecs_table_cache_insert(&idr->cache, table, &tr->hdr);
    } else {
        tr->count ++;
    }

    ecs_assert(tr->hdr.cache != NULL, ECS_INTERNAL_ERROR, NULL);
}

/* Main table initialization function */
void flecs_table_init(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_table_t *from)
{
    table->data = ecs_os_calloc_t(ecs_table_data_t);

    /* Make sure table->flags is initialized */
    flecs_table_init_flags(world, table);

    /* The following code walks the table type to discover which id records the
     * table needs to register table records with. 
     *
     * In addition to registering itself with id records for each id in the
     * table type, a table also registers itself with wildcard id records. For
     * example, if a table contains (Eats, Apples), it will register itself with
     * wildcard id records (Eats, *),  (*, Apples) and (*, *). This makes it
     * easier for wildcard queries to find the relevant tables. */

    int32_t dst_i = 0, dst_count = table->type.count;
    int32_t src_i = 0, src_count = 0;
    ecs_id_t *dst_ids = table->type.array;
    ecs_id_t *src_ids = NULL;
    ecs_table_record_t *tr = NULL, *src_tr = NULL;
    if (from) {
        src_count = from->type.count;
        src_ids = from->type.array;
        src_tr = from->_->records;
    }

    /* We don't know in advance how large the records array will be, so use
     * cached vector. This eliminates unnecessary allocations, and/or expensive
     * iterations to determine how many records we need. */
    ecs_allocator_t *a = &world->allocator;
    ecs_vec_t *records = &world->store.records;
    ecs_vec_reset_t(a, records, ecs_table_record_t);
    ecs_id_record_t *idr, *childof_idr = NULL;

    int32_t last_id = -1; /* Track last regular (non-pair) id */
    int32_t first_pair = -1; /* Track the first pair in the table */
    int32_t first_role = -1; /* Track first id with role */

    /* Scan to find boundaries of regular ids, pairs and roles */
    for (dst_i = 0; dst_i < dst_count; dst_i ++) {
        ecs_id_t dst_id = dst_ids[dst_i];
        if (first_pair == -1 && ECS_IS_PAIR(dst_id)) {
            first_pair = dst_i;
        }
        if ((dst_id & ECS_COMPONENT_MASK) == dst_id) {
            last_id = dst_i;
        } else if (first_role == -1 && !ECS_IS_PAIR(dst_id)) {
            first_role = dst_i;
        }
    }

    /* The easy part: initialize a record for every id in the type */
    for (dst_i = 0; (dst_i < dst_count) && (src_i < src_count); ) {
        ecs_id_t dst_id = dst_ids[dst_i];
        ecs_id_t src_id = src_ids[src_i];

        idr = NULL;

        if (dst_id == src_id) {
            ecs_assert(src_tr != NULL, ECS_INTERNAL_ERROR, NULL);
            idr = (ecs_id_record_t*)src_tr[src_i].hdr.cache;
        } else if (dst_id < src_id) {
            idr = flecs_id_record_ensure(world, dst_id);
        }
        if (idr) {
            tr = ecs_vec_append_t(a, records, ecs_table_record_t);
            tr->hdr.cache = (ecs_table_cache_t*)idr;
            tr->index = flecs_ito(int16_t, dst_i);
            tr->count = 1;
        }

        dst_i += dst_id <= src_id;
        src_i += dst_id >= src_id;
    }

    /* Add remaining ids that the "from" table didn't have */
    for (; (dst_i < dst_count); dst_i ++) {
        ecs_id_t dst_id = dst_ids[dst_i];
        tr = ecs_vec_append_t(a, records, ecs_table_record_t);
        idr = flecs_id_record_ensure(world, dst_id);
        tr->hdr.cache = (ecs_table_cache_t*)idr;
        ecs_assert(tr->hdr.cache != NULL, ECS_INTERNAL_ERROR, NULL);
        tr->index = flecs_ito(int16_t, dst_i);
        tr->count = 1;
    }

    /* We're going to insert records from the vector into the index that
     * will get patched up later. To ensure the record pointers don't get
     * invalidated we need to grow the vector so that it won't realloc as
     * we're adding the next set of records */
    if (first_role != -1 || first_pair != -1) {
        int32_t start = first_role;
        if (first_pair != -1 && (start != -1 || first_pair < start)) {
            start = first_pair;
        }

        /* Total number of records can never be higher than
         * - number of regular (non-pair) ids +
         * - three records for pairs: (R,T), (R,*), (*,T)
         * - one wildcard (*), one any (_) and one pair wildcard (*,*) record
         * - one record for (ChildOf, 0)
         */
        int32_t flag_id_count = dst_count - start;
        int32_t record_count = start + 3 * flag_id_count + 3 + 1;
        ecs_vec_set_min_size_t(a, records, ecs_table_record_t, record_count);
    }

    /* Add records for ids with roles (used by cleanup logic) */
    if (first_role != -1) {
        for (dst_i = first_role; dst_i < dst_count; dst_i ++) {
            ecs_id_t id = dst_ids[dst_i];
            if (!ECS_IS_PAIR(id)) {
                ecs_entity_t first = 0;
                ecs_entity_t second = 0;
                if (ECS_HAS_ID_FLAG(id, PAIR)) {
                    first = ECS_PAIR_FIRST(id);
                    second = ECS_PAIR_SECOND(id);
                } else {
                    first = id & ECS_COMPONENT_MASK;
                }
                if (first) {
                    flecs_table_append_to_records(world, table, records, 
                        ecs_pair(EcsFlag, first), dst_i);
                }
                if (second) {
                    flecs_table_append_to_records(world, table, records, 
                        ecs_pair(EcsFlag, second), dst_i);
                }
            }
        }
    }

    int32_t last_pair = -1;
    bool has_childof = table->flags & EcsTableHasChildOf;
    if (first_pair != -1) {
        /* Add a (Relationship, *) record for each relationship. */
        ecs_entity_t r = 0;
        for (dst_i = first_pair; dst_i < dst_count; dst_i ++) {
            ecs_id_t dst_id = dst_ids[dst_i];
            if (!ECS_IS_PAIR(dst_id)) {
                break; /* no more pairs */
            }
            if (r != ECS_PAIR_FIRST(dst_id)) { /* New relationship, new record */
                tr = ecs_vec_get_t(records, ecs_table_record_t, dst_i);

                ecs_id_record_t *p_idr = (ecs_id_record_t*)tr->hdr.cache;
                r = ECS_PAIR_FIRST(dst_id);
                if (r == EcsChildOf) {
                    childof_idr = p_idr;
                }

                idr = p_idr->parent; /* (R, *) */
                ecs_assert(idr != NULL, ECS_INTERNAL_ERROR, NULL);

                tr = ecs_vec_append_t(a, records, ecs_table_record_t);
                tr->hdr.cache = (ecs_table_cache_t*)idr;
                tr->index = flecs_ito(int16_t, dst_i);
                tr->count = 0;
            }

            ecs_assert(tr != NULL, ECS_INTERNAL_ERROR, NULL);
            tr->count ++;
        }

        last_pair = dst_i;

        /* Add a (*, Target) record for each relationship target. Type
         * ids are sorted relationship-first, so we can't simply do a single linear 
         * scan to find all occurrences for a target. */
        for (dst_i = first_pair; dst_i < last_pair; dst_i ++) {
            ecs_id_t dst_id = dst_ids[dst_i];
            ecs_id_t tgt_id = ecs_pair(EcsWildcard, ECS_PAIR_SECOND(dst_id));

            flecs_table_append_to_records(
                world, table, records, tgt_id, dst_i);
        }
    }

    /* Lastly, add records for all-wildcard ids */
    if (last_id >= 0) {
        tr = ecs_vec_append_t(a, records, ecs_table_record_t);
        tr->hdr.cache = (ecs_table_cache_t*)world->idr_wildcard;
        tr->index = 0;
        tr->count = flecs_ito(int16_t, last_id + 1);
    }
    if (last_pair - first_pair) {
        tr = ecs_vec_append_t(a, records, ecs_table_record_t);
        tr->hdr.cache = (ecs_table_cache_t*)world->idr_wildcard_wildcard;
        tr->index = flecs_ito(int16_t, first_pair);
        tr->count = flecs_ito(int16_t, last_pair - first_pair);
    }
    if (dst_count) {
        tr = ecs_vec_append_t(a, records, ecs_table_record_t);
        tr->hdr.cache = (ecs_table_cache_t*)world->idr_any;
        tr->index = 0;
        tr->count = 1;
    }
    if (dst_count && !has_childof) {
        tr = ecs_vec_append_t(a, records, ecs_table_record_t);
        childof_idr = world->idr_childof_0;
        tr->hdr.cache = (ecs_table_cache_t*)childof_idr;
        tr->index = 0;
        tr->count = 1;
    }

    /* Now that all records have been added, copy them to array */
    int32_t i, dst_record_count = ecs_vec_count(records);
    ecs_table_record_t *dst_tr = flecs_wdup_n(world, ecs_table_record_t, 
        dst_record_count, ecs_vec_first_t(records, ecs_table_record_t));
    table->_->record_count = flecs_ito(int16_t, dst_record_count);
    table->_->records = dst_tr;
    int32_t column_count = 0;

    /* Register & patch up records */
    for (i = 0; i < dst_record_count; i ++) {
        tr = &dst_tr[i];
        idr = (ecs_id_record_t*)dst_tr[i].hdr.cache;
        ecs_assert(idr != NULL, ECS_INTERNAL_ERROR, NULL);

        if (ecs_table_cache_get(&idr->cache, table)) {
            /* If this is a target wildcard record it has already been 
             * registered, but the record is now at a different location in
             * memory. Patch up the linked list with the new address */
            ecs_table_cache_replace(&idr->cache, table, &tr->hdr);
        } else {
            /* Other records are not registered yet */
            ecs_assert(idr != NULL, ECS_INTERNAL_ERROR, NULL);
            ecs_table_cache_insert(&idr->cache, table, &tr->hdr);
        }

        /* Claim id record so it stays alive as long as the table exists */
        flecs_id_record_claim(world, idr);

        /* Initialize event flags */
        table->flags |= idr->flags & EcsIdEventMask;

        /* Initialize column index (will be overwritten by init_columns) */
        tr->column = -1;

        if (idr->flags & EcsIdAlwaysOverride) {
            table->flags |= EcsTableHasOverrides;
        }

        if ((i < table->type.count) && (idr->type_info != NULL)) {
            column_count ++;
        }
    }

    if (column_count) {
        table->column_map = flecs_walloc_n(world, int32_t, 
            dst_count + column_count);
    }

    flecs_table_data_init(world, table, column_count);

    if (table->flags & EcsTableHasName) {
        ecs_assert(childof_idr != NULL, ECS_INTERNAL_ERROR, NULL);
        table->_->name_index = 
            flecs_id_record_name_index_ensure(world, childof_idr);
        ecs_assert(table->_->name_index != NULL, ECS_INTERNAL_ERROR, NULL);
    }

    if (table->flags & EcsTableHasOnTableCreate) {
        flecs_emit(world, world, &(ecs_event_desc_t) {
            .ids = &table->type,
            .event = EcsOnTableCreate,
            .table = table,
            .flags = EcsEventTableOnly,
            .observable = world
        });
    }
}

/* Unregister table from id records */
static
void flecs_table_records_unregister(
    ecs_world_t *world,
    ecs_table_t *table)
{
    uint64_t table_id = table->id;
    int32_t i, count = table->_->record_count;
    for (i = 0; i < count; i ++) {
        ecs_table_record_t *tr = &table->_->records[i];
        ecs_table_cache_t *cache = tr->hdr.cache;
        ecs_id_t id = ((ecs_id_record_t*)cache)->id;

        ecs_assert(tr->hdr.cache == cache, ECS_INTERNAL_ERROR, NULL);
        ecs_assert(tr->hdr.table == table, ECS_INTERNAL_ERROR, NULL);
        ecs_assert(flecs_id_record_get(world, id) == (ecs_id_record_t*)cache,
            ECS_INTERNAL_ERROR, NULL);
        (void)id;

        ecs_table_cache_remove(cache, table_id, &tr->hdr);
        flecs_id_record_release(world, (ecs_id_record_t*)cache);
    }

    flecs_wfree_n(world, ecs_table_record_t, count, table->_->records);
}

/* Keep track for what kind of builtin events observers are registered that can
 * potentially match the table. This allows code to early out of calling the
 * emit function that notifies observers. */
static
void flecs_table_add_trigger_flags(
    ecs_world_t *world, 
    ecs_table_t *table, 
    ecs_entity_t event) 
{
    (void)world;

    if (event == EcsOnAdd) {
        table->flags |= EcsTableHasOnAdd;
    } else if (event == EcsOnRemove) {
        table->flags |= EcsTableHasOnRemove;
    } else if (event == EcsOnSet) {
        table->flags |= EcsTableHasOnSet;
    } else if (event == EcsUnSet) {
        table->flags |= EcsTableHasUnSet;
    } else if (event == EcsOnTableFill) {
        table->flags |= EcsTableHasOnTableFill;
    } else if (event == EcsOnTableEmpty) {
        table->flags |= EcsTableHasOnTableEmpty;
    }
}

/* Invoke type hook for entities in table */
static
void flecs_table_invoke_hook(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_iter_action_t callback,
    ecs_entity_t event,
    ecs_column_t *column,
    ecs_entity_t *entities,
    int32_t row,
    int32_t count)
{
    void *ptr = ecs_vec_get(&column->data, column->size, row);
    flecs_invoke_hook(world, table, count, row, entities, ptr, column->id, 
        column->ti, event, callback);
}

/* Construct components */
static
void flecs_table_invoke_ctor(
    ecs_column_t *column,
    int32_t row,
    int32_t count)
{
    ecs_type_info_t *ti = column->ti;
    ecs_assert(ti != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_xtor_t ctor = ti->hooks.ctor;
    if (ctor) {
        void *ptr = ecs_vec_get(&column->data, column->size, row);
        ctor(ptr, count, ti);
    }
}

/* Destruct components */
static
void flecs_table_invoke_dtor(
    ecs_column_t *column,
    int32_t row,
    int32_t count)
{
    ecs_type_info_t *ti = column->ti;
    ecs_assert(ti != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_xtor_t dtor = ti->hooks.dtor;
    if (dtor) {
        void *ptr = ecs_vec_get(&column->data, column->size, row);
        dtor(ptr, count, ti);
    }
}

/* Run hooks that get invoked when component is added to entity */
static
void flecs_table_invoke_add_hooks(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_column_t *column,
    ecs_entity_t *entities,
    int32_t row,
    int32_t count,
    bool construct)
{
    ecs_type_info_t *ti = column->ti;
    ecs_assert(ti != NULL, ECS_INTERNAL_ERROR, NULL);

    if (construct) {
        flecs_table_invoke_ctor(column, row, count);
    }

    ecs_iter_action_t on_add = ti->hooks.on_add;
    if (on_add) {
        flecs_table_invoke_hook(world, table, on_add, EcsOnAdd, column,
            entities, row, count);
    }
}

/* Run hooks that get invoked when component is removed from entity */
static
void flecs_table_invoke_remove_hooks(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_column_t *column,
    ecs_entity_t *entities,
    int32_t row,
    int32_t count,
    bool dtor)
{
    ecs_type_info_t *ti = column->ti;
    ecs_assert(ti != NULL, ECS_INTERNAL_ERROR, NULL);

    ecs_iter_action_t on_remove = ti->hooks.on_remove;
    if (on_remove) {
        flecs_table_invoke_hook(world, table, on_remove, EcsOnRemove, column,
            entities, row, count);
    }
    
    if (dtor) {
        flecs_table_invoke_dtor(column, row, count);
    }
}

/* Destruct all components and/or delete all entities in table in range */
static
void flecs_table_dtor_all(
    ecs_world_t *world,
    ecs_table_t *table,
    int32_t row,
    int32_t count,
    bool update_entity_index,
    bool is_delete)
{
    /* Can't delete and not update the entity index */
    ecs_assert(!is_delete || update_entity_index, ECS_INTERNAL_ERROR, NULL);

    ecs_table_data_t *data = flecs_table_data(table);
    int32_t ids_count = data->column_count;
    ecs_record_t **records = data->records.array;
    ecs_entity_t *entities = data->entities.array;
    int32_t i, c, end = row + count;

    (void)records;

    if (is_delete && table->_->traversable_count) {
        /* If table contains monitored entities with traversable relationships,
         * make sure to invalidate observer cache */
        flecs_emit_propagate_invalidate(world, table, row, count);
    }

    /* If table has components with destructors, iterate component columns */
    if (table->flags & EcsTableHasDtors) {
        /* Throw up a lock just to be sure */
        table->_->lock = true;

        /* Run on_remove callbacks first before destructing components */
        for (c = 0; c < ids_count; c++) {
            ecs_column_t *column = &data->columns[c];
            ecs_iter_action_t on_remove = column->ti->hooks.on_remove;
            if (on_remove) {
                flecs_table_invoke_hook(world, table, on_remove, EcsOnRemove, 
                    column, &entities[row], row, count);
            }
        }

        /* Destruct components */
        for (c = 0; c < ids_count; c++) {
            flecs_table_invoke_dtor(&data->columns[c], row, count);
        }

        /* Iterate entities first, then components. This ensures that only one
         * entity is invalidated at a time, which ensures that destructors can
         * safely access other entities. */
        for (i = row; i < end; i ++) {
            /* Update entity index after invoking destructors so that entity can
             * be safely used in destructor callbacks. */
            if (update_entity_index) {
                ecs_entity_t e = entities[i];
                ecs_assert(!e || ecs_is_valid(world, e), 
                    ECS_INTERNAL_ERROR, NULL);
                ecs_assert(!e || records[i] == flecs_entities_get(world, e), 
                    ECS_INTERNAL_ERROR, NULL);
                ecs_assert(!e || records[i]->table == table, 
                    ECS_INTERNAL_ERROR, NULL);

                if (is_delete) {
                    flecs_entities_remove(world, e);
                    ecs_assert(ecs_is_valid(world, e) == false, 
                        ECS_INTERNAL_ERROR, NULL);
                } else {
                    // If this is not a delete, clear the entity index record
                    records[i]->table = NULL;
                    records[i]->row = 0;
                }
            } else {
                /* This should only happen in rare cases, such as when the data
                 * cleaned up is not part of the world (like with snapshots) */
            }
        }

        table->_->lock = false;

    /* If table does not have destructors, just update entity index */
    } else if (update_entity_index) {
        if (is_delete) {
            for (i = row; i < end; i ++) {
                ecs_entity_t e = entities[i];
                ecs_assert(!e || ecs_is_valid(world, e), ECS_INTERNAL_ERROR, NULL);
                ecs_assert(!e || records[i] == flecs_entities_get(world, e), 
                    ECS_INTERNAL_ERROR, NULL);
                ecs_assert(!e || records[i]->table == table, 
                    ECS_INTERNAL_ERROR, NULL);

                flecs_entities_remove(world, e);
                ecs_assert(!ecs_is_valid(world, e), ECS_INTERNAL_ERROR, NULL);
            } 
        } else {
            for (i = row; i < end; i ++) {
                ecs_entity_t e = entities[i];
                ecs_assert(!e || ecs_is_valid(world, e), ECS_INTERNAL_ERROR, NULL);
                ecs_assert(!e || records[i] == flecs_entities_get(world, e), 
                    ECS_INTERNAL_ERROR, NULL);
                ecs_assert(!e || records[i]->table == table, 
                    ECS_INTERNAL_ERROR, NULL);
                records[i]->table = NULL;
                records[i]->row = records[i]->row & ECS_ROW_FLAGS_MASK;
                (void)e;
            }
        }      
    }
}

/* Cleanup table storage */
static
void flecs_table_fini_data(
    ecs_world_t *world,
    ecs_table_t *table,
    bool do_on_remove,
    bool update_entity_index,
    bool is_delete,
    bool deactivate)
{
    ecs_assert(!table->_->lock, ECS_LOCKED_STORAGE, NULL);
    ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_table_data_t *data = table->data;
    ecs_assert(data != NULL, ECS_INTERNAL_ERROR, NULL);

    int32_t count = ecs_table_count(table);
    if (count) {
        if (do_on_remove) {
            flecs_notify_on_remove(world, table, NULL, 0, count, &table->type);
        }

        flecs_table_dtor_all(world, table, 0, count, 
            update_entity_index, is_delete);
    }

    /* Sanity check */
    ecs_assert(data->records.count == 
        data->entities.count, ECS_INTERNAL_ERROR, NULL);

    ecs_column_t *columns = data->columns;
    if (columns) {
        int32_t c, column_count = table->data->column_count;
        for (c = 0; c < column_count; c ++) {
            /* Sanity check */
            ecs_assert(columns[c].data.count == data->entities.count,
                ECS_INTERNAL_ERROR, NULL);
            ecs_vec_fini(&world->allocator,
                &columns[c].data, columns[c].size);
        }
        flecs_wfree_n(world, ecs_column_t, column_count, columns);
        data->columns = NULL;
    }

    ecs_bitset_column_t *bs_columns = data->bitsets;
    if (bs_columns) {
        int32_t c, column_count = data->bs_count;
        for (c = 0; c < column_count; c ++) {
            flecs_bitset_fini(&bs_columns[c].data);
        }
        flecs_wfree_n(world, ecs_bitset_t, column_count, bs_columns);
        data->bitsets = NULL;
    }

    ecs_vec_fini_t(&world->allocator, &data->entities, ecs_entity_t);
    ecs_vec_fini_t(&world->allocator, &data->records, ecs_record_t*);

    if (deactivate && count) {
        flecs_table_set_empty(world, table);
    }

    table->_->traversable_count = 0;
    table->flags &= ~EcsTableHasTraversable;
}

/* Cleanup, no OnRemove, don't update entity index, don't deactivate table */
static
void flecs_table_clear_data(
    ecs_world_t *world,
    ecs_table_t *table)
{
    flecs_table_fini_data(world, table, false, false, false, false);
}

/* Cleanup, run OnRemove, clear entity index (don't delete), deactivate table */
void flecs_table_clear_entities(
    ecs_world_t *world,
    ecs_table_t *table)
{
    flecs_table_fini_data(world, table, true, true, false, true);
}

/* Cleanup, run OnRemove, delete from entity index, deactivate table */
void flecs_table_delete_entities(
    ecs_world_t *world,
    ecs_table_t *table)
{
    flecs_table_fini_data(world, table, true, true, true, true);
}

/* Free table resources. */
void flecs_table_free(
    ecs_world_t *world,
    ecs_table_t *table)
{
    ecs_allocator_t *a = &world->allocator;
    bool is_root = table == &world->store.root;
    ecs_assert(!table->_->lock, ECS_LOCKED_STORAGE, NULL);
    ecs_assert(is_root || table->id != 0, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(is_root || flecs_sparse_is_alive(&world->store.tables, table->id),
        ECS_INTERNAL_ERROR, NULL);
    (void)world;

    if (!is_root && !(world->flags & EcsWorldQuit)) {
        if (table->flags & EcsTableHasOnTableDelete) {
            flecs_emit(world, world, &(ecs_event_desc_t) {
                .ids = &table->type,
                .event = EcsOnTableDelete,
                .table = table,
                .flags = EcsEventTableOnly,
                .observable = world
            });
        }
    }

    if (ecs_should_log_2()) {
        char *expr = ecs_type_str(world, &table->type);
        ecs_dbg_2(
            "#[green]table#[normal] [%s] #[red]deleted#[reset] with id %d", 
            expr, table->id);
        ecs_os_free(expr);
        ecs_log_push_2();
    }

    world->info.empty_table_count -= (ecs_table_count(table) == 0);

    /* Cleanup data, no OnRemove, delete from entity index, don't deactivate */
    flecs_table_fini_data(world, table, false, true, true, false);
    flecs_table_clear_edges(world, table);

    if (!is_root) {
        ecs_type_t ids = {
            .array = table->type.array,
            .count = table->type.count
        };

        flecs_hashmap_remove_w_hash(
            &world->store.table_map, &ids, ecs_table_t*, table->_->hash);
    }

    flecs_wfree_n(world, int32_t, table->data->column_count + 1, flecs_table_data(table)->dirty_state);
    flecs_wfree_n(world, int32_t, table->data->column_count + table->type.count, 
        table->column_map);
    flecs_table_records_unregister(world, table);

    /* Update counters */
    world->info.table_count --;
    world->info.table_record_count -= table->_->record_count;
    world->info.table_storage_count -= table->data->column_count;
    world->info.table_delete_total ++;

    if (!table->data->column_count) {
        world->info.tag_table_count --;
    } else {
        world->info.trivial_table_count -= !(table->flags & EcsTableIsComplex);
    }

    flecs_free_t(a, ecs_table__t, table->_);
    flecs_wfree_n(world, ecs_id_t, table->type.count, table->type.array);

    if (!(world->flags & EcsWorldFini)) {
        ecs_assert(!is_root, ECS_INTERNAL_ERROR, NULL);
        flecs_sparse_remove_t(&world->store.tables, ecs_table_t, table->id);
    }

    ecs_log_pop_2();
}

/* Reset a table to its initial state. */
void flecs_table_reset(
    ecs_world_t *world,
    ecs_table_t *table)
{
    ecs_assert(!table->_->lock, ECS_LOCKED_STORAGE, NULL);
    flecs_table_clear_edges(world, table);
}

/* Keep track of number of traversable entities in table. A traversable entity
 * is an entity used as target in a pair with a traversable relationship. The
 * traversable count and flag are used by code to early out of mechanisms like
 * event propagation and recursive cleanup. */
void flecs_table_traversable_add(
    ecs_table_t *table,
    int32_t value)
{
    int32_t result = table->_->traversable_count += value;
    ecs_assert(result >= 0, ECS_INTERNAL_ERROR, NULL);
    if (result == 0) {
        table->flags &= ~EcsTableHasTraversable;
    } else if (result == value) {
        table->flags |= EcsTableHasTraversable;
    }
}

/* Mark table component dirty */
void flecs_table_mark_dirty(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_entity_t component)
{
    ecs_assert(!table->_->lock, ECS_LOCKED_STORAGE, NULL);
    ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);

    if (flecs_table_data(table)->dirty_state) {
        ecs_id_record_t *idr = flecs_id_record_get(world, component);
        if (!idr) {
            return;
        }

        const ecs_table_record_t *tr = flecs_id_record_get_table(idr, table);
        if (!tr || tr->column == -1) {
            return;
        }

        flecs_table_data(table)->dirty_state[tr->column + 1] ++;
    }
}

/* Get (or create) dirty state of table. Used by queries for change tracking */
int32_t* flecs_table_get_dirty_state(
    ecs_world_t *world,
    ecs_table_t *table)
{
    ecs_poly_assert(world, ecs_world_t);
    ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);
    if (!flecs_table_data(table)->dirty_state) {
        int32_t column_count = table->data->column_count;
        flecs_table_data(table)->dirty_state = flecs_alloc_n(&world->allocator,
             int32_t, column_count + 1);
        ecs_assert(flecs_table_data(table)->dirty_state != NULL, ECS_INTERNAL_ERROR, NULL);
        for (int i = 0; i < column_count + 1; i ++) {
            flecs_table_data(table)->dirty_state[i] = 1;
        }
    }
    return flecs_table_data(table)->dirty_state;
}

/* Grow all data structures in a table */
int32_t flecs_table_appendn(
    ecs_world_t *world,
    ecs_table_t *table,
    int32_t to_add,
    const ecs_entity_t *ids)
{
    ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(!table->_->lock, ECS_LOCKED_STORAGE, NULL);

    flecs_table_check_sanity(table);

    int32_t cur_count = ecs_table_count(table);

    flecs_table_data_appendn(world, table, to_add, ids);
    if (!(world->flags & EcsWorldReadonly) && !cur_count) {
        flecs_table_set_empty(world, table);
    }

    flecs_table_check_sanity(table);

    /* Return index of first added entity */
    return cur_count;
}

/* Append entity to table */
int32_t flecs_table_append(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_entity_t entity,
    ecs_record_t *record,
    bool construct,
    bool on_add)
{
    ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(!table->_->lock, ECS_LOCKED_STORAGE, NULL);
    ecs_assert(!(table->flags & EcsTableHasTarget), 
        ECS_INVALID_OPERATION, NULL);

    flecs_table_check_sanity(table);

    int32_t row = flecs_table_data_append(world, table, 
        entity, record, construct, on_add);
    if (row == 0) {
        /* If this is the first entity in this table, signal queries so that the
         * table moves from an inactive table to an active table. */
        flecs_table_set_empty(world, table);
    }

    flecs_table_check_sanity(table);

    return row;
}

/* Delete entity from table */
void flecs_table_delete(
    ecs_world_t *world,
    ecs_table_t *table,
    int32_t index,
    bool destruct)
{
    ecs_assert(world != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(!table->_->lock, ECS_LOCKED_STORAGE, NULL);
    ecs_assert(!(table->flags & EcsTableHasTarget), 
        ECS_INVALID_OPERATION, NULL);

    flecs_table_check_sanity(table);

    if (!flecs_table_data_delete(world, table, index, destruct)) {
        flecs_table_set_empty(world, table);
    }

    flecs_table_check_sanity(table);
}

/* Move entity from src to dst table */
void flecs_table_move(
    ecs_world_t *world,
    ecs_entity_t dst_entity,
    ecs_entity_t src_entity,
    ecs_table_t *dst_table,
    int32_t dst_index,
    ecs_table_t *src_table,
    int32_t src_index,
    bool construct)
{
    ecs_assert(dst_table != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(src_table != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(!dst_table->_->lock, ECS_LOCKED_STORAGE, NULL);
    ecs_assert(!src_table->_->lock, ECS_LOCKED_STORAGE, NULL);

    ecs_assert(src_index >= 0, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(dst_index >= 0, ECS_INTERNAL_ERROR, NULL);

    flecs_table_check_sanity(dst_table);
    flecs_table_check_sanity(src_table);

    flecs_table_data_move(world, dst_entity, src_entity, dst_table, dst_index,
        src_table, src_index, construct);

    flecs_table_check_sanity(dst_table);
    flecs_table_check_sanity(src_table);
}

/* Swap two rows in a table. Used for table sorting. */
void flecs_table_swap(
    ecs_world_t *world,
    ecs_table_t *table,
    int32_t row_1,
    int32_t row_2)
{    
    (void)world;

    ecs_assert(!table->_->lock, ECS_LOCKED_STORAGE, NULL);
    ecs_assert(row_1 >= 0, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(row_2 >= 0, ECS_INTERNAL_ERROR, NULL);

    flecs_table_check_sanity(table);

    flecs_table_data_swap(world, table, row_1, row_2);

    flecs_table_check_sanity(table);
}

/* Merge source table into destination table. This typically happens as result
 * of a bulk operation, like when a component is removed from all entities in 
 * the source table (like for the Remove OnDelete policy). */
void flecs_table_merge(
    ecs_world_t *world,
    ecs_table_t *dst_table,
    ecs_table_t *src_table)
{
    ecs_assert(src_table != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(!src_table->_->lock, ECS_LOCKED_STORAGE, NULL);

    flecs_table_check_sanity(src_table);
    flecs_table_check_sanity(dst_table);

    ecs_table_data_t *dst_data = dst_table->data;
    ecs_table_data_t *src_data = src_table->data;
    ecs_assert(dst_data != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(src_data != NULL, ECS_INTERNAL_ERROR, NULL);
    
    /* If there is nothing to merge to, just clear the old table */
    if (!dst_table) {
        flecs_table_clear_data(world, src_table);
        flecs_table_check_sanity(src_table);
        return;
    } else {
        ecs_assert(!dst_table->_->lock, ECS_LOCKED_STORAGE, NULL);
    }

    int32_t src_count = src_data->entities.count;
    int32_t dst_count = dst_data->entities.count;

    flecs_table_data_merge(world, dst_table, src_table);

    if (src_count) {
        if (!dst_count) {
            flecs_table_set_empty(world, dst_table);
        }
        flecs_table_set_empty(world, src_table);

        flecs_table_traversable_add(dst_table, src_table->_->traversable_count);
        flecs_table_traversable_add(src_table, -src_table->_->traversable_count);
        ecs_assert(src_table->_->traversable_count == 0, ECS_INTERNAL_ERROR, NULL);
    }

    flecs_table_check_sanity(src_table);
    flecs_table_check_sanity(dst_table);
}

/* Shrink table storage to fit number of entities */
bool flecs_table_shrink(
    ecs_world_t *world,
    ecs_table_t *table)
{
    ecs_assert(table != NULL, ECS_LOCKED_STORAGE, NULL);
    ecs_assert(!table->_->lock, ECS_LOCKED_STORAGE, NULL);
    (void)world;

    flecs_table_check_sanity(table);

    bool has_payload = flecs_table_data_shrink(world, table);

    flecs_table_check_sanity(table);

    return has_payload;
}

/* Internal mechanism for propagating information to tables */
void flecs_table_notify(
    ecs_world_t *world,
    ecs_table_t *table,
    ecs_table_event_t *event)
{
    if (world->flags & EcsWorldFini) {
        return;
    }

    switch(event->kind) {
    case EcsTableTriggersForId:
        flecs_table_add_trigger_flags(world, table, event->event);
        break;
    case EcsTableNoTriggersForId:
        break;
    }
}

/* -- Public API -- */

void ecs_table_lock(
    ecs_world_t *world,
    ecs_table_t *table)
{
    if (table) {
        if (ecs_poly_is(world, ecs_world_t) && !(world->flags & EcsWorldReadonly)) {
            table->_->lock ++;
        }
    }
}

void ecs_table_unlock(
    ecs_world_t *world,
    ecs_table_t *table)
{
    if (table) {
        if (ecs_poly_is(world, ecs_world_t) && !(world->flags & EcsWorldReadonly)) {
            table->_->lock --;
            ecs_assert(table->_->lock >= 0, ECS_INVALID_OPERATION, NULL);
        }
    }
}

const ecs_type_t* ecs_table_get_type(
    const ecs_table_t *table)
{
    if (table) {
        return &table->type;
    } else {
        return NULL;
    }
}

int32_t ecs_table_get_type_index(
    const ecs_world_t *world,
    const ecs_table_t *table,
    ecs_id_t id)
{
    ecs_poly_assert(world, ecs_world_t);
    ecs_check(table != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(ecs_id_is_valid(world, id), ECS_INVALID_PARAMETER, NULL);

    ecs_id_record_t *idr = flecs_id_record_get(world, id);
    if (!idr) {
        return -1;
    }

    ecs_table_record_t *tr = flecs_id_record_get_table(idr, table);
    if (!tr) {
        return -1;
    }

    return tr->index;
error:
    return -1;
}

int32_t ecs_table_get_column_index(
    const ecs_world_t *world,
    const ecs_table_t *table,
    ecs_id_t id)
{
    ecs_poly_assert(world, ecs_world_t);
    ecs_check(table != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(ecs_id_is_valid(world, id), ECS_INVALID_PARAMETER, NULL);

    ecs_id_record_t *idr = flecs_id_record_get(world, id);
    if (!idr) {
        return -1;
    }

    ecs_table_record_t *tr = flecs_id_record_get_table(idr, table);
    if (!tr) {
        return -1;
    }

    return tr->column;
error:
    return -1;
}

int32_t ecs_table_column_count(
    const ecs_table_t *table)
{
    return table->data->column_count;
}

int32_t ecs_table_type_to_column_index(
    const ecs_table_t *table,
    int32_t index)
{
    ecs_assert(index >= 0, ECS_INVALID_PARAMETER, NULL);
    ecs_check(index < table->type.count, ECS_INVALID_PARAMETER, NULL);
    int32_t *column_map = table->column_map;
    if (column_map) {
        return column_map[index];
    }
error:
    return -1;
}

int32_t ecs_table_column_to_type_index(
    const ecs_table_t *table,
    int32_t index)
{
    ecs_check(index < table->data->column_count, ECS_INVALID_PARAMETER, NULL);
    ecs_check(table->column_map != NULL, ECS_INVALID_PARAMETER, NULL);
    int32_t offset = table->type.count;
    return table->column_map[offset + index];
error:
    return -1;
}

void* ecs_table_get_column(
    const ecs_table_t *table,
    int32_t index,
    int32_t offset)
{
    ecs_check(table != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(index < table->data->column_count, ECS_INVALID_PARAMETER, NULL);

    ecs_column_t *column = flecs_table_column(table, index);
    void *result = column->data.array;
    if (offset) {
        result = ECS_ELEM(result, column->size, offset);
    }

    return result;
error:
    return NULL;
}

void* ecs_table_get_id(
    const ecs_world_t *world,
    const ecs_table_t *table,
    ecs_id_t id,
    int32_t offset)
{
    ecs_check(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(table != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(ecs_id_is_valid(world, id), ECS_INVALID_PARAMETER, NULL);

    world = ecs_get_world(world);

    int32_t index = ecs_table_get_column_index(world, table, id);
    if (index == -1) {
        return NULL;
    }

    return ecs_table_get_column(table, index, offset);
error:
    return NULL;
}

size_t ecs_table_get_column_size(
    const ecs_table_t *table,
    int32_t index)
{
    ecs_check(table != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(index < table->data->column_count, ECS_INVALID_PARAMETER, NULL);
    ecs_check(table->column_map != NULL, ECS_INVALID_PARAMETER, NULL);
    return flecs_ito(size_t, flecs_table_column(table, index)->size);
error:
    return 0;
}

int32_t ecs_table_count(
    const ecs_table_t *table)
{
    ecs_assert(table != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_vec_t *entities = flecs_table_entities(table);
    if (entities) {
        return ecs_vec_count(entities);
    }
    return 0;
}

bool ecs_table_has_id(
    const ecs_world_t *world,
    const ecs_table_t *table,
    ecs_id_t id)
{
    return ecs_table_get_type_index(world, table, id) != -1;
}

int32_t ecs_table_get_depth(
    const ecs_world_t *world,
    const ecs_table_t *table,
    ecs_entity_t rel)
{
    ecs_check(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(table != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(ecs_id_is_valid(world, rel), ECS_INVALID_PARAMETER, NULL);
    ecs_check(ecs_has_id(world, rel, EcsAcyclic), ECS_INVALID_PARAMETER, NULL);

    world = ecs_get_world(world);

    return flecs_relation_depth(world, rel, table);
error:
    return -1;
}

bool ecs_table_has_flags(
    ecs_table_t *table,
    ecs_flags32_t flags)
{
    return (table->flags & flags) == flags;
}

void ecs_table_swap_rows(
    ecs_world_t* world,
    ecs_table_t* table,
    int32_t row_1,
    int32_t row_2)
{
    flecs_table_swap(world, table, row_1, row_2);
}

int32_t flecs_table_observed_count(
    const ecs_table_t *table)
{
    return table->_->traversable_count;
}

void* ecs_record_get_column(
    const ecs_record_t *r,
    int32_t index,
    size_t c_size)
{
    (void)c_size;
    ecs_table_t *table = r->table;

    ecs_check(index < table->data->column_count, ECS_INVALID_PARAMETER, NULL);
    ecs_column_t *column = flecs_table_column(table, index);
    ecs_size_t size = column->size;

    ecs_check(!flecs_utosize(c_size) || flecs_utosize(c_size) == size, 
        ECS_INVALID_PARAMETER, NULL);

    return ecs_vec_get(&column->data, size, ECS_RECORD_TO_ROW(r->row));
error:
    return NULL;
}

ecs_record_t* ecs_record_find(
    const ecs_world_t *world,
    ecs_entity_t entity)
{
    ecs_check(world != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(entity != 0, ECS_INVALID_PARAMETER, NULL);

    world = ecs_get_world(world);

    ecs_record_t *r = flecs_entities_get(world, entity);
    if (r) {
        return r;
    }
error:
    return NULL;
}
