/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/data_listeners.hh"
#include "replica/database.hh"
#include "readers/filtering.hh"
#include "db_clock.hh"

#include <tuple>

extern logging::logger dblog;

namespace db {

void data_listeners::install(data_listener* listener) {
    _listeners.emplace(listener);
    dblog.debug("data_listeners: install listener {}", fmt::ptr(listener));
}

void data_listeners::uninstall(data_listener* listener) {
    dblog.debug("data_listeners: uninstall listener {}", fmt::ptr(listener));
    _listeners.erase(listener);
}

bool data_listeners::exists(data_listener* listener) const {
    return _listeners.contains(listener);
}

flat_mutation_reader_v2 data_listeners::on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, flat_mutation_reader_v2&& rd) {
    for (auto&& li : _listeners) {
        rd = li->on_read(s, range, slice, std::move(rd));
    }
    return std::move(rd);
}

void data_listeners::on_write(const schema_ptr& s, const frozen_mutation& m) {
    for (auto&& li : _listeners) {
        li->on_write(s, m);
    }
}

toppartitions_item_key::operator sstring() const {
    std::ostringstream oss;
    oss << key.key().with_schema(*schema);
    return oss.str();
}

toppartitions_data_listener::toppartitions_data_listener(replica::database& db, std::unordered_set<std::tuple<sstring, sstring>, utils::tuple_hash> table_filters,
        std::unordered_set<sstring> keyspace_filters) : _db(db), _table_filters(std::move(table_filters)), _keyspace_filters(std::move(keyspace_filters)) {
    dblog.debug("toppartitions_data_listener: installing {}", fmt::ptr(this));
    _db.data_listeners().install(this);
}

toppartitions_data_listener::~toppartitions_data_listener() {
    dblog.debug("toppartitions_data_listener: uninstalling {}", fmt::ptr(this));
    _db.data_listeners().uninstall(this);
}

future<> toppartitions_data_listener::stop() {
    dblog.debug("toppartitions_data_listener: stopping {}", fmt::ptr(this));
    return make_ready_future<>();
}

void toppartitions_data_listener::reset() {
    dblog.debug("toppartitions_data_listener: resetting {}", fmt::ptr(this));
    _top_k_read = top_k();
    _top_k_write = top_k();
}

flat_mutation_reader_v2 toppartitions_data_listener::on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, flat_mutation_reader_v2&& rd) {
    bool include_all = _table_filters.empty() && _keyspace_filters.empty();

    if (include_all || _keyspace_filters.contains(s->ks_name()) || _table_filters.contains({s->ks_name(), s->cf_name()})) {
        dblog.trace("toppartitions_data_listener::on_read: {}.{}", s->ks_name(), s->cf_name());
        return make_filtering_reader(std::move(rd), [zis = this->weak_from_this(), s = std::move(s)] (const dht::decorated_key& dk) {
            // The data query may be executing after the toppartitions_data_listener object has been removed, so check
            if (zis) {
                zis->_top_k_read.append(toppartitions_item_key{s, dk, this_shard_id()});
            }
            return true;
        });
    }

    return std::move(rd);
}

void toppartitions_data_listener::on_write(const schema_ptr& s, const frozen_mutation& m) {
    bool include_all = _table_filters.empty() && _keyspace_filters.empty();
    
    if (include_all || _keyspace_filters.contains(s->ks_name()) || _table_filters.contains({s->ks_name(), s->cf_name()})) {
        dblog.trace("toppartitions_data_listener::on_write: {}.{}", s->ks_name(), s->cf_name());
        _top_k_write.append(toppartitions_item_key{s, m.decorated_key(*s), this_shard_id()});
    }
}

toppartitions_data_listener::global_top_k::results
toppartitions_data_listener::globalize(top_k::results&& r) {
    toppartitions_data_listener::global_top_k::results n;
    n.values.reserve(r.values.size());
    n.cardinality = r.cardinality;
    for (auto&& e : r.values) {
        n.values.emplace_back(global_top_k::result(toppartitions_global_item_key(std::move(e.item)), e.count, e.error));
    }
    return n;
}

toppartitions_data_listener::top_k::results
toppartitions_data_listener::localize(const global_top_k::results& r) {
    toppartitions_data_listener::top_k::results n;
    n.values.reserve(r.values.size());
    n.cardinality = r.cardinality;
    for (auto&& e : r.values) {
        n.values.emplace_back(top_k::result(toppartitions_item_key(e.item), e.count, e.error));
    }
    return n;
}

toppartitions_query::toppartitions_query(distributed<replica::database>& xdb, std::unordered_set<std::tuple<sstring, sstring>, utils::tuple_hash>&& table_filters,
        std::unordered_set<sstring>&& keyspace_filters, std::chrono::milliseconds duration, size_t list_size, size_t capacity)
        : _xdb(xdb), _table_filters(std::move(table_filters)), _keyspace_filters(std::move(keyspace_filters)), _duration(duration), _list_size(list_size), _capacity(capacity),
          _query(std::make_unique<sharded<toppartitions_data_listener>>()) {
    dblog.debug("toppartitions_query on {} column families and {} keyspaces", !_table_filters.empty() ? std::to_string(_table_filters.size()) : "all",
                !_keyspace_filters.empty() ? std::to_string(_keyspace_filters.size()) : "all");
}

future<> toppartitions_query::scatter() {
    return _query->start(std::ref(_xdb), _table_filters, _keyspace_filters);
}

using top_t = toppartitions_data_listener::global_top_k::results;

future<toppartitions_query::results> toppartitions_query::gather(sharded<replica::database>& sharded_db, bool per_shard, unsigned res_size) {
    dblog.debug("toppartitions_query::gather");

    auto map = [res_size] (toppartitions_data_listener& listener) {
        dblog.trace("toppartitions_query::map_reduce with listener {}", fmt::ptr(&listener));
        top_t rd = toppartitions_data_listener::globalize(listener._top_k_read.top(res_size));
        top_t wr = toppartitions_data_listener::globalize(listener._top_k_write.top(res_size));

        return make_foreign(std::make_unique<std::tuple<top_t, top_t>>(std::move(rd), std::move(wr)));
    };
    auto map_per_shard = [map] (replica::database &db) {
        return map(db.tp_listener());
    };
    auto reduce = [] (results res, foreign_ptr<std::unique_ptr<std::tuple<top_t, top_t>>> rd_wr) {
        res.read.append(toppartitions_data_listener::localize(std::get<0>(*rd_wr)));
        res.write.append(toppartitions_data_listener::localize(std::get<1>(*rd_wr)));

        res.read_cardinality += std::get<0>(*rd_wr).cardinality;
        res.write_cardinality += std::get<1>(*rd_wr).cardinality;
        return res;
    };

    if (per_shard) {
        return sharded_db.map_reduce0(map_per_shard, results{res_size * smp::count}, reduce)
            .handle_exception([] (auto ep) {
                dblog.error("toppartitions_query::gather_per_shard: {}", ep);
                return make_exception_future<results>(ep);
            });
    } else {
        return _query->map_reduce0(map, results{res_size * smp::count}, reduce)
            .handle_exception([] (auto ep) {
                dblog.error("toppartitions_query::gather: {}", ep);
                return make_exception_future<results>(ep);
            }).finally([this] () {
                dblog.debug("toppartitions_query::gather: stopping query");
                return _query->stop().then([] {
                    dblog.debug("toppartitions_query::gather: query stopped");
                });
            });
    }
}

} // namespace db
