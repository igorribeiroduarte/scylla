/*
 * Copyright (C) 2011 Clearspring Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

/*
 Based on the following implementation ([2]) for the Space-Saving algorithm from [1].

 [1] Metwally, A., Agrawal, D., & El Abbadi, A. (2005, January).
     Efficient computation of frequent and top-k elements in data streams.
     In International Conference on Database Theory (pp. 398-412). Springer, Berlin, Heidelberg.
     http://www.cse.ust.hk/~raywong/comp5331/References/EfficientComputationOfFrequentAndTop-kElementsInDataStreams.pdf

 [2] https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/StreamSummary.java

 The algorithm keeps a map between keys seen and their counts, keeping a bound on the number of tracked keys.
 Replacement policy evicts the key with the lowest count while inheriting its count, and recording an estimation
 of the error which results from that.
 This error estimation can be later used to prove if the distribution we arrived at corresponds to the real top-K,
 which we can display alongside the results.
 Accuracy depends on the number of tracked keys.

*/

#include <cstdio>
#include <list>
#include <vector>
#include <unordered_map>
#include <boost/intrusive/unordered_set.hpp>
#include <memory>
#include <tuple>
#include <assert.h>

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include "utils/chunked_vector.hh"

namespace bi = boost::intrusive;

namespace utils {

using namespace seastar;

template <class T, class Hash = std::hash<T>, class KeyEqual = std::equal_to<T>>
class space_saving_top_k {
private:
    class counters_map_entry;
    struct bucket;
    using buckets_iterator = typename std::list<bucket>::iterator;

    struct counter {
        buckets_iterator bucket_it;
        counters_map_entry cmap_entry;
        unsigned count = 0;
        unsigned error = 0;

        counter(T item, unsigned count = 0, unsigned error = 0) : cmap_entry(item), count(count), error(error) {}
    };

    using counter_ptr = lw_shared_ptr<counter>;

    using counters = std::list<counter_ptr>;
    using counters_iterator = typename counters::iterator;

private:
    struct counters_map_entry : public bi::unordered_set_base_hook<bi::store_hash<true>> {
        using key_type = T;
        using value_type = counters_iterator;

        key_type _key;
        std::optional<value_type> _val;

        // FIXME: Remove this method if I decide to keep everything public in this class
        const key_type& key() const noexcept {
            return _key;
        }

        // FIXME: Remove this method if I decide to keep everything public in this class
        const value_type& value() const noexcept {
            return *_val;
        }

        // FIXME: Remove this method if I decide to keep everything public in this class
        void set_value(value_type new_val) {
            _val.emplace(std::move(new_val));
        }

        friend bool operator==(const counters_map_entry& a, const counters_map_entry& b){
            return KeyEqual()(a.key(), b.key());
        }

        friend std::size_t hash_value(const counters_map_entry& v) {
            return Hash()(v.key());
        }

        counters_map_entry(key_type k) : _key(std::move(k)) {}
    };

    // FIXME: Double check if the number of buckets will really always be a power of 2 for this use case
    using counters_map = bi::unordered_set<counters_map_entry, bi::power_2_buckets<true>, bi::compare_hash<true>>;
    using counters_map_iterator = typename counters_map::iterator;
    using counters_map_bucket_traits = typename counters_map::bucket_traits;

    struct bucket {
        std::list<counter_ptr> counters;
        unsigned count;

        // FIXME: Use a better name for this method
        void reset(counter_ptr new_ctr) {
            count = new_ctr->count;
            counters.push_back(new_ctr);
        }

        bucket(counter_ptr ctr) {
            count = ctr->count;
            counters.push_back(ctr);
        }

        bucket(T item, unsigned count, unsigned error) {
            counters.push_back(make_lw_shared<counter>(item, count, error));
            this->count = count;
        }
    };

    using buckets = std::list<bucket>;

    size_t _capacity;
    std::vector<typename counters_map::bucket_type> _counters_map_buckets;
    counters_map _counters_map;

    buckets _buckets; // buckets list in ascending order

    bool _valid = true;

public:
    /// capacity: maximum number of elements to be tracked
    space_saving_top_k(size_t capacity = 256)
        : _capacity(capacity)
        , _counters_map_buckets(capacity)
        , _counters_map(counters_map_bucket_traits(_counters_map_buckets.data(), _counters_map_buckets.size())) {}

    // FIXME: Is it really ok to use noexcept here?
    space_saving_top_k(space_saving_top_k &&t) noexcept
        : _capacity(t._capacity)
        , _counters_map_buckets(t._capacity)
        , _counters_map(counters_map_bucket_traits(_counters_map_buckets.data(), _counters_map_buckets.size())) {
        append(t.top(t._capacity));
    }

    // FIXME: Do this in the correct way
    space_saving_top_k& operator=(space_saving_top_k t) noexcept {
        _capacity = t._capacity;
        _counters_map_buckets = std::vector<typename counters_map::bucket_type>(t._capacity);
        _counters_map = counters_map(counters_map_bucket_traits(_counters_map_buckets.data(), _counters_map_buckets.size()));
        append(t.top(t._capacity));
        return *this;
    }

    ~space_saving_top_k() {
        // Counters_map needs to be cleaned before the other attributes to avoid the deletion
        // of elements while they're still linked
        _counters_map.clear();
    }

    size_t capacity() const { return _capacity; }

    size_t size() const {
        if (!_valid) {
            throw std::runtime_error("space_saving_top_k state is invalid");
        }
        return _counters_map.size();
    }

    bool valid() const { return _valid; }

    // returns true if item is a new one
    bool append(T item, unsigned inc = 1, unsigned err = 0) {
        return std::get<0>(append_return_all(std::move(item), inc, err));
    }

    // returns optionally dropped item (due to capacity overflow)
    std::optional<T> append_return_dropped(T item, unsigned inc = 1, unsigned err = 0) {
        return std::get<1>(append_return_all(std::move(item), inc, err));
    }

    // returns whether an element is new and an optionally dropped item (due to capacity overflow)
    std::tuple<bool, std::optional<T>> append_return_all(T item, unsigned inc = 1, unsigned err = 0) {
        if (!_valid) {
            return {false, std::optional<T>()};
        }
        try {
            counters_map_iterator cmap_it = _counters_map.find(item);
            bool is_new_item = cmap_it == _counters_map.end();
            std::optional<T> dropped_item;
            counters_iterator counter_it;

            if (is_new_item) {
                if (size() < _capacity) {
                    _buckets.emplace_front(bucket(std::move(item), 0, err)); // inc added later via increment_counter
                    buckets_iterator new_bucket_it = _buckets.begin();
                    counter_it = new_bucket_it->counters.begin();
                    (*counter_it)->bucket_it = new_bucket_it;
                } else {
                    buckets_iterator min_bucket = _buckets.begin();
                    assert(min_bucket != _buckets.end());
                    counter_it = min_bucket->counters.begin();
                    assert(counter_it != min_bucket->counters.end());
                    counter_ptr ctr = *counter_it;

                    _counters_map.erase(ctr->cmap_entry._key);

                    dropped_item = std::exchange(ctr->cmap_entry._key, std::move(item));
                    ctr->error = min_bucket->count + err;
                }

                _counters_map.insert((*counter_it)->cmap_entry);
            } else {
                counter_it = cmap_it->value();
            }

            increment_counter(counter_it, inc);

            return {is_new_item, std::move(dropped_item)};
        } catch (...) {
            _valid = false;
            std::rethrow_exception(std::current_exception());
        }
    }

private:
    void increment_counter(counters_iterator counter_it, unsigned inc) {
        counter_ptr ctr = *counter_it;

        buckets_iterator old_bucket_it = ctr->bucket_it;
        auto& old_buck = *old_bucket_it;
        old_buck.counters.erase(counter_it);

        ctr->count += inc;

        buckets_iterator bi_prev = old_bucket_it;
        buckets_iterator bi_next = std::next(old_bucket_it);
        while (bi_next != _buckets.end()) {
            bucket& buck = *bi_next;
            if (ctr->count == buck.count) {
                buck.counters.push_back(ctr);
                counter_it = std::prev(buck.counters.end());
                break;
            } else if (ctr->count > buck.count) {
                bi_prev = bi_next;
                bi_next = std::next(bi_prev);
            } else {
                bi_next = _buckets.end(); // create new bucket
            }
        }

        if (bi_next == _buckets.end()) {
            if (old_buck.counters.empty()) {
                // If it got here, it means that n_buckets < n_counters
                old_buck.reset(ctr);
                counter_it = old_buck.counters.begin();
                bi_next = _buckets.insert(std::next(bi_prev), std::move(old_buck));
            } else {
                // This allocation will only happen while n_buckets < capacity
                bucket buck{ctr};

                counter_it = buck.counters.begin();
                bi_next = _buckets.insert(std::next(bi_prev), std::move(buck));
            }
        }

        ctr->bucket_it = bi_next;

        counters_map_iterator cmap_it = _counters_map.find(ctr->cmap_entry._key);
        cmap_it->set_value(std::move(counter_it));


        if (old_buck.counters.empty()) {
            _buckets.erase(old_bucket_it);
        }
    }

    //-----------------------------------------------------------------------------------------
    // Results
public:
    struct result {
        T item;
        unsigned count;
        unsigned error;

        result(T item, unsigned count, unsigned error) : item(item), count(count), error(error) {}
    };

    struct results {
        chunked_vector<result> values;
        unsigned cardinality;
    };

    results top(unsigned k) const
    {
        if (!_valid) {
            throw std::runtime_error("space_saving_top_k state is invalid");
        }

        results list;
        list.cardinality = size();
        // _buckets are in ascending order
        for (auto b_it = _buckets.rbegin(); b_it != _buckets.rend(); ++b_it) {
            auto& b = *b_it;
            for (auto& c: b.counters) {
                if (list.values.size() == k) {
                    return list;
                }
                list.values.emplace_back(result{c->cmap_entry._key, c->count, c->error});
            }
        }
        return list;
    }

    void append(const results& res) {
        for (auto& r: res.values) {
            append(r.item, r.count, r.error);
        }
    }

    //-----------------------------------------------------------------------------------------
    // Diagnostics
public:
    template <class TT>
    friend std::ostream& operator<<(std::ostream& out, const typename space_saving_top_k<TT>::counter& c);
    template <class TT>
    friend std::ostream& operator<<(std::ostream& out, const typename space_saving_top_k<TT>::counters_map& counters_map);
    template <class TT>
    friend std::ostream& operator<<(std::ostream& out, const typename space_saving_top_k<TT>::buckets& buckets);
    template <class TT>
    friend std::ostream& operator<<(std::ostream& out, const space_saving_top_k<TT>& top_k);
};

//---------------------------------------------------------------------------------------------

template <class T>
std::ostream& operator<<(std::ostream& out, const typename space_saving_top_k<T>::counter& c) {
    out << c.item << " " << c.count << "/" << c.error << " " << &*c.bucket_it;
    return out;
}

template <class T>
std::ostream& operator<<(std::ostream& out, const typename space_saving_top_k<T>::counters_map& counters_map) {
    out << "{\n";
    for (auto const& [item, counter_i]: counters_map) {
        out << item << " => " << **counter_i << "\n";
    }
    out << "}\n";
    return out;
}

template <class T>
std::ostream& operator<<(std::ostream& out, const typename space_saving_top_k<T>::buckets& buckets) {
    for (auto& b: buckets) {
        out << &b << " " << b.count << " [";
        for (auto& c: b.counters) {
            out << *c << " ";
        }
        out << "]\n";
    }
    return out;
}

template <class T>
std::ostream& operator<<(std::ostream& out, const space_saving_top_k<T>& top_k) {
    out << top_k._buckets;
    out << top_k._counters_map;
    out << "---\n";
    return out;
}

} // namespace utils
