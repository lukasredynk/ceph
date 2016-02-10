// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013- Sage Weil <sage@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_OSD_PMSTORE_H
#define CEPH_OSD_PMSTORE_H

#include <vector>
#include <mutex>
#include <boost/intrusive_ptr.hpp>
#include <boost/intrusive/list.hpp>
#include "pmbackend/include/pmbackend.h"
#include "include/assert.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/RefCountedObj.h"
#include "common/Finisher.h"
#include "common/RWLock.h"
#include "os/ObjectStore.h"
#include "kv/KeyValueDB.h"

//#ifdef WITH_LTTNG
//#include "tracing/pmstore.h"
//#else
//#define tracepoint(...)
//#endif

class PMStore : public ObjectStore {
  CephContext *const cct;

public:
  class TXctx : RefCountedObject {
      uint64_t tx_id;
  public:
      typedef boost::intrusive_ptr<TXctx> Ref;
      friend void intrusive_ptr_add_ref(TXctx *t) { t->get(); }
      friend void intrusive_ptr_release(TXctx *t) { t->put(); }

      pmb_handle* store;
      KeyValueDB *db;
      KeyValueDB::Transaction t;

      TXctx(pmb_handle* store_, KeyValueDB* db_) : store(store_), db(db_) { }

      void tx_begin() {
        pmb_tx_begin(store, &tx_id);
        // loop for tx slot = 0
        if (tx_id == 0) {
          int i = 5;
          while (tx_id == 0 && i > 0) {
            pmb_tx_begin(store, &tx_id);
            i--;
          }
        }

        assert(tx_id != 0);

        t = db->get_transaction();
      }
      void tx_finish() {
        db->submit_transaction(t);
        pmb_tx_commit(store, tx_id);
        pmb_tx_execute(store, tx_id);
      }

      void tx_abort() {
        pmb_tx_abort(store, tx_id);
      }

      uint64_t get_tx_id() const {
        return tx_id;
      }
  };
  typedef TXctx::Ref TXctxRef;

  struct Object : public RefCountedObject {
    uint64_t id;                // handle for omaps
    uint64_t data_len;          // "size" of object: boundry of l
    size_t used_blocks;         // number of datablocks used by object
    std::vector<uint64_t> data; // vector with ids of data blocks

    set<string> xattr_keys;
    uint64_t xattrs;

    typedef boost::intrusive_ptr<Object> Ref;
    friend void intrusive_ptr_add_ref(Object *o) { o->get(); }
    friend void intrusive_ptr_release(Object *o) { o->put(); }

    Object() : id(0), data_len(0), used_blocks(0), xattrs(0) {}

    int write(TXctxRef txc, const coll_t& cid, const ghobject_t& oid,
        uint32_t data_blocksize, void *buf, size_t offset, size_t len);

    int touch(TXctxRef txc, const coll_t& cid, const ghobject_t& oid);

    int truncate(TXctxRef txc, const coll_t& cid, const ghobject_t& oid,
        uint64_t data_blocksize, size_t len, int* diff);

    // helpers for writing blocks with object's metadata
    int write_xattr(TXctxRef txc, const coll_t& cid, const ghobject_t& oid,
        map<string, bufferptr>& attrs);

    int write_xattr(TXctxRef txc, const coll_t& cid, const ghobject_t& oid,
        void* buf, const size_t len);

    int remove_xattr(TXctxRef txc, const char* xattr_key);

    int change_key(TXctxRef txc, const coll_t& cid, const ghobject_t& oid);

    void add(uint32_t part, const uint64_t blk_id) {
      if (data.empty()) {
        data_len = 0;
      }

      size_t size = data.size();
      while (size < part + 1) {
        data.push_back(0UL);
        ++size;
      }

      data[part] = blk_id;
      used_blocks++;
    }

    void dump(Formatter *f) const {
      f->dump_int("data_len", data_len);
      f->open_array_section("data_ids");
      for (auto i = data.cbegin(); i != data.cend(); ++i) {
        f->dump_int("id", *i);
      }
      f->close_section();
      if (xattrs) {
        f->dump_int("xattrs", xattrs);
        f->open_array_section("xattr_map");
        for (set<string>::const_iterator p = xattr_keys.begin();
             p != xattr_keys.end();
             ++p) {
          f->open_object_section("xattr");
          f->dump_string("key", *p);
          f->close_section();
        }
        f->close_section();
      }
    }

    void encode_key(bufferlist &bl, const coll_t &cid, const ghobject_t &oid,
        const size_t part) {
      ENCODE_START(1, 1, bl);
      ::encode(cid, bl);
      ::encode(oid, bl);
      ::encode(part, bl);
      ENCODE_FINISH(bl);
    }

    void encode_key_meta(bufferlist &bl, const coll_t &cid, const ghobject_t &oid,
        const char type) {
      ENCODE_START(1, 1, bl);
      ::encode(cid, bl);
      ::encode(oid, bl);
      ::encode(type, bl);
      ENCODE_FINISH(bl);
    }
  };
  typedef Object::Ref ObjectRef;

  struct Collection : public RefCountedObject {
    ceph::unordered_map<ghobject_t, ObjectRef> object_hash;  ///< for lookup
    map<ghobject_t, ObjectRef, ghobject_t::BitwiseComparator> object_map;        ///< for iteration
    RWLock lock;   ///< for object_{map,hash}
    uint64_t id;   // only for collections without objects, removed after first
                   // object is written to the collection

    typedef boost::intrusive_ptr<Collection> Ref;
    friend void intrusive_ptr_add_ref(Collection *c) { c->get(); }
    friend void intrusive_ptr_release(Collection *c) { c->put(); }
    // NOTE: The lock only needs to protect the object_map/hash, not the
    // contents of individual objects.  The osd is already sequencing
    // reads and writes, so we will never see them concurrently at this
    // level.

    ObjectRef get_object(ghobject_t oid) {
      RWLock::RLocker l(lock);
      ceph::unordered_map<ghobject_t,ObjectRef>::iterator o = object_hash.find(oid);
      if (o == object_hash.end())
        return ObjectRef();
      return o->second;
    }

    ObjectRef get_or_create_object(ghobject_t oid) {
      RWLock::WLocker l(lock);
      auto result = object_hash.emplace(oid, ObjectRef());
      if (result.second)
        object_map[oid] = result.first->second = new Object;
      return result.first->second;
    }

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      uint32_t s = object_map.size();
      ::encode(s, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& p) {
      DECODE_START(1, p);
      uint32_t s;
      ::decode(s, p);
      DECODE_FINISH(p);
    }

    uint64_t used_bytes() const {
      uint64_t result = 0;
      for (map<ghobject_t, ObjectRef, ghobject_t::BitwiseComparator>::const_iterator p = object_map.begin();
	   p != object_map.end();
	   ++p) {
        result += (p->second->data_len);
      }

      return result;
    }

    Collection() : lock("PMStore::Collection::lock"), id(0) { }
  };
  typedef Collection::Ref CollectionRef;

private:
  class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
      CollectionRef c;
      ObjectRef o;
      KeyValueDB::Iterator it;
      string head, tail;
  public:
      OmapIteratorImpl(CollectionRef c, ObjectRef o, KeyValueDB::Iterator it);
      int seek_to_first();
      int upper_bound(const string &after);
      int lower_bound(const string &to);
      bool valid();
      int next(bool validate=true);
      string key();
      bufferlist value();
      int status() {
        return 0;
      }
  };

  pmb_handle *store;
  KeyValueDB *db;
  ceph::unordered_map<coll_t, CollectionRef> coll_map;
  RWLock coll_lock;    ///< rwlock to protect coll_map
  Mutex apply_lock;    ///< serialize all updates

  CollectionRef get_collection(const coll_t& cid);

  Finisher finisher;

  uint64_t used_bytes;
  uint32_t data_blocksize;
  uint32_t meta_blocksize;

  std::mutex id_lock;
  uint64_t id_last;
  uint64_t id_max;

  void _assign_id(TXctxRef txc, ObjectRef o);

  int _do_transaction(Transaction& t, TXctxRef txc);
  /// -----------------------------

  int _touch(TXctxRef txc, const coll_t& cid, const ghobject_t& oid);
  int _write(TXctxRef txc, const coll_t& cid, const ghobject_t& oid, uint64_t offset,
              size_t len, bufferlist& bl,  uint32_t fadvsie_flags = 0);
  int _zero(TXctxRef txc, const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len);
  int _truncate(TXctxRef txc, const coll_t& cid, const ghobject_t& oid, uint64_t size);
  int _remove(TXctxRef txc, const coll_t& cid, const ghobject_t& oid);
  int _setattrs(TXctxRef txc, const coll_t& cid, const ghobject_t& oid, map<string,bufferptr>& aset);
  int _rmattr(TXctxRef txc, const coll_t& cid, const ghobject_t& oid, const char *name);
  int _rmattrs(TXctxRef txc, const coll_t& cid, const ghobject_t& oid);
  int _clone(TXctxRef txc, const coll_t& cid, const ghobject_t& oldoid, const ghobject_t& newoid);
  int _clone_range(TXctxRef txc, const coll_t& cid, const ghobject_t& oldoid,
                  const ghobject_t& newoid, uint64_t srcoff, uint64_t len, uint64_t dstoff);
  void _do_omap_clear(TXctxRef txc, uint64_t id);
  int _omap_clear(TXctxRef txc, const coll_t& cid, const ghobject_t &oid);
  int _omap_setkeys(TXctxRef txc, const coll_t& cid, const ghobject_t &oid, bufferlist &aset);
  int _omap_rmkeys(TXctxRef txc, const coll_t& cid, const ghobject_t &oid, bufferlist &keys);
  int _omap_rmkeyrange(TXctxRef txc, const coll_t& cid, const ghobject_t &oid,
		       const string& first, const string& last);
  int _omap_setheader(TXctxRef txc, const coll_t& cid, const ghobject_t &oid, bufferlist &bl);

  int _collection_hint_expected_num_objs(const coll_t& cid, uint32_t pg_num,
      uint64_t num_objs) const { return 0; }
  int _create_collection(TXctxRef txc, const coll_t& c);
  int _destroy_collection(TXctxRef txc, const coll_t& c);
  int _collection_add(TXctxRef txc, const coll_t& cid, const coll_t& ocid, const ghobject_t& oid);
  int _collection_move_rename(TXctxRef txc, const coll_t& oldcid, const ghobject_t& oldoid,
			      const coll_t& cid, const ghobject_t& o);
  int _split_collection(TXctxRef txc, const coll_t& cid, uint32_t bits, uint32_t rem, coll_t dest);

  int _save();
  int _load();

  void _add_object(pmb_pair *pair);
  void _add_meta_object(pmb_pair *pair);

  void dump(Formatter *f);
  void dump_all();

public:
  PMStore(CephContext *cct, const string& path)
    : ObjectStore(path),
      cct(cct),
      coll_lock("PMStore::coll_lock"),
      apply_lock("PMStore::apply_lock"),
      finisher(cct),
      used_bytes(0),
      data_blocksize(g_conf->pmstore_max_val_len),
      meta_blocksize(g_conf->pmstore_meta_max_val_len),
      id_max(0),
      sharded(false) { }
  ~PMStore() { }

  virtual bool needs_journal() { return false; };
  virtual bool wants_journal() { return false; };
  virtual bool allows_journal() { return false; };
  virtual string get_type() { return "pmstore"; }
  int peek_journal_fsid(uuid_d *fsid);

  bool test_mount_in_use() { return false; }

  int mount();
  int umount();

  unsigned get_max_object_name_length() { return 4096; }
  // arbitrary; there is no real limit internally
  unsigned get_max_attr_name_length() { return 256; }

  int mkfs();
  int mkjournal() { return 0; }

  bool sharded;
  void set_allow_sharded_objects() { sharded = true; }
  bool get_allow_sharded_objects() { return sharded; }

  int statfs(struct statfs *buf);

  bool exists(const coll_t& cid, const ghobject_t& oid);
  int stat(
    const coll_t& cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false); // struct stat?
  int read(
    const coll_t& cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0,
    bool allow_eio = false);
  int fiemap(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);
  int getattr(const coll_t& cid, const ghobject_t& oid, const char *name, bufferptr& value);
  int getattrs(const coll_t& cid, const ghobject_t& oid, map<string,bufferptr>& aset);

  int list_collections(vector<coll_t>& ls);
  bool collection_exists(const coll_t& c);
  bool collection_empty(const coll_t& c);
  int collection_list(const coll_t& cid, ghobject_t start, ghobject_t end,
      bool sort_bitwise, int max, vector<ghobject_t> *ls, ghobject_t *next);

  int omap_get(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    );

  /// Get omap header
  int omap_get_header(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    );

  /// Get keys defined on oid
  int omap_get_keys(
    const coll_t& cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    );

  /// Get key values
  int omap_get_values(
    const coll_t& cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    );

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    );

  ObjectMap::ObjectMapIterator get_omap_iterator(
    const coll_t& cid,              ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    );

  void set_fsid(uuid_d u);
  uuid_d get_fsid();

  objectstore_perf_stat_t get_cur_stats();

  int queue_transactions(
    Sequencer *osr, vector<Transaction>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL);
};

#endif // CEPH_OSD_PMSTORE_H
