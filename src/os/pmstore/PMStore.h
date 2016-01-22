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


#ifndef CEPH_PMSTORE_H
#define CEPH_PMSTORE_H

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

//#ifdef WITH_LTTNG
//#include "tracing/pmstore.h"
//#else
//#define tracepoint(...)
//#endif

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "pmstore::opseq: "

class PMStore : public ObjectStore {
  CephContext *const cct;
public:

  // Block types in meta store
  // XATTR - object's xattr data
  // OMAP  - object's omap data
  // OMAP_HDR - object's header
  // LINK - links object from one collection to another
  // COLLECTION - only for empty collection, perserves collection entry in
  //              case when at OSD stop collection doesn't have any objects
  enum meta_type{
      XATTR,      // 'x' in key
      OMAP_HDR,   // 'h' in key
      OMAP,       // 'o' in key
      LINK,       // 'l' in key
      COLLECTION, // 'c' in key
      INVALID     // invalid meta object type
  };

  meta_type get_meta_type(char t) {
    if (t == 'o')
      return OMAP;
    if (t == 'x')
      return XATTR;
    if (t == 'h')
      return OMAP_HDR;

    return INVALID;
  }


  struct Object : public RefCountedObject {
    uint64_t data_len;          // "size" of object: boundry of l
    size_t used_blocks;         // number of datablocks used by object
    std::vector<uint64_t> data; // vector with ids of data blocks

    uint64_t omap_header;       // id of omap header

    set<string> omap_keys;
    uint64_t omaps;

    set<string> xattr_keys;
    uint64_t xattrs;

    typedef boost::intrusive_ptr<Object> Ref;
    friend void intrusive_ptr_add_ref(Object *o) { o->get(); }
    friend void intrusive_ptr_release(Object *o) { o->put(); }

    Object() : data_len(0), used_blocks(0), omap_header(0), omaps(0), xattrs(0) {}

    int write(pmb_handle* store, coll_t& cid, const ghobject_t& oid, uint64_t tx_id,
        uint32_t data_blocksize, void *buf, size_t offset, size_t len);

    int touch(pmb_handle* store, coll_t& cid, const ghobject_t& oid, uint64_t tx_id);

    int truncate(pmb_handle* store, const coll_t& cid, const ghobject_t& oid,
        uint64_t tx_id, uint64_t data_blocksize, size_t len, int* diff);

    // helpers for writing blocks with object's metadata
    int write_xattr(pmb_handle* store, const coll_t& cid, const ghobject_t& oid,
        uint64_t tx_id, map<string, bufferptr>& attrs);

    int write_xattr(pmb_handle* store, const coll_t& cid, const ghobject_t& oid,
        uint64_t tx_id, void* buf, const size_t len);

    int remove_xattr(pmb_handle* store, uint64_t tx_id, const char* xattr_key);

    int write_omap(pmb_handle* store, const coll_t& cid, const ghobject_t& oid,
        uint64_t tx_id, map<string, bufferlist>& aset);

    int write_omap(pmb_handle* store, const coll_t& cid, const ghobject_t& oid,
        uint64_t tx_id, void* buf, const size_t size);

    int remove_omaps(pmb_handle* store, uint64_t tx_id, const set<string>& omap_keys);

    int write_omap_header(pmb_handle* store, const coll_t& cid,
        const ghobject_t& oid, uint64_t tx_id, bufferlist& omap_header_bl);

    int write_omap_header(pmb_handle* store, const coll_t& cid,
        const ghobject_t& oid, uint64_t tx_id, void* buf, const size_t size);

    int change_key(pmb_handle* store, const coll_t& cid, const ghobject_t& oid,
        uint64_t tx_id);

    void add(uint32_t part, uint64_t obj_id) {
      if (data.empty()) {
        data_len = 0;
      }

      size_t size = data.size();
      while (size < part + 1) {
        data.push_back(0UL);
        ++size;
      }

      data[part] = obj_id;
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

      if (omap_header) {
        f->dump_int("omap header", omap_header);
      }

      if (omaps) {
        f->dump_int("omaps", omaps);
        f->open_array_section("omap_map");
        for (set<string>::const_iterator p = omap_keys.begin();
             p != omap_keys.end();
             ++p) {
          f->open_object_section("omap");
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
        object_map[oid] = result.first->second = new Object();
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
    map<string, bufferlist> omaps;
    map<string, bufferlist>::iterator it;
  public:
    OmapIteratorImpl(pmb_handle* store, CollectionRef c, ObjectRef o)
      : c(c), o(o) {
        pmb_pair kvp;
        assert(pmb_get(store, o->omaps, &kvp) == PMB_OK);
        bufferlist bl;
        bl.append((const char*) kvp.val, kvp.val_len);
        bufferlist::iterator p = bl.begin();
        ::decode(omaps, p);
        it = omaps.begin();
      }
    ~OmapIteratorImpl() {
      omaps.clear();
    }

    int seek_to_first() {
      RWLock::RLocker l(c->lock);
      it = omaps.begin();
      return 0;
    }
    int upper_bound(const string &after) {
      RWLock::RLocker l(c->lock);
      it = omaps.upper_bound(after);
      return 0;
    }
    int lower_bound(const string &to) {
      RWLock::RLocker l(c->lock);
      it = omaps.lower_bound(to);
      return 0;
    }
    bool valid() {
      RWLock::RLocker l(c->lock);
      return it != omaps.end();
    }
    int next(bool validate = true) {
      RWLock::RLocker l(c->lock);
      ++it;
      return 0;
    }
    string key() {
      RWLock::RLocker l(c->lock);
      return it->first;
    }
    bufferlist value() {
      RWLock::RLocker l(c->lock);
      return it->second;
    }
    int status() {
      return 0;
    }
  };

  pmb_handle *store;
  ceph::unordered_map<coll_t, CollectionRef> coll_map;
  RWLock coll_lock;    ///< rwlock to protect coll_map
  Mutex apply_lock;    ///< serialize all updates

  CollectionRef get_collection(coll_t cid);

  Finisher finisher;

  uint64_t used_bytes;
  uint32_t data_blocksize;
  uint32_t meta_blocksize;

  int _do_transaction(Transaction& t, uint64_t tx_id);
  /// -----------------------------

  int _touch(uint64_t tx_id, coll_t cid, const ghobject_t& oid);
  int _write(uint64_t tx_id, coll_t cid, const ghobject_t& oid, uint64_t offset,
              size_t len, bufferlist& bl,  uint32_t fadvsie_flags = 0);
  int _zero(uint64_t tx_id, coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len);
  int _truncate(uint64_t tx_id, coll_t cid, const ghobject_t& oid, uint64_t size);
  int _remove(uint64_t tx_id, coll_t cid, const ghobject_t& oid);
  int _setattrs(uint64_t tx_id, coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset);
  int _rmattr(uint64_t tx_id, coll_t cid, const ghobject_t& oid, const char *name);
  int _rmattrs(uint64_t tx_id, coll_t cid, const ghobject_t& oid);
  int _clone(uint64_t tx_id, coll_t cid, const ghobject_t& oldoid, const ghobject_t& newoid);
  int _clone_range(uint64_t tx_id, coll_t cid, const ghobject_t& oldoid,
                  const ghobject_t& newoid, uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _omap_clear(uint64_t tx_id, coll_t cid, const ghobject_t &oid);
  int _omap_setkeys(uint64_t tx_id, coll_t cid, const ghobject_t &oid,
		    map<string, bufferlist> &aset);
  int _omap_rmkeys(uint64_t tx_id, coll_t cid, const ghobject_t &oid, const set<string> &keys);
  int _omap_rmkeyrange(uint64_t tx_id, coll_t cid, const ghobject_t &oid,
		       const string& first, const string& last);
  int _omap_setheader(uint64_t tx_id, coll_t cid, const ghobject_t &oid, bufferlist &bl);

  int _collection_hint_expected_num_objs(coll_t cid, uint32_t pg_num,
      uint64_t num_objs) const { return 0; }
  int _create_collection(uint64_t tx_id, coll_t c);
  int _destroy_collection(uint64_t tx_id, coll_t c);
  int _collection_add(uint64_t tx_id, coll_t cid, coll_t ocid, const ghobject_t& oid);
  int _collection_move_rename(uint64_t tx_id, coll_t oldcid, const ghobject_t& oldoid,
			      coll_t cid, const ghobject_t& o);
  int _split_collection(uint64_t tx_id, coll_t cid, uint32_t bits, uint32_t rem, coll_t dest);

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
      sharded(false) {}
  ~PMStore() { }

  virtual bool needs_journal() { return false; };
  virtual bool wants_journal() { return false; };
  virtual bool allows_journal() { return false; };
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

  bool exists(coll_t cid, const ghobject_t& oid);
  int stat(
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false); // struct stat?
  int read(
    coll_t cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0,
    bool allow_eio = false);
  int fiemap(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);
  int getattr(coll_t cid, const ghobject_t& oid, const char *name, bufferptr& value);
  int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset);

  int list_collections(vector<coll_t>& ls);
  bool collection_exists(coll_t c);
  bool collection_empty(coll_t c);
  int collection_list(coll_t cid, ghobject_t start, ghobject_t end,
      bool sort_bitwise, int max, vector<ghobject_t> *ls, ghobject_t *next);

  int omap_get(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    );

  /// Get omap header
  int omap_get_header(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    );

  /// Get keys defined on oid
  int omap_get_keys(
    coll_t cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    );

  /// Get key values
  int omap_get_values(
    coll_t cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    );

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    );

  ObjectMap::ObjectMapIterator get_omap_iterator(
    coll_t cid,              ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    );

  void set_fsid(uuid_d u);
  uuid_d get_fsid();

  objectstore_perf_stat_t get_cur_stats();

  int queue_transactions(
    Sequencer *osr, list<Transaction*>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL);
};

#endif
