// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "acconfig.h"

#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif

#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#include <cstring>
#include <iostream>
#include <os/kstore/kv.h>

#include "include/types.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "PMStore.h"

//#ifdef WITH_LTTNG
//#define TRACEPOINT_DEFINE
//#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
//#include "tracing/objectstore.h"
//#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
//#undef TRACEPOINT_DEFINE
//#else
#define tracepoint(...)
//#endif

#define dout_subsys ceph_subsys_pmstore
#undef dout_prefix
#define dout_prefix *_dout << "pmstore::object "

const string PREFIX_SUPER = "S";   // field -> value
const string PREFIX_OMAP = "M";    // u64 + keyname -> value

// for comparing collections for lock ordering
bool operator>(const PMStore::CollectionRef& l,
	       const PMStore::CollectionRef& r)
{
  return (unsigned long)l.get() > (unsigned long)r.get();
}

// '-' < '.' < '~'
static void get_omap_header(uint64_t id, string *out)
{
  _key_encode_u64(id, out);
  out->push_back('-');
}

static void get_omap_tail(uint64_t id, string *out)
{
  _key_encode_u64(id, out);
  out->push_back('~');
}

// hmm, I don't think there's any need to escape the user key since we
// have a clean prefix.
static void get_omap_key(uint64_t id, const string& key, string *out)
{
  _key_encode_u64(id, out);
  out->push_back('.');
  out->append(key);
}

static void decode_omap_key(const string& key, string *user_key)
{
  *user_key = key.substr(sizeof(uint64_t) + 1);
}

static string pretty_binary_string(const string& in)
{
  char buf[10];
  string out;
  out.reserve(in.length() * 3);
  enum { NONE, HEX, STRING } mode = NONE;
  unsigned from = 0, i;
  for (i=0; i < in.length(); ++i) {
    if ((in[i] < 32 || (unsigned char)in[i] > 126) ||
        (mode == HEX && in.length() - i >= 4 &&
         ((in[i] < 32 || (unsigned char)in[i] > 126) ||
          (in[i+1] < 32 || (unsigned char)in[i+1] > 126) ||
          (in[i+2] < 32 || (unsigned char)in[i+2] > 126) ||
          (in[i+3] < 32 || (unsigned char)in[i+3] > 126)))) {
      if (mode == STRING) {
        out.append(in.substr(from, i - from));
        out.push_back('\'');
      }
      if (mode != HEX) {
        out.append("0x");
        mode = HEX;
      }
      if (in.length() - i >= 4) {
        // print a whole u32 at once
        snprintf(buf, sizeof(buf), "%08x",
                 (uint32_t)(((unsigned char)in[i] << 24) |
                            ((unsigned char)in[i+1] << 16) |
                            ((unsigned char)in[i+2] << 8) |
                            ((unsigned char)in[i+3] << 0)));
        i += 3;
      } else {
        snprintf(buf, sizeof(buf), "%02x", (int)(unsigned char)in[i]);
      }
      out.append(buf);
    } else {
      if (mode != STRING) {
        out.push_back('\'');
        mode = STRING;
        from = i;
      }
    }
  }
  if (mode == STRING) {
    out.append(in.substr(from, i - from));
    out.push_back('\'');
  }
  return out;
}

// ~~~~~~~~~~~~~

int PMStore::Object::write(TXctxRef txc, const coll_t& cid, const ghobject_t& oid,
                           const uint32_t data_blocksize, void* buf, size_t offset, size_t len)
{
  size_t old_size = used_blocks;
  size_t new_size = old_size;
  size_t start, end;
  start = offset / data_blocksize;
  if (offset + len > 0) {
    end = (offset + len - 1) / data_blocksize;
  } else {
    end = 0;
  }

  if (data.size() <= end) {
    for (size_t i = data.size(); i <= end; i++) {
      data.push_back(0);
    }
  }

  uint64_t result;
  size_t remining = len;
  pmb_pair kvp;
  bufferlist write_key;

  // regular writes
  for (size_t i = start; i <= end; i++) {
    encode_key(write_key, cid, oid, i);
    kvp.key = (void *) write_key.c_str();
    kvp.key_len = write_key.length();

    kvp.val = buf;
    kvp.blk_id = data[i];
    kvp.id = id;
    // we're adding new block
    if (kvp.blk_id == 0)
      new_size++;

    if (i == start) {
      kvp.offset = offset % data_blocksize;
      kvp.val_len = remining + kvp.offset <= data_blocksize ? \
        remining : data_blocksize - kvp.offset;
    } else {
      kvp.offset = 0;
      kvp.val_len = remining < data_blocksize ?
        remining : data_blocksize;
    }
    dout(5) << __func__ << " BEFORE: oid: " << kvp.blk_id << " offset: " << kvp.offset << " len: " << kvp.val_len << dendl;

    result = pmb_tput(txc->store, txc->get_tx_id(), &kvp);

    dout(5) << __func__ << " AFTER: oid: " << kvp.blk_id << dendl;

    if (result != PMB_OK) {
      dout(5) << __func__ << " result: " << result << dendl;
      if (result == PMB_ENOSPC) {
        return -ENOSPC;
      } else {
        return -EIO;
      }
    } else {
      data[i] = kvp.blk_id;
    }
    remining -= kvp.val_len;
    if (remining > 0) {
      buf += kvp.val_len;
    }
  }

  if(data_len < (offset + len)) {
    data_len = offset + len;
  }

  used_blocks = new_size;

  return ((new_size - old_size) * data_blocksize);
}

int PMStore::Object::touch(TXctxRef txc, const coll_t& cid, const ghobject_t& oid)
{
  int result = 0;
  if (used_blocks == 0) {
    pmb_pair kvp = {0};
    bufferlist key;
    encode_key(key, cid, oid, 0);
    kvp.key = key.c_str();
    kvp.key_len = key.length();
    kvp.id = id;
    result = pmb_tput(txc->store, txc->get_tx_id(), &kvp);
  }

  if (result) {
    dout(0) << __func__ << " cannot write datablock, error code: " << result << dendl;
  }

  return result;
}

/*
 * Truncates object to requested size. First map "len" to appropiate block index, then decide wheter blocks should be
 * added to the object or removed.
 */
int PMStore::Object::truncate(TXctxRef txc, const coll_t& cid, const ghobject_t& oid,
                              const uint64_t data_blocksize, size_t len, int* diff)
{
  uint64_t requested_block = len / data_blocksize;
  if (len % data_blocksize) {
    ++requested_block;
  } else if (len == 0) {
    requested_block = 1;
  }

  pmb_pair kvp = {0};
  uint8_t result;
  dout(5) << __func__ << " len: " << len << " req_block " << requested_block << " data.size()" << data.size() << dendl;
  if (data.size() < requested_block) {
    // add blocks to the object
    bufferlist write_key;

    for(size_t i = data.size(); i < requested_block; i++) {
      kvp.blk_id = 0;
      encode_key(write_key, cid, oid, i);
      kvp.key = (void *) write_key.c_str();
      kvp.key_len = write_key.length();
      kvp.id = id;
      result = pmb_tput(txc->store, txc->get_tx_id(), &kvp);
      if (result != PMB_OK)
        goto err;
      data.push_back(kvp.blk_id);
      ++used_blocks;
    }
  } else if (data.size() > requested_block) {
    // remove blocks from the end
    for(uint64_t i = data.size(); i > requested_block; --i) {
      dout(0) << __func__ << " truncating: " << data[i] << dendl;
      result = pmb_tdel(txc->store, txc->get_tx_id(), data[i]);
      if (result != PMB_OK) {
        goto err;
      }
      data[i] = 0;
      --used_blocks;
    }
  }

  if (len % data_blocksize != 0) {
    // truncate most likely is within boundary of single block, zero it
    kvp = {0};
    pmb_get(txc->store, data[len / data_blocksize], &kvp);

    size_t off = len % data_blocksize;
    bufferlist bl;
    if (kvp.val_len < off) {
      bl.append((const char*) kvp.val, kvp.val_len);
      bl.append_zero(off - kvp.val_len);
    } else {
      bl.append((const char*) kvp.val, off);
    }
    kvp.val_len = off;
    kvp.val = bl.c_str();
    kvp.id = id;
    result = pmb_tdel(txc->store, txc->get_tx_id(), kvp.blk_id);
    if (result != PMB_OK) {
      goto err;
    }
    kvp.blk_id = 0;
    result = pmb_tput(txc->store, txc->get_tx_id(), &kvp);
    if (result != PMB_OK) {
      goto err;
    }
    data[len / data_blocksize] = kvp.blk_id;
  }

  *diff = data_len - len;
  data_len = len;
  return 0;

err:
  dout(0) << __func__ << " error modify block: " << result << \
          " key len: " << kvp.key_len << " val len: " << kvp.val_len  << dendl;
  return -EIO;
}

int PMStore::Object::write_xattr(TXctxRef txc, const coll_t& cid,
                                 const ghobject_t& oid, map<string, bufferptr>& attrs) {

  pmb_pair kvp = {0};

  if (xattrs != 0) {
    // populate xattrs with missing values
    assert(pmb_get(txc->store, xattrs, &kvp) == PMB_OK);
    bufferlist bl;
    bl.append((const char*) kvp.val, kvp.val_len);
    bufferlist::iterator bi = bl.begin();
    map<string,bufferptr> old_xattrs;
    ::decode(old_xattrs, bi);
    for(map<string,bufferptr>::iterator i = old_xattrs.begin(); i != old_xattrs.end(); ++i) {
      if (!attrs.count(i->first)) {
        attrs.insert(pair<string,bufferptr>(i->first, i->second));
      }
    }
  }

  for(map<string, bufferptr>::iterator i = attrs.begin(); i != attrs.end(); ++i) {
    xattr_keys.insert(i->first);
  }

  memset(&kvp, 0, sizeof(kvp));

  kvp.blk_id = xattrs;

  bufferlist xkey;
  encode_key_meta(xkey, cid, oid, 'x');
  kvp.key = (void *) xkey.c_str();
  kvp.key_len = xkey.length();

  // encode map in single buffer
  bufferlist to_write;
  ::encode(attrs, to_write);
  kvp.val = (void *) to_write.c_str();
  kvp.val_len = to_write.length();

  dout(5) << __func__ << " cid: " << cid << " oid: " << oid << \
    " key_len: " << kvp.key_len << " val_len: " << kvp.val_len << dendl;

  int result = pmb_tput_meta(txc->store, txc->get_tx_id(), &kvp);

  if (result == PMB_OK) {
    xattrs = kvp.blk_id;
  } else {
    dout(0) << __func__ << " error writing xattrs: " << result << \
      " key len: " << kvp.key_len << " val len: " << kvp.val_len  << dendl;
  }

  return result;
}

int PMStore::Object::write_xattr(TXctxRef txc, const coll_t& cid,
                                 const ghobject_t& oid, void* buf, const size_t len)
{
  pmb_pair kvp = {0};
  bufferlist xkey;
  encode_key_meta(xkey, cid, oid, 'x');
  kvp.key = xkey.c_str();
  kvp.key_len = xkey.length();
  kvp.val = buf;
  kvp.val_len = len;

  dout(5) << __func__ << " cid: " << cid << " oid: " << oid << \
    " key_len: " << kvp.key_len << " val_len: " << kvp.val_len << dendl;

  int result = pmb_tput_meta(txc->store, txc->get_tx_id(), &kvp);

  if (result == PMB_OK) {
    xattrs = kvp.blk_id;
  } else {
    dout(0) << __func__ << " error writing xattrs: " << result << \
      " key len: " << kvp.key_len << " val len: " << kvp.val_len  << dendl;
  }

  return result;
}

int PMStore::Object::remove_xattr(TXctxRef txc, const char* xattr_key) {
  if (!xattr_keys.count(xattr_key)) {
    return -ENOENT;
  }

  if (!xattrs) {
    return -ENOENT;
  }

  pmb_pair kvp = {0};
  int result = pmb_get(txc->store, xattrs, &kvp);
  if (result != PMB_OK) {
    dout(0) << __func__ << " ERROR remove attr: " << xattr_key << " "
       << " blk_id: " << xattrs << dendl;
    return -ENODATA;
  }

  map<string,bufferptr> aset;
  {
    bufferlist bl;
    bl.append((const char*) kvp.val, kvp.val_len);
    bufferlist::iterator bi = bl.begin();
    ::decode(aset, bi);
  }

  // change to aset->find
  for(auto it = aset.begin(); it != aset.end(); ++it) {
    if (!it->first.compare(xattr_key)) {
      aset.erase(it);
    }
  }

  bufferlist to_write;
  ::encode(aset, to_write);
  kvp.val = (void *) to_write.c_str();
  kvp.val_len = to_write.length();
  kvp.blk_id = xattrs;

  result = pmb_tput_meta(txc->store, txc->get_tx_id(), &kvp);

  if (result == PMB_OK) {
    xattrs = kvp.blk_id;

    for(auto iset = xattr_keys.begin(); iset != xattr_keys.end(); ++iset) {
      if (!iset->compare(xattr_key)) {
        xattr_keys.erase(iset);
      }
    }
  } else {
    dout(0) << __func__ << " error writing xattrs: " << result << \
      " key len: " << kvp.key_len << " val len: " << kvp.val_len  << dendl;
  }

  dout(4) << __func__ << " removed attr: " << xattr_key << " len: " << kvp.val_len << dendl;
  return result;
}

/*
 * Changes cid and oid for an object
 */
int PMStore::Object::change_key(TXctxRef txc, const coll_t& newcid,
                                const ghobject_t& newoid) {

  pmb_pair kvp = {0};

  int result = 0;

  // rewrite blocks
  for(size_t i = 0; i < data.size(); i++) {
    if (data[i] == 0) {
      continue;
    }

    pmb_get(txc->store, data[i], &kvp);
    bufferlist write_key;
    encode_key(write_key, newcid, newoid, i);
    kvp.key = (void*) write_key.c_str();
    kvp.key_len = write_key.length();

    result = pmb_tput(txc->store, txc->get_tx_id(), &kvp);

    if (result != PMB_OK) {
      if (result == PMB_ENOSPC) {
        return -ENOSPC;
      } else {
        return -EIO;
      }
    } else {
      data[i] = kvp.blk_id;
    }
  }

  // XXX rewrite omap_header
//  if (omap_header != 0) {
//    pmb_get(txc->store, omap_header, &kvp);
//    bufferlist bl;
//    bl.append((const char*) kvp.val, kvp.val_len);
//    result = write_omap_header(txc, newcid, newoid, bl);
//  }

  return result;
}

#undef dout_prefix
#define dout_prefix *_dout << "pmstore "

void PMStore::_assign_id(TXctxRef txc, ObjectRef o)
{
  if (o->id)
    return;
  std::lock_guard<std::mutex> l(id_lock);
  o->id = ++id_last;
  dout(20) << __func__ << " " << o->id << dendl;
  if (id_last > id_max) {
    id_max += g_conf->bluestore_nid_prealloc;
    bufferlist bl;
    ::encode(id_max, bl);
    txc->t->set(PREFIX_SUPER, "id_max", bl);
    dout(5) << __func__ << " id_max now " << id_max << dendl;
  }
}

int PMStore::peek_journal_fsid(uuid_d *fsid)
{
  *fsid = uuid_d();
  return 0;
}

int PMStore::mount()
{
  dout(4) << __func__ << dendl;
  int r = _load();
  if (r < 0) {
    return r;
  }
  //op_tp.start();
  finisher.start();
  return 0;
}

int PMStore::umount()
{
  dout(4) << __func__ << dendl;

  //op_tp.stop();
  finisher.stop();
  int retval= _save();
  return retval;
}

int PMStore::_save()
{
  dout(4) << __func__ << dendl;
  Mutex::Locker l(apply_lock); // block any writer
  dump_all();
  set<coll_t> collections;
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    dout(5) << __func__ << " coll " << p->first << " " << p->second << dendl;
    collections.insert(p->first);
    bufferlist bl;
    assert(p->second);
    p->second->encode(bl);
    string fn = path + "/" + stringify(p->first);
    int r = bl.write_file(fn.c_str());
    if (r < 0) {
      return r;
    }
  }

  string fn = path + "/collections";
  bufferlist bl;
  ::encode(collections, bl);
  int r = bl.write_file(fn.c_str());
  if (r < 0) {
      return r;
  }

  if (sharded) {
    string fn = path + "/sharded";
    bufferlist bl;
    int r = bl.write_file(fn.c_str());
    if (r < 0) {
      return r;
    }
  }

  if (store != NULL) {
    pmb_close(store);
    store = NULL;
  }

  if (!db) {
    delete db;
    db = NULL;
  }

  return 0;
}

void PMStore::dump_all()
{
  dout(4) << __func__ << dendl;

  Formatter *f = Formatter::create("json-pretty");
  f->open_object_section("store");
  dump(f);
  f->close_section();
  dout(0) << "dump:";
  f->flush(*_dout);
  *_dout << dendl;
  delete f;
}

void PMStore::dump(Formatter *f)
{
  dout(4) << __func__ << dendl;

  f->open_array_section("collections");
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    f->open_object_section("collection");
    f->dump_string("name", stringify(p->first));

    f->open_array_section("objects");
    for (map<ghobject_t,ObjectRef>::iterator q = p->second->object_map.begin();
	 q != p->second->object_map.end();
	 ++q) {
      f->open_object_section("object");
      f->dump_string("name", stringify(q->first));
      if (q->second) {
        q->second->dump(f);
      }
      f->close_section();
    }
    f->close_section();

    f->close_section();
  }
  f->close_section();
}

int PMStore::_load()
{
  dout(4) << __func__ << dendl;

  bufferlist bl;
  string fn = path + "/collections";
  string err;
  int r = bl.read_file(fn.c_str(), &err);
  if (r < 0) {
    return r;
  }

  set<coll_t> collections;
  bufferlist::iterator p = bl.begin();
  ::decode(collections, p);

  for (set<coll_t>::iterator q = collections.begin();
       q != collections.end();
       ++q) {
    string fn = path + "/" + stringify(*q);
    bufferlist cbl;
    int r = cbl.read_file(fn.c_str(), &err);
    if (r < 0) {
      return r;
    }
    CollectionRef c(new Collection);
    bufferlist::iterator p = cbl.begin();
    c->decode(p);
    coll_map[*q] = c;
    used_bytes += c->used_bytes();
  }

  fn = path + "/sharded";
  struct stat st;
  if (::stat(fn.c_str(), &st) == 0) {
    set_allow_sharded_objects();
  }

  dump_all();

  pmb_opts opts = {};

  // common
  fn = path + "/data.pool";
  opts.path = fn.c_str();
  opts.write_log_entries = g_conf->pmstore_tx_slots;

  // data area
  opts.data_size = g_conf->pmstore_size;
  opts.max_key_len = g_conf->pmstore_max_key_len;
  opts.max_val_len = g_conf->pmstore_max_val_len;

  // meta area
  opts.meta_max_key_len = g_conf->pmstore_meta_max_key_len;
  opts.meta_max_val_len = g_conf->pmstore_meta_max_val_len;
  opts.meta_size = g_conf->pmstore_meta_size;

  if (!g_conf->pmstore_sync.compare("sync")) {
    dout(0) << __func__ << " sync type: " << g_conf->pmstore_sync << dendl;
    opts.sync_type = PMB_SYNC;
  } else if (!g_conf->pmstore_sync.compare("async")) {
    dout(0) << __func__ << " sync type: " << g_conf->pmstore_sync << dendl;
    opts.sync_type = PMB_ASYNC;
  } else if (!g_conf->pmstore_sync.compare("nosync")) {
    dout(0) << __func__ << " sync type: " << g_conf->pmstore_sync << dendl;
    opts.sync_type = PMB_NOSYNC;
  } else if (!g_conf->pmstore_sync.compare("thsync")) {
    dout(0) << __func__ << " sync type: " << g_conf->pmstore_sync << dendl;
    opts.sync_type = PMB_THSYNC;
  } else {
    dout(0) << __func__ << " wrong sync type: " << g_conf->pmstore_sync << dendl;
    assert(0 == "wrong sync type for pmbackend!");
  }

  uint8_t error;
  dout(4) << __func__ << " open PMStore data.pool at: " << opts.path << dendl;
  store = pmb_open(&opts, &error);

  if (store == NULL) {
    dout(0) << __func__ << " ERROR cannot open data.pool: " << error << dendl;
    return -1;
  }


  pmb_iter *iter = pmb_iter_open(store, PMB_DATA);
  pmb_pair kvp;
  while(pmb_iter_valid(iter)) {
    dout(4) << __func__ << " current iter @ blk_id: " << pmb_iter_pos(iter) << dendl;
    r = pmb_iter_get(iter, &kvp);
    if (r != PMB_OK) {
      dout(4) << __func__ << " ERROR processing iterator!" << dendl;
    } else if (kvp.key_len != 0) {
      _add_object(&kvp);
    } else {
      dout(0) << __func__ << " ERROR iterator returned empty object: " << kvp.blk_id << dendl;
    }
    pmb_iter_next(iter);
  }
  pmb_iter_close(iter);

  // ~~~~~~~~~~~~~~~~~~~~~

  db = KeyValueDB::create(g_ceph_context,
                          "rocksdb",
                          path + "/db",
                          NULL);
  stringstream rdberr;
  string options = g_conf->bluestore_rocksdb_options;
  db->init(options);
  r = db->open(rdberr);
  assert(r == 0);

  id_max = 0;
  bufferlist ibl;
  db->get(PREFIX_SUPER, "id_max", &ibl);
  try {
    ::decode(id_max, ibl);
  } catch (buffer::error& e) {
  }
  dout(5) << __func__ << " old iid_max " << id_max << dendl;
  id_last = id_max;

  // ~~~~~~~~~~~~~~~~~~~~
  iter = pmb_iter_open(store, PMB_META);
  while(pmb_iter_valid(iter)) {
    dout(4) << __func__ << " current iter @ blk_id: " << pmb_iter_pos(iter) << dendl;
    r = pmb_iter_get(iter, &kvp);
    if (r != PMB_OK) {
      dout(0) << __func__ << " ERROR processing iterator!" << dendl;
    } else if (kvp.key_len != 0) {
      _add_meta_object(&kvp);
    } else {
      dout(0) << __func__ << " ERROR iterator returned empty object: " << kvp.blk_id << dendl;
    }
    pmb_iter_next(iter);
  }
  pmb_iter_close(iter);

  dout(0) << __func__ << " finished" << dendl;

  return 0;
}

void PMStore::_add_object(pmb_pair *kvp)
{
  dout(4) << __func__ << dendl;

  bufferlist bl;
  bl.append((const char*) kvp->key, kvp->key_len);
  coll_t cid;
  ghobject_t oid;
  uint32_t part;

  // decode key: coll_t, ghobject_t, part
  bufferlist::iterator bi = bl.begin();
  DECODE_START(1, bi);
  decode(cid, bi);
  decode(oid, bi);
  decode(part, bi);
  DECODE_FINISH(bi);

  CollectionRef c = get_collection(cid);
  if (!c) {
    dout(4) << __func__ << " ERROR collection not found!" << dendl;

    return;
  }

  dout(4) << __func__ << " before getting ObjectRef:" << oid << dendl;
  ObjectRef o = c->get_or_create_object(oid);

  dout(4) << __func__ << " adding part: " << part << " blk_id: " << kvp->blk_id
    << " block size: " << data_blocksize << dendl;
  o->add(part, kvp->blk_id);
  if((part + 1) * data_blocksize > o->data_len) {
    o->data_len = part * data_blocksize + kvp->val_len;
  }
  dout(4) << __func__ << " object data_len: " << o->data_len << dendl;
}

/*
 * Format: type$...
 * Type: x
 * Type: h
 */
void PMStore::_add_meta_object(pmb_pair *kvp)
{
  dout(4) << __func__ << dendl;

  bufferlist bl;
  bl.append((const char*) kvp->key, kvp->key_len);
  coll_t cid;
  ghobject_t oid;
  char type;

  //dout(4) << __func__ << " key: " << key << " key_len: " << kvp->key_len <<
  //  " val_len: " << kvp->val_len << dendl;

  // decode key: coll_t, ghobject_t, part
  bufferlist::iterator bi = bl.begin();
  DECODE_START(1, bi);
  decode(cid, bi);
  decode(oid, bi);
  decode(type, bi);
  DECODE_FINISH(bi);

  // find collection and object, both should exist at this point
  CollectionRef c = get_collection(cid);
  if (!c) {
    dout(4) << __func__ << " ERROR collection not found!" << dendl;

    return;
  }

  ObjectRef o = c->get_object(oid);
  if(!o) {
    dout(4) << __func__ << " ERROR object not found!" << dendl;

    return;
  }

  switch(type) {
    case 'x':
      {
        bufferlist xbl;
        xbl.append((const char*) kvp->val, kvp->val_len);
        bufferlist::iterator xbi = xbl.begin();
        map<string, bufferptr> obj_xattrs;
        DECODE_START(1, xbi);
        ::decode(obj_xattrs, xbi);
        DECODE_FINISH(xbi);
        o->xattrs = kvp->blk_id;
        for(pair<string, bufferptr> i : obj_xattrs) {
          o->xattr_keys.insert(i.first);
        }
        break;
      }
    case 'o':
      {
        dout(0) << __func__ << "WRONG OMAP: " << (const char* ) kvp->key << dendl;
        assert(0);
//        bufferlist obl;
//        obl.append((const char *)kvp->val, kvp->val_len);
//        bufferlist::iterator obi = obl.begin();
//        map<string, bufferlist> obj_omaps;
//        DECODE_START(1, obi);
//        ::decode(obj_omaps, obi);
//        DECODE_FINISH(obi);
//        o->omaps = kvp->blk_id;
//        for(pair<string, bufferlist> i : obj_omaps) {
//          o->omap_keys.insert(i.first);
//        }
//        break;
      }
    case 'h':
      dout(0) << __func__ << "WRONG OMAP: " << (const char* ) kvp->key << dendl;
      assert(0);
      //o->omap_header = kvp->blk_id;
      break;
    default:
      assert(0 == "invalid meta type in store");
  }
}

void PMStore::set_fsid(uuid_d u)
{
  dout(4) << __func__ << dendl;

  int r = write_meta("fs_fsid", stringify(u));
  assert(r >= 0);
}

uuid_d PMStore::get_fsid()
{
  dout(4) << __func__ << dendl;

  string fsid_str;
  int r = read_meta("fs_fsid", &fsid_str);
  assert(r >= 0);
  uuid_d uuid;
  bool b = uuid.parse(fsid_str.c_str());
  assert(b);
  return uuid;
}

int PMStore::mkfs()
{
  dout(4) << __func__ << dendl;

  string fsid_str;
  int r = read_meta("fs_fsid", &fsid_str);
  if (r == -ENOENT) {
    uuid_d fsid;
    fsid.generate_random();
    fsid_str = stringify(fsid);
    r = write_meta("fs_fsid", fsid_str);
    if (r < 0) {
      return r;
    }
    dout(1) << __func__ << " new fsid " << fsid_str << dendl;
  } else {
    dout(1) << __func__ << " had fsid " << fsid_str << dendl;
  }

  string fn = path + "/collections";
  derr << path << dendl;
  bufferlist bl;
  set<coll_t> collections;
  ::encode(collections, bl);
  r = bl.write_file(fn.c_str());
  if (r < 0) {
    return r;
  }

  stringstream err;
  ::mkdir((path + "/db").c_str(), 0755);
  db = KeyValueDB::create(g_ceph_context,
                          "rocksdb",
                          path + "/db",
                          NULL);
  string options = g_conf->bluestore_rocksdb_options;
  db->init(options);
  r = db->create_and_open(err);
  assert(r == 0);
  delete db;
  db = NULL;

  return 0;
}

int PMStore::statfs(struct statfs *st)
{
  dout(4) << __func__ << dendl;

  st->f_bsize = data_blocksize;
  st->f_bfree = st->f_bavail = pmb_nfree(store, PMB_DATA);
  st->f_blocks = pmb_ntotal(store, PMB_DATA);
  return 0;
}

objectstore_perf_stat_t PMStore::get_cur_stats()
{
  // fixme
  return objectstore_perf_stat_t();
}

PMStore::CollectionRef PMStore::get_collection(const coll_t& cid)
{
  RWLock::RLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end()) {
    return CollectionRef();
  }
  return cp->second;
}


// ---------------
// read operations

bool PMStore::exists(const coll_t& cid, const ghobject_t& oid)
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return false;
  }

  // Perform equivalent of c->get_object_(oid) != NULL. In C++11 the
  // shared_ptr needs to be compared to nullptr.
  bool retval = (bool)c->get_object(oid);
  return retval;
}

int PMStore::stat(
    const coll_t& cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio)
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }
  st->st_size = o->data_len;
  st->st_blksize = data_blocksize;
  st->st_blocks = o->used_blocks;
  st->st_nlink = 1;
  return 0;
}

int PMStore::read(
    const coll_t& cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags,
    bool allow_eio)
{
  dout(4) << __func__ << " " << cid << " " << oid << " "
	   << offset << "~" << len << dendl;

  CollectionRef c = get_collection(cid);
  if (!c){
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  dout(4) << __func__ << " oid: " << oid << " object data_len: " << o->data_len << dendl;
  if (offset >= o->data_len) {
    return 0;
  }
  size_t l = len;
  if (l == 0) { // note: len == 0 means read the entire object
    l = o->data_len;
  } else if (offset + l > o->data_len) {
    l = o->data_len - offset;
  }
  bl.clear();

  uint64_t start, end, block_offset, remining;
  start = offset / data_blocksize;
  end = (offset + l) / data_blocksize;
  block_offset = start * data_blocksize;
  remining = l;

  dout(4) << __func__ << " " << oid << " data_len: " << o->data_len << " offset: " << offset <<
    " block_offset: " << block_offset <<  " remining: " << remining <<
    " start: " << start << " end: " << end << dendl;

  uint8_t status;
  pmb_pair kvp;
  if (offset > block_offset) {
    if (o->data.size() > start && o->data[start] != 0) {
      // block exists
      dout(0) << __func__ << " block exists" << dendl;
      status = pmb_get(store, o->data[start], &kvp);
      if (status != PMB_OK) {
        dout(0) << __func__ << " ERROR reading block: " <<
          o->data[start] << " error code: " << status << dendl;
        return -EIO;
      }

      // check if block was only "touched" -> if yes fill buffer with zeros
      // else read data
      if (kvp.val_len == 0) {
        // empty block
        // block don't exist, return empty buffer
        if (remining < (data_blocksize - (offset - block_offset))) {
          kvp.val_len = remining;
        } else {
          kvp.val_len = data_blocksize - (offset - block_offset);
        }
        dout(4) << __func__ << " allocated, but empty block, appending " << kvp.val_len << " zeros" << dendl;
        bl.append_zero(kvp.val_len);
      } else {
        dout(0) << __func__ << " val len: " << kvp.val_len << dendl;
        if (remining < (data_blocksize - (offset - block_offset))) {
          kvp.val_len = remining;
        } else {
          kvp.val_len = data_blocksize - (offset - block_offset);
        }
        dout(0) << __func__ << " allocated, non empty block, reading block: " << \
        o->data[start] << " len: " << kvp.val_len << dendl;
        kvp.val += (offset - block_offset);
        bl.append((const char*) kvp.val, kvp.val_len);
      }
    } else {
      // block don't exist, return empty buffer
      if (remining < (data_blocksize - (offset - block_offset))) {
        kvp.val_len = remining;
      } else {
        kvp.val_len = data_blocksize - (offset - block_offset);
      }
      dout(4) << __func__ << " block " << start << " don't exist, appending " << kvp.val_len << " zeros" << dendl;
      bl.append_zero(kvp.val_len);
    }
    start++;
    remining -= kvp.val_len;
  }

  while(start <= end && remining > 0) {
    if (o->data.size() > start && o->data[start] != 0) {
      status = pmb_get(store, o->data[start], &kvp);
      if (status != PMB_OK) {
        dout(0) << __func__ << " ERROR reading block: " << o->data[start] << " error code: " << status << dendl;
        return -EIO;
      }

      if (kvp.val_len == 0) {
        // append zeros to the bufferlist
        kvp.val_len = remining < data_blocksize ? remining : data_blocksize;
        bl.append_zero(kvp.val_len);
      } else {
        kvp.val_len = remining < data_blocksize ? remining : data_blocksize;
        bl.append((const char*) kvp.val, kvp.val_len);
      }
    } else {
      // we're after writen part, append 0-filled buffer
      kvp.val_len = remining < data_blocksize ? remining : data_blocksize;
      bl.append_zero(kvp.val_len);
    }
    start++;
    remining -= kvp.val_len;
  }

  return bl.length();
}

// fixme
int PMStore::fiemap(const coll_t& cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, bufferlist& bl)
{
  dout(4) << __func__ << " " << cid << " " << oid << " " << offset << "~"
	   << len << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  map<uint64_t, uint64_t> m;
  if (offset >= o->data.size() * data_blocksize) {
    m[offset] = len;
    ::encode(m, bl);
    return 0;
  }
  size_t l = len;
  if (offset + l > o->data_len) {
    l = o->data.size() * data_blocksize - offset;
  }
  m[offset] = l;
  ::encode(m, bl);
  return 0;
}

int PMStore::getattr(const coll_t& cid, const ghobject_t& oid,
		      const char *name, bufferptr& value)
{
  dout(4) << __func__ << " " << cid << " " << oid << " " << name << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  string k(name);
  if (!o->xattr_keys.count(k)) {
    return -ENODATA;
  }

  pmb_pair kvp;
  int status = pmb_get(store, o->xattrs, &kvp);
  if (status != PMB_OK) {
    dout(0) << __func__ << " ERROR get attr: " << stringify(cid) << " " <<
      stringify(oid) << " " << k << " blk_id: " << o->xattrs << dendl;
    return -ENODATA;
  }

  bufferlist bl;
  bl.append((const char*) kvp.val, kvp.val_len);
  bufferlist::iterator bi = bl.begin();
  map<string,bufferptr> aset;
  ::decode(aset, bi);

  assert(aset.count(k) == 1);
  value = aset[k];

  dout(4) << __func__ << " attr: " << name << " len: " << kvp.val_len << dendl;
  return 0;
}

int PMStore::getattrs(const coll_t& cid, const ghobject_t& oid,
		       map<string,bufferptr>& aset)
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  pmb_pair kvp;
  int status = pmb_get(store, o->xattrs, &kvp);
  if (status != PMB_OK) {
    dout(0) << __func__ << " ERROR get attrs: " << stringify(cid) << " " <<
      stringify(oid) << " " << " blk_id: " << o->xattrs << dendl;
    return -ENODATA;
  }

  bufferlist bl;
  bl.append((const char*) kvp.val, kvp.val_len);
  bufferlist::iterator bi = bl.begin();
  ::decode(aset, bi);

  return 0;
}

int PMStore::list_collections(vector<coll_t>& ls)
{
  dout(4) << __func__ << dendl;

  RWLock::RLocker l(coll_lock);
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    ls.push_back(p->first);
  }
  return 0;
}

bool PMStore::collection_exists(const coll_t& cid)
{
  dout(4) << __func__ << " " << cid << dendl;

  RWLock::RLocker l(coll_lock);
  bool ret = coll_map.count(cid);
  return ret;
}

bool PMStore::collection_empty(const coll_t& cid)
{
  dout(4) << __func__ << " " << cid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }
  RWLock::RLocker l(c->lock);
  bool ret = c->object_map.empty();
  return ret;
}

int PMStore::collection_list(const coll_t& cid, ghobject_t start, ghobject_t end,
                              bool sort_bitwise, int max,
                              vector<ghobject_t> *ls, ghobject_t *next)
{
  dout(4) << __func__ << " " << cid << dendl;

  if (!sort_bitwise) {
    return -EOPNOTSUPP;
  }
  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }
  RWLock::RLocker l(c->lock);

  map<ghobject_t,ObjectRef,ghobject_t::BitwiseComparator>::iterator p = c->object_map.lower_bound(start);
  while (p != c->object_map.end() &&
         ls->size() < (unsigned)max &&
         cmp_bitwise(p->first, end) < 0) {
    ls->push_back(p->first);
    ++p;
  }
  if (next != NULL) {
    if (p == c->object_map.end()) {
      *next = ghobject_t::get_max();
    } else {
      *next = p->first;
    }
  }

  return 0;
}

int PMStore::omap_get(
    const coll_t& cid,                   ///< [in] Collection containing oid
    const ghobject_t &oid,        ///< [in] Object containing omap
    bufferlist *header,           ///< [out] omap header
    map<string, bufferlist> *out) ///< [out] Key to value map
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o || o->id == 0) {
    return -ENOENT;
  }

  KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
  string head, tail;
  get_omap_header(o->id, &head);
  get_omap_tail(o->id, &tail);
  it->lower_bound(head);
  while (it->valid()) {
    if (it->key() == head) {
      dout(5) << __func__ << "  got header" << dendl;
      *header = it->value();
    } else if (it->key() >= tail) {
      dout(5) << __func__ << "  reached tail" << dendl;
      break;
    } else {
      string user_key;
      decode_omap_key(it->key(), &user_key);
      dout(5) << __func__ << "  got " << pretty_binary_string(it->key())
               << " -> " << user_key << dendl;
      assert(it->key() < tail);
      (*out)[user_key] = it->value();
    }
    it->next();
  }

  return 0;
}

int PMStore::omap_get_header(
    const coll_t& cid,        ///< [in] Collection containing oid
    const ghobject_t &oid,    ///< [in] Object containing omap
    bufferlist *header,       ///< [out] omap header
    bool allow_eio)           ///< [in] don't assert on eio
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o || o->id == 0) {
    return -ENOENT;
  }

  string head;
  get_omap_header(o->id, &head);
  if (db->get(PREFIX_OMAP, head, header) >= 0) {
    dout(5) << __func__ << "  got header" << dendl;
  } else {
    dout(5) << __func__ << "  no header" << dendl;
  }

  return 0;
}

int PMStore::omap_get_keys(
    const coll_t& cid,      ///< [in] Collection containing oid
    const ghobject_t &oid,  ///< [in] Object containing omap
    set<string> *keys)      ///< [out] Keys defined on oid
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  {
    KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
    string head, tail;
    get_omap_key(o->id, string(), &head);
    get_omap_tail(o->id, &tail);
    it->lower_bound(head);
    while (it->valid()) {
      if (it->key() >= tail) {
        dout(30) << __func__ << "  reached tail" << dendl;
        break;
      }
      string user_key;
      decode_omap_key(it->key(), &user_key);
      dout(30) << __func__ << "  got " << pretty_binary_string(it->key())
               << " -> " << user_key << dendl;
      assert(it->key() < tail);
      keys->insert(user_key);
      it->next();
    }
  }

  return 0;
}

int PMStore::omap_get_values(
    const coll_t& cid,            ///< [in] Collection containing oid
    const ghobject_t &oid,        ///< [in] Object containing omap
    const set<string> &keys,      ///< [in] Keys to get
    map<string, bufferlist> *out) ///< [out] Returned keys and values
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o || o->id == 0) {
    return -ENOENT;
  }

  for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
    string key;
    get_omap_key(o->id, *p, &key);
    bufferlist val;
    if (db->get(PREFIX_OMAP, key, &val) >= 0) {
      dout(30) << __func__ << "  got " << pretty_binary_string(key)
               << " -> " << *p << dendl;
      out->insert(make_pair(*p, val));
    }
  }

  return 0;
}

int PMStore::omap_check_keys(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    )
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o || o->id == 0) {
    return -ENOENT;
  }

  for (set<string>::const_iterator p = keys.begin(); p != keys.end(); ++p) {
    string key;
    get_omap_key(o->id, *p, &key);
    bufferlist val;
    if (db->get(PREFIX_OMAP, key, &val) >= 0) {
      dout(30) << __func__ << "  have " << pretty_binary_string(key)
               << " -> " << *p << dendl;
      out->insert(*p);
    } else {
      dout(30) << __func__ << "  miss " << pretty_binary_string(key)
               << " -> " << *p << dendl;
    }
  }

  return 0;
}

ObjectMap::ObjectMapIterator PMStore::get_omap_iterator(const coll_t& cid,
							 const ghobject_t& oid)
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return ObjectMap::ObjectMapIterator();
  }

  ObjectRef o = c->get_object(oid);
  if (!o || o->id == 0) {
    return ObjectMap::ObjectMapIterator();
  }
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(c, o, it));
}


// ---------------
// write operations

int PMStore::queue_transactions(Sequencer *osr,
				 vector<Transaction>& tls,
				 TrackedOpRef op,
				 ThreadPool::TPHandle *handle)
{
  dout(4) << __func__ << dendl;

  // because pmstore operations are synchronous, we can implement the
  // Sequencer with a mutex. this guarantees ordering on a given sequencer,
  // while allowing operations on different sequencers to happen in parallel
  struct OpSequencer : public Sequencer_impl {
    std::mutex mutex;
    void flush() override {}
    bool flush_commit(Context*) override { return true; }
  };

  std::unique_lock<std::mutex> lock;
  if (osr) {
    auto seq = reinterpret_cast<OpSequencer**>(&osr->p);
    if (*seq == nullptr) {
      *seq = new OpSequencer;
    }
    lock = std::unique_lock<std::mutex>((*seq)->mutex);
  }

  TXctxRef txctx = new TXctx(store, db);
  txctx->tx_begin();
  for (auto p = tls.begin(); p != tls.end(); ++p) {
    // poke the TPHandle heartbeat just to exercise that code path
    if (handle) {
      handle->reset_tp_timeout();
    }

    if(_do_transaction(*p, txctx) < 0) {
      txctx->tx_abort();
      return -EIO; // check potential error codes
    }
  }
  txctx->tx_finish();

  Context *on_apply = NULL, *on_apply_sync = NULL, *on_commit = NULL;
  ObjectStore::Transaction::collect_contexts(tls, &on_apply, &on_commit,
					     &on_apply_sync);
  if (on_apply_sync) {
    on_apply_sync->complete(0);
  }
  if (on_apply) {
    finisher.queue(on_apply);
  }
  if (on_commit) {
    finisher.queue(on_commit);
  }
  return 0;
}

int PMStore::_do_transaction(Transaction& t, TXctxRef txc)
{
  dout(4) << __func__ << dendl;
  Transaction::iterator i = t.begin();
  int pos = 0;

  while (i.have_op()) {
    Transaction::Op *op = i.decode_op();
    int r = 0;
    switch (op->op) {
    case Transaction::OP_NOP:
      break;

    case Transaction::OP_TOUCH:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        tracepoint(objectstore, touch_enter, osr_name);
        r = _touch(txc, cid, oid);
        tracepoint(objectstore, touch_exit, r);
      }
      break;

    case Transaction::OP_WRITE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        uint32_t fadvise_flags = i.get_fadvise_flags();
        bufferlist bl;
        i.decode_bl(bl);
        tracepoint(objectstore, write_enter, osr_name, off, len);
        r = _write(txc, cid, oid, off, len, bl, fadvise_flags);
        tracepoint(objectstore, write_exit, r);
      }
      break;

    case Transaction::OP_ZERO:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        tracepoint(objectstore, zero_enter, osr_name, off, len);
        r = _zero(txc, cid, oid, off, len);
        tracepoint(objectstore, zero_exit, r);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
        // deprecated, no-op
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        tracepoint(objectstore, truncate_enter, osr_name, off);
        r = _truncate(txc, cid, oid, off);
	tracepoint(objectstore, truncate_exit, r);
      }
      break;

    case Transaction::OP_REMOVE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	tracepoint(objectstore, remove_enter, osr_name);
        r = _remove(txc, cid, oid);
	tracepoint(objectstore, remove_exit, r);
      }
      break;

    case Transaction::OP_SETATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        string name = i.decode_string();
        bufferlist bl;
        i.decode_bl(bl);
        map<string, bufferptr> to_set;
        to_set[name] = bufferptr(bl.c_str(), bl.length());
        tracepoint(objectstore, setattr_enter, osr_name);
        r = _setattrs(txc, cid, oid, to_set);
        tracepoint(objectstore, setattr_exit, r);
      }
      break;

    case Transaction::OP_SETATTRS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        map<string, bufferptr> aset;
        i.decode_attrset(aset);
        tracepoint(objectstore, setattrs_enter, osr_name);
        r = _setattrs(txc, cid, oid, aset);
        tracepoint(objectstore, setattrs_exit, r);
      }
      break;

    case Transaction::OP_RMATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        string name = i.decode_string();
        tracepoint(objectstore, rmattr_enter, osr_name);
        r = _rmattr(txc, cid, oid, name.c_str());
        tracepoint(objectstore, rmattr_exit, r);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        tracepoint(objectstore, rmattrs_enter, osr_name);
        r = _rmattrs(txc, cid, oid);
        tracepoint(objectstore, rmattrs_exit, r);
      }
      break;

    case Transaction::OP_CLONE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
        tracepoint(objectstore, clone_enter, osr_name);
        r = _clone(txc, cid, oid, noid);
        tracepoint(objectstore, clone_exit, r);
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        tracepoint(objectstore, clone_range_enter, osr_name, len);
        r = _clone_range(txc, cid, oid, noid, off, len, off);
        tracepoint(objectstore, clone_range_exit, r);
      }
      break;

    case Transaction::OP_CLONERANGE2:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
        tracepoint(objectstore, clone_range2_enter, osr_name, len);
        r = _clone_range(txc, cid, oid, noid, srcoff ,len, dstoff);
        tracepoint(objectstore, clone_range2_exit, r);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
        tracepoint(objectstore, mkcoll_enter, osr_name);
        r = _create_collection(txc, cid);
        tracepoint(objectstore, mkcoll_exit, r);
      }
      break;

    case Transaction::OP_COLL_HINT:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t type = op->hint_type;
        bufferlist hint;
        i.decode_bl(hint);
        bufferlist::iterator hiter = hint.begin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          ::decode(pg_num, hiter);
          ::decode(num_objs, hiter);
          r = _collection_hint_expected_num_objs(cid, pg_num, num_objs);
        } else {
          // Ignore the hint
          dout(4) << "Unrecognized collection hint type: " << type << dendl;
        }
      }
      break;

    case Transaction::OP_RMCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
        tracepoint(objectstore, rmcoll_enter, osr_name);
        r = _destroy_collection(txc, cid);
        tracepoint(objectstore, rmcoll_exit, r);
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
//        coll_t ocid = i.get_cid(op->cid);
//        coll_t ncid = i.get_cid(op->dest_cid);
//        ghobject_t oid = i.get_oid(op->oid);
//        tracepoint(objectstore, coll_add_enter);
//        r = _collection_add(tx_id, ncid, ocid, oid);
//        tracepoint(objectstore, coll_add_exit, r);
        r = -EOPNOTSUPP;
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        r = _remove(txc, cid, oid);
       }
      break;

    case Transaction::OP_COLL_MOVE:
      {
        // deprecated, no-op
      }
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
        coll_t oldcid = i.get_cid(op->cid);
        ghobject_t oldoid = i.get_oid(op->oid);
        coll_t newcid = i.get_cid(op->dest_cid);
        ghobject_t newoid = i.get_oid(op->dest_oid);
        tracepoint(objectstore, coll_move_rename_enter);
        r = _collection_move_rename(txc, oldcid, oldoid, newcid, newoid);
        tracepoint(objectstore, coll_move_rename_exit, r);
      }
      break;

    case Transaction::OP_COLL_SETATTR:
    case Transaction::OP_COLL_RMATTR:
      {
        // deprecated, no-op
      }
      break;

    case Transaction::OP_COLL_RENAME:
      {
        r = -EOPNOTSUPP;
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        tracepoint(objectstore, omap_clear_enter, osr_name);
        r = _omap_clear(txc, cid, oid);
        tracepoint(objectstore, omap_clear_exit, r);
      }
      break;

    case Transaction::OP_OMAP_SETKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        bufferlist aset_bl;
        i.decode_attrset_bl(&aset_bl);
        tracepoint(objectstore, omap_setkeys_enter, osr_name);
        r = _omap_setkeys(txc, cid, oid, aset_bl);
        tracepoint(objectstore, omap_setkeys_exit, r);
      }
      break;

    case Transaction::OP_OMAP_RMKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        bufferlist keys_bl;
        i.decode_keyset_bl(&keys_bl);
        tracepoint(objectstore, omap_rmkeys_enter, osr_name);
        r = _omap_rmkeys(txc, cid, oid, keys_bl);
        tracepoint(objectstore, omap_rmkeys_exit, r);
      }
      break;

    case Transaction::OP_OMAP_RMKEYRANGE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
        tracepoint(objectstore, omap_rmkeyrange_enter, osr_name);
        r = _omap_rmkeyrange(txc, cid, oid, first, last);
        tracepoint(objectstore, omap_rmkeyrange_exit, r);
      }
      break;

    case Transaction::OP_OMAP_SETHEADER:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        bufferlist bl;
        i.decode_bl(bl);
        tracepoint(objectstore, omap_setheader_enter, osr_name);
        r = _omap_setheader(txc, cid, oid, bl);
        tracepoint(objectstore, omap_setheader_exit, r);
      }
      break;

    case Transaction::OP_SPLIT_COLLECTION:
      {
        r = -EOPNOTSUPP;
      }
      break;

    case Transaction::OP_SPLIT_COLLECTION2:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
        coll_t dest = i.get_cid(op->dest_cid);
        tracepoint(objectstore, split_coll2_enter, osr_name);
        r = _split_collection(txc, cid, bits, rem, dest);
        tracepoint(objectstore, split_coll2_exit, r);
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
        r = -EOPNOTSUPP;
      }
      break;

    default:
      derr << "bad op " << op->op << dendl;
      assert(0);
    }

    if (r < 0) {
      std::cout << "False :( with r=" << r << std::endl;
      bool ok = false;

      if (r == -ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
			    op->op == Transaction::OP_CLONE ||
			    op->op == Transaction::OP_CLONERANGE2 ||
			    op->op == Transaction::OP_COLL_ADD)) {
        // -ENOENT is usually okay
        ok = true;
      }

      if (r == -ENODATA) {
        ok = true;
      }

      if (!ok) {
        const char *msg = "unexpected error code";

        if (r == -ENOENT && (op->op == Transaction::OP_CLONERANGE ||
                 op->op == Transaction::OP_CLONE ||
                 op->op == Transaction::OP_CLONERANGE2)) {
          msg = "ENOENT on clone suggests osd bug";
        }

        if (r == -ENOSPC) {
          // For now, if we hit _any_ ENOSPC, crash, before we do any damage
          // by partially applying transactions.
          msg = "ENOSPC handling not implemented";
        }

        if (r == -ENOTEMPTY) {
          msg = "ENOTEMPTY suggests garbage data in osd data dir";
          dump_all();
        }

        dout(0) << " error " << cpp_strerror(r) << " not handled on operation " << op->op
          << " (op " << pos << ", counting from 0)" << dendl;
        dout(0) << msg << dendl;
        dout(0) << " transaction dump:\n";
        JSONFormatter f(true);
        f.open_object_section("transaction");
        t.dump(&f);
        f.close_section();
        f.flush(*_dout);
        *_dout << dendl;
        assert(0 == "unexpected error");
      }
    }

    ++pos;
  }

  return 0;
}

int PMStore::_touch(TXctxRef txc, const coll_t& cid, const ghobject_t& oid)
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_or_create_object(oid);
  _assign_id(txc, o);

  return o->touch(txc, cid, oid);
}

int PMStore::_write(TXctxRef txc, const coll_t& cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, bufferlist& bl,
		     uint32_t fadvise_flags)
{
  dout(4) << __func__ << " cid: " << cid << " ghobject_t: " <<
    oid << " offset: " << offset << " len: " << len << dendl;
  assert(len == bl.length());

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_or_create_object(oid);
  _assign_id(txc, o);

  int result = o->write(txc, cid, oid, data_blocksize, bl.c_str(), offset, len);
  dout(4) << __func__ << " cid: " << cid << " ghobject_t: " <<
           oid << " result: " << result << dendl;
  if (result >= 0) {
    used_bytes += result;
    return 0;
  } else {
    return result;
  }
}

int PMStore::_zero(TXctxRef txc, const coll_t& cid, const ghobject_t& oid,
		    uint64_t offset, size_t len)
{
  dout(4) << __func__ << " " << cid << " " << oid << " " << offset << "~"
	   << len << dendl;

  bufferlist bl;
  bl.append_zero(len);
  int result = _write(txc, cid, oid, offset, len, bl);
  return result;
}

int PMStore::_truncate(TXctxRef txc, const coll_t& cid, const ghobject_t& oid, uint64_t size)
{
  dout(4) << __func__ << " " << cid << " " << oid << " " << size << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  int diff = 0;
  int result = o->truncate(txc, cid, oid, data_blocksize, size, &diff);

  if (result >= 0) {
    used_bytes += diff;
    return 0;
  } else {
    return result;
  }
}

int PMStore::_remove(TXctxRef txc, const coll_t& cid, const ghobject_t& oid)
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  // remove object data
  for(size_t i = 0; i < o->data.size(); i++) {
    if (o->data[i] != 0) {
      dout(0) << __func__ << " removing: " << o->data[i] << dendl;
      assert(pmb_tdel(store, txc->get_tx_id(), o->data[i]) == PMB_OK);
      o->data[i] = 0;
    }
  }

  // remove associated xattrs and omap
  if (o->id != 0) {
    _do_omap_clear(txc, o->id);
  }

  if (o->xattrs != 0) {
    assert (pmb_tdel(store, txc->get_tx_id(), o->xattrs) == PMB_OK);
    o->xattrs = 0;
    o->xattr_keys.clear();
  }

  c->object_map.erase(oid);
  c->object_hash.erase(oid);

  used_bytes -= o->used_blocks * data_blocksize;
  return 0;
}

int PMStore::_setattrs(TXctxRef txc, const coll_t& cid, const ghobject_t& oid,
			map<string,bufferptr>& aset)
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  return o->write_xattr(txc, cid, oid, aset);
}

int PMStore::_rmattr(TXctxRef txc, const coll_t& cid, const ghobject_t& oid, const char *name)
{
  dout(4) << __func__ << " " << cid << " " << oid << " " << name << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  return o->remove_xattr(txc, name);
}

int PMStore::_rmattrs(TXctxRef txc, const coll_t& cid, const ghobject_t& oid)
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  if (o->xattrs != 0) {
    assert(pmb_tdel(store, txc->get_tx_id(), o->xattrs) == PMB_OK);
    o->xattrs = 0;
    o->xattr_keys.clear();
  }
  return 0;
}

int PMStore::_clone(TXctxRef txc, const coll_t& cid,
    const ghobject_t& oldoid, const ghobject_t& newoid)
{
  dout(4) << __func__ << " " << cid << " " << oldoid
	   << " -> " << newoid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef oo = c->get_object(oldoid);
  if (!oo) {
    return -ENOENT;
  }
  ObjectRef no = c->get_or_create_object(newoid);

  // clone whole object:
  // 1. copy data blocks with new key
  // 2. copy OMAP/XATTRS with new key
  int result = 0;
  pmb_pair kvp = {};

  size_t offset = 0;
  int written_data = 0;
  if (no->data.size() != 0) {
    for(size_t i = oo->data.size(); i < no->data.size(); i++) {
      pmb_tdel(store, txc->get_tx_id(), no->data[i]);
    }
  }
  for (size_t i = 0; i < oo->data.size(); i++) {
    if (oo->data[i] != 0) {
      // copy block
      pmb_get(store, oo->data[i], &kvp);
      result = no->write(txc, cid, newoid, data_blocksize, kvp.val, offset, kvp.val_len);
      if (result < 0) {
        c->object_map.erase(newoid);
        c->object_hash.erase(newoid);
        dout(0) << __func__ << " error copying data: " << result << dendl;
        return result;
      } else {
        written_data += result;
      }
    }
    offset += data_blocksize;
  }

  used_bytes += written_data;

  if (oo->xattrs != 0) {
    pmb_get(store, oo->xattrs, &kvp);

    result = no->write_xattr(txc, cid, newoid, kvp.val, kvp.val_len);

    if (result < 0) {
      dout(0) << __func__ << " error copying xattrs: " << result << dendl;
      return result;
    }
  }
  dout(4) << __func__ << " return: " << result << dendl;
  return result;
}

int PMStore::_clone_range(TXctxRef txc, const coll_t& cid,
                          const ghobject_t& oldoid, const ghobject_t& newoid,
                          uint64_t srcoff, uint64_t len, uint64_t dstoff)
{
  dout(4) << __func__ << " " << cid << " "
	   << oldoid << " " << srcoff << "~" << len << " -> "
	   << newoid << " " << dstoff << "~" << len
	   << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef oo = c->get_object(oldoid);
  if (!oo) {
    return -ENOENT;
  }

  ObjectRef no = c->get_or_create_object(newoid);

  if (srcoff >= oo->data_len) {
    return 0;
  }

  if (srcoff + len >= oo->data_len) {
    len = oo->data_len - srcoff;
  }

  /*
   * We have to map srcoff + len to range og blocks + offset in the first block,
   * same goes for dstoff. If srcoff != dstoff we need to take care of reminder.
   */
  size_t start, end, block_offset;
  start = srcoff / data_blocksize;
  end = (srcoff + len - 1) / data_blocksize;
  block_offset = start * data_blocksize;

  // clone whole object:
  // 1. copy data blocks with new key
  // 2. copy OMAP/XATTRS with new key
  int result;
  int written_data = 0;
  pmb_pair kvp = {};

  // first block: write from offset
  // middle blocks: write whole
  // last block: write val_len <= data_blocksize
  for (size_t i = start; i <= end; i++) {
    if (oo->data[i] != 0) {
      // copy block
      pmb_get(store, oo->data[i], &kvp);

      if (i > start && i < end) {
        // we'll copy only part of block val_len <= data_blocksize,
        // but this will count as copy of whole block
        len -= data_blocksize;
      } else if (i == start) {
        // first block
        kvp.val += (srcoff - block_offset);
        kvp.val_len = data_blocksize - (srcoff - block_offset) < len ?
          data_blocksize - (srcoff - block_offset) : len;
        len -= kvp.val_len;
      } else if (i == end) {
        // last block
        kvp.val_len = len;
        len -= kvp.val_len;
      }

      result = no->write(txc, cid, newoid, data_blocksize, kvp.val, dstoff, kvp.val_len);

      if (result < 0) {
        c->object_map.erase(newoid);
        c->object_hash.erase(newoid);
        return result;
      } else {
        written_data += result;
      }

      dstoff += data_blocksize;
    } else {
      len -= data_blocksize;
      dstoff += data_blocksize;
    }
  }

  used_bytes += written_data;

  return 0;
}

void PMStore::_do_omap_clear(TXctxRef txc, uint64_t id)
{
  KeyValueDB::Iterator it = db->get_iterator(PREFIX_OMAP);
  string prefix, tail;
  get_omap_header(id, &prefix);
  get_omap_tail(id, &tail);
  it->lower_bound(prefix);
  while (it->valid()) {
    if (it->key() >= tail) {
      dout(30) << __func__ << "  stop at " << tail << dendl;
      break;
    }
    txc->t->rmkey(PREFIX_OMAP, it->key());
    it->next();
  }
}

int PMStore::_omap_clear(TXctxRef txc, const coll_t& cid, const ghobject_t &oid)
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  if (o->id) {
    _do_omap_clear(txc, o->id);
  }

  return 0;
}

int PMStore::_omap_setkeys(TXctxRef txc, const coll_t& cid, const ghobject_t &oid,
                           bufferlist &aset)
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;
  int r = 0;
  bufferlist::iterator p = aset.begin();
  __u32 num;

  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o || o->id == 0) {
    return -ENOENT;
  }

  ::decode(num, p);
  while (num--) {
    string key;
    bufferlist value;
    ::decode(key, p);
    ::decode(value, p);
    string final_key;
    get_omap_key(o->id, key, &final_key);
    dout(30) << __func__ << "  " << pretty_binary_string(final_key)
             << " <- " << key << dendl;
    txc->t->set(PREFIX_OMAP, final_key, value);
  }
  r = 0;

  return r;
}

int PMStore::_omap_rmkeys(TXctxRef txc, const coll_t& cid, const ghobject_t &oid, bufferlist &keys)
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;
  int r = 0;
  bufferlist::iterator p = keys.begin();
  __u32 num;

  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o || o->id == 0) {
    return -ENOENT;
  }

  ::decode(num, p);
  while (num--) {
    string key;
    ::decode(key, p);
    string final_key;
    get_omap_key(o->id, key, &final_key);
    dout(30) << __func__ << "  rm " << pretty_binary_string(final_key)
             << " <- " << key << dendl;
    txc->t->rmkey(PREFIX_OMAP, final_key);
  }
  r = 0;

  return r;
}

int PMStore::_omap_rmkeyrange(TXctxRef txc, const coll_t& cid, const ghobject_t &oid,
			       const string& first, const string& last)
{
  dout(4) << __func__ << " " << cid << " " << oid << " " << first
	   << " " << last << dendl;
  int r = 0;
  KeyValueDB::Iterator it;
  string key_first, key_last;

  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o || o->id == 0) {
    return -ENOENT;
  }

  it = db->get_iterator(PREFIX_OMAP);
  get_omap_key(o->id, first, &key_first);
  get_omap_key(o->id, last, &key_last);
  it->lower_bound(key_first);
  while (it->valid()) {
    if (it->key() >= key_last) {
      dout(30) << __func__ << "  stop at " << pretty_binary_string(key_last)
               << dendl;
      break;
    }
    txc->t->rmkey(PREFIX_OMAP, it->key());
    dout(30) << __func__ << "  rm " << pretty_binary_string(it->key()) << dendl;
    it->next();
  }
  r = 0;

  return r;
}

int PMStore::_omap_setheader(TXctxRef txc, const coll_t& cid, const ghobject_t &oid,
			      bufferlist &bl)
{
  dout(4) << __func__ << " " << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o || o->id == 0) {
    return -ENOENT;
  }

  string key;
  get_omap_header(o->id, &key);
  txc->t->set(PREFIX_OMAP, key, bl);
  return 0;
}

int PMStore::_create_collection(TXctxRef txc, const coll_t& cid)
{
  dout(4) << __func__ << " " << cid << dendl;

  RWLock::WLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp != coll_map.end()) {
    return -EEXIST;
  }
  coll_map[cid].reset(new Collection);

  // TODO: add entry to the metad store
  return 0;
}

int PMStore::_destroy_collection(TXctxRef txc, const coll_t& cid)
{
  dout(4) << __func__ << " " << cid << dendl;

  RWLock::WLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end()) {
    return -ENOENT;
  }
  {
    RWLock::RLocker l2(cp->second->lock);
    if (!cp->second->object_map.empty()) {
      return -ENOTEMPTY;
    } else {
      for (map<ghobject_t,ObjectRef>::iterator p = cp->second->object_map.begin();
         p != cp->second->object_map.end();
         ++p) {
        ObjectRef o = p->second;

        for(size_t i = 0; i < o->data.size(); i++) {
          if (o->data[i] != 0) {
            if (pmb_tdel(store, txc->get_tx_id(), o->data[i]) != PMB_OK) {
              dout(4) << __func__ << " ERROR: cannot destroy block: " << o->data[i] <<
                " from object: " << stringify(p->first) << dendl;
            }
          }
        }

        // remove associated xattrs and omap
        if (o->id != 0) {
          _do_omap_clear(txc, o->id);
        }

        if (o->xattrs != 0) {
          assert(pmb_tdel(store, txc->get_tx_id(), o->xattrs) == PMB_OK);
          o->xattrs = 0;
          o->xattr_keys.clear();
        }
      }
    }
  }
  used_bytes -= cp->second->used_bytes();
  coll_map.erase(cp);
  return 0;
}

int PMStore::_collection_add(TXctxRef txc, const coll_t& cid, const coll_t& ocid, const ghobject_t& oid)
{
  dout(4) << __func__ << " " << cid << " " << ocid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }
  CollectionRef oc = get_collection(ocid);
  if (!oc) {
    return -ENOENT;
  }
  RWLock::WLocker l1(MIN(&(*c), &(*oc))->lock);
  RWLock::WLocker l2(MAX(&(*c), &(*oc))->lock);

  if (c->object_hash.count(oid)) {
    return -EEXIST;
  }
  if (oc->object_hash.count(oid) == 0) {
    return -ENOENT;
  }
  ObjectRef o = oc->object_hash[oid];
  c->object_map[oid] = o;
  c->object_hash[oid] = o;
  return 0;
}

int PMStore::_collection_move_rename(TXctxRef txc, const coll_t& oldcid,
                                     const ghobject_t& oldoid, const coll_t& cid, const ghobject_t& oid)
{
  dout(4) << __func__ << " " << oldcid << " " << oldoid << " -> "
	   << cid << " " << oid << dendl;

  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }
  CollectionRef oc = get_collection(oldcid);
  if (!oc) {
    return -ENOENT;
  }

  // note: c and oc may be the same
  if (&(*c) == &(*oc)) {
    c->lock.get_write();
  } else if (&(*c) < &(*oc)) {
    c->lock.get_write();
    oc->lock.get_write();
  } else if (&(*c) > &(*oc)) {
    oc->lock.get_write();
    c->lock.get_write();
  }

  int r = -EEXIST;
  if (c->object_hash.count(oid)) {
    goto out;
  }
  r = -ENOENT;
  if (oc->object_hash.count(oldoid) == 0) {
    goto out;
  }
  {
    ObjectRef o = oc->object_hash[oldoid];
    r = o->change_key(txc, cid, oid);
    if (r == 0) {
      c->object_map[oid] = o;
      c->object_hash[oid] = o;
      oc->object_map.erase(oldoid);
      oc->object_hash.erase(oldoid);
    } else {
      goto out;
    }
  }
  r = 0;
 out:
  c->lock.put_write();
  if (&(*c) != &(*oc)) {
    oc->lock.put_write();
  }
  return r;
}

int PMStore::_split_collection(TXctxRef txc, const coll_t& cid, uint32_t bits,
                               uint32_t match, coll_t dest)
{
  dout(4) << __func__ << " " << cid << " " << bits << " " << match << " "
	   << dest << dendl;

  CollectionRef sc = get_collection(cid);
  if (!sc) {
    return -ENOENT;
  }
  CollectionRef dc = get_collection(dest);
  if (!dc) {
    return -ENOENT;
  }
  RWLock::WLocker l1(MIN(&(*sc), &(*dc))->lock);
  RWLock::WLocker l2(MAX(&(*sc), &(*dc))->lock);

  map<ghobject_t,ObjectRef>::iterator p = sc->object_map.begin();
  while (p != sc->object_map.end()) {
    if (p->first.match(bits, match)) {
      dout(5) << " moving " << p->first << dendl;
      p->second->change_key(txc, dest, p->first);
      dc->object_map.insert(make_pair(p->first, p->second));
      dc->object_hash.insert(make_pair(p->first, p->second));
      sc->object_hash.erase(p->first);
      sc->object_map.erase(p++);
    } else {
      ++p;
    }
  }
  return 0;
}

PMStore::OmapIteratorImpl::OmapIteratorImpl(
    CollectionRef c, ObjectRef o, KeyValueDB::Iterator it)
    : c(c), o(o), it(it)
{
  RWLock::RLocker l(c->lock);
  if (o->id) {
    get_omap_key(o->id, string(), &head);
    get_omap_tail(o->id, &tail);
    it->lower_bound(head);
  }
}

int PMStore::OmapIteratorImpl::seek_to_first()
{
  RWLock::RLocker l(c->lock);
  if (o->id) {
    it->lower_bound(head);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

int PMStore::OmapIteratorImpl::upper_bound(const string& after)
{
  RWLock::RLocker l(c->lock);
  if (o->id) {
    string key;
    get_omap_key(o->id, after, &key);
    it->upper_bound(key);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

int PMStore::OmapIteratorImpl::lower_bound(const string& to)
{
  RWLock::RLocker l(c->lock);
  if (o->id) {
    string key;
    get_omap_key(o->id, to, &key);
    it->lower_bound(key);
  } else {
    it = KeyValueDB::Iterator();
  }
  return 0;
}

bool PMStore::OmapIteratorImpl::valid()
{
  RWLock::RLocker l(c->lock);
  if (o->id && it->valid() && it->raw_key().second <= tail) {
    return true;
  } else {
    return false;
  }
}

int PMStore::OmapIteratorImpl::next(bool validate)
{
  RWLock::RLocker l(c->lock);
  if (o->id) {
    it->next();
    return 0;
  } else {
    return -1;
  }
}

string PMStore::OmapIteratorImpl::key()
{
  RWLock::RLocker l(c->lock);
  assert(it->valid());
  string db_key = it->raw_key().second;
  string user_key;
  decode_omap_key(db_key, &user_key);
  return user_key;
}

bufferlist PMStore::OmapIteratorImpl::value()
{
  RWLock::RLocker l(c->lock);
  assert(it->valid());
  return it->value();
}