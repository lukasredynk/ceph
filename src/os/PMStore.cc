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

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "pmstore::object "

// for comparing collections for lock ordering
bool operator>(const PMStore::CollectionRef& l,
	       const PMStore::CollectionRef& r)
{
  return (unsigned long)l.get() > (unsigned long)r.get();
}

int PMStore::Object::write(pmb_handle* store, coll_t& cid, const ghobject_t& oid,
    const uint64_t tx_id, const uint32_t data_blocksize, void* buf, size_t offset, size_t len) {

  size_t old_size = used_blocks;
  size_t new_size = old_size;
  size_t start, end;
  start = offset / data_blocksize;
  if (offset + len > 0)
    end = (offset + len - 1) / data_blocksize;
  else
    end = 0;

  if (data.size() <= end) {
    for(size_t i = data.size(); i <= end; i++)
      data.push_back(0);
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
    kvp.obj_id = data[i];

    // we're adding new block
    if (kvp.obj_id == 0)
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
    dout(20) << __func__ << " BEFORE: oid: " << kvp.obj_id << " offset: " << kvp.offset << " len: " << kvp.val_len << dendl;

    result = pmb_tput(store, tx_id, &kvp);

    dout(20) << __func__ << " AFTER: oid: " << kvp.obj_id << dendl;

    if (result != PMB_OK) {
      dout(20) << __func__ << " result: " << result << dendl;
      if (result == PMB_ENOSPC)
        return -ENOSPC;
      else
        return -EIO;
    } else {
      data[i] = kvp.obj_id;
    }
    remining -= kvp.val_len;
    if (remining > 0)
      buf += kvp.val_len;
  }

  if(data_len < (offset + len))
    data_len = offset + len;

  used_blocks = new_size;

  return ((new_size - old_size) * data_blocksize);
}

int PMStore::Object::touch(pmb_handle* store, coll_t& cid, const ghobject_t& oid,
    const uint64_t tx_id)
{
  int result = 0;
  if (used_blocks == 0) {
    pmb_pair kvp = {};
    bufferlist key;
    encode_key(key, cid, oid, 0);
    kvp.key = key.c_str();
    kvp.key_len = key.length();
    result = pmb_tput(store, tx_id, &kvp);
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
int PMStore::Object::truncate(pmb_handle* store, const coll_t& cid,
    const ghobject_t& oid, const uint64_t tx_id, const uint64_t data_blocksize, size_t len,
    int* diff)
{
  uint64_t requested_block = len / data_blocksize;
  if (len % data_blocksize) {
    ++requested_block;
  } else if (len == 0) {
    requested_block = 1;
  }

  uint8_t result;
  dout(20) << __func__ << " len: " << len << " req_block " << requested_block << " data.size()" << data.size() << dendl;
  if (data.size() < requested_block) {
    // add blocks to the object
    bufferlist write_key;
    pmb_pair kvp = {};

    for(size_t i = data.size(); i < requested_block; i++) {
      kvp.obj_id = 0;
      //write_key = get_part_key(i);
      encode_key(write_key, cid, oid, i);
      kvp.key = (void *) write_key.c_str();
      kvp.key_len = write_key.length();
      result = pmb_tput(store, tx_id, &kvp);
      if (result != PMB_OK) {
        return -EIO;
      }
      data.push_back(kvp.obj_id);
      ++used_blocks;
    }
  } else if (data.size() > requested_block) {
    // remove blocks from the end
    for(uint64_t i = data.size(); i > requested_block; --i) {
      dout(0) << __func__ << " truncating: " << data[i] << dendl;
      result = pmb_tdel(store, tx_id, data[i]);
      if (result != PMB_OK) {
        return -EIO;
      }
      data[i] = 0;
      --used_blocks;
    }
  }

  if (len % data_blocksize != 0) {
    // truncate most likely is within boundary of single block, zero it
    pmb_pair kvp = {};
    pmb_get(store, data[len / data_blocksize], &kvp);

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
    pmb_tdel(store, tx_id, kvp.obj_id);
    kvp.obj_id = 0;
    pmb_tput(store, tx_id, &kvp);
    data[len / data_blocksize] = kvp.obj_id;
  }

  *diff = data_len - len;
  data_len = len;
  return 0;
}

int PMStore::Object::write_omap_header(pmb_handle* store, const coll_t& cid,
    const ghobject_t& oid, const uint64_t tx_id, bufferlist& omap_header_bl) {

  pmb_pair kvp = {};
  bufferlist ohkey;
  encode_key_meta(ohkey, cid, oid, 'h');
  kvp.key = (void*) ohkey.c_str();
  kvp.key_len = ohkey.length();
  assert(kvp.key_len < g_conf->pmstore_meta_max_key_len);
  kvp.val = omap_header_bl.c_str();
  kvp.val_len = omap_header_bl.length();
  kvp.obj_id = omap_header;

  uint64_t result = pmb_tput_meta(store, tx_id, &kvp);

  if (result != PMB_OK) {
    if (result == PMB_ENOSPC)
      return -ENOSPC;
    else
      return -EIO;
  }

  omap_header = kvp.obj_id;

  return 0;
}

int PMStore::Object::write_omap_header(pmb_handle* store, const coll_t& cid,
    const ghobject_t& oid, const uint64_t tx_id, void* buf, const size_t len)
{
  pmb_pair kvp = {};
  bufferlist ohkey;
  encode_key_meta(ohkey, cid, oid, 'h');
  kvp.key = ohkey.c_str();
  kvp.key_len = ohkey.length();
  kvp.val = buf;
  kvp.val_len = len;

  int result = pmb_tput_meta(store, tx_id, &kvp);

  if (result == PMB_OK) {
    omap_header = kvp.obj_id;
  }

  return result;
}


int PMStore::Object::write_xattr(pmb_handle* store, const coll_t& cid,
    const ghobject_t& oid, const uint64_t tx_id, map<string, bufferptr>& attrs) {

  pmb_pair kvp = {};

  if (xattrs != 0) {
    // populate xattrs with missing values
    assert(pmb_get(store, xattrs, &kvp) == PMB_OK);
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

  kvp.obj_id = xattrs;

  bufferlist xkey;
  encode_key_meta(xkey, cid, oid, 'x');
  kvp.key = (void *) xkey.c_str();
  kvp.key_len = xkey.length();

  // encode map in single buffer
  bufferlist to_write;
  ::encode(attrs, to_write);
  kvp.val = (void *) to_write.c_str();
  kvp.val_len = to_write.length();

  dout(0) << __func__ << " key_len: " << kvp.key_len << " val_len: " << kvp.val_len << dendl;
  assert(pmb_tput_meta(store, tx_id, &kvp) == PMB_OK);

  xattrs = kvp.obj_id;

  return 0;
}

int PMStore::Object::write_xattr(pmb_handle* store, const coll_t& cid,
    const ghobject_t& oid, const uint64_t tx_id, void* buf, const size_t len)
{
  pmb_pair kvp = {};
  bufferlist xkey;
  encode_key_meta(xkey, cid, oid, 'x');
  kvp.key = xkey.c_str();
  kvp.key_len = xkey.length();
  kvp.val = buf;
  kvp.val_len = len;

  dout(0) << __func__ << " key_len: " << kvp.key_len << " val_len: " << kvp.val_len << dendl;

  int result = pmb_tput_meta(store, tx_id, &kvp);

  if (result == PMB_OK) {
    xattrs = kvp.obj_id;
  } else {
    dout(0) << " error writing xattrs: " << result << dendl;
  }

  return result;
}

int PMStore::Object::remove_omaps(pmb_handle* store, const uint64_t tx_id, const set<string>& omap_keys) {
  if (!omaps) {
    return -ENOENT;
  }

  pmb_pair kvp {};
  int result = pmb_get(store, omaps, &kvp);
  if (result != PMB_OK) {
    dout(0) << __func__ << " ERROR removing omaps" << dendl;
    return -ENODATA;
  }

  map<string,bufferlist> aset;
  {
    bufferlist bl;
    bl.append((const char*) kvp.val, kvp.val_len);
    bufferlist::iterator bi = bl.begin();
    ::decode(aset, bi);
  }

  for (set<string>::const_iterator p = omap_keys.begin(); p != omap_keys.end(); ++p) {
    aset.erase(*p);
  }

  if (!aset.empty()) {
    bufferlist to_write;
    ::encode(aset, to_write);
    kvp.val = (void *) to_write.c_str();
    kvp.val_len = to_write.length();
    kvp.obj_id = omaps;

    result = pmb_tput_meta(store, tx_id, &kvp);
    omaps = kvp.obj_id;
    for (set<string>::const_iterator p = omap_keys.begin(); p != omap_keys.end(); ++p) {
      this->omap_keys.erase(*p);
    }
  } else {
    result = pmb_tdel(store, tx_id, omaps);
    omaps = 0;
    this->omap_keys.clear();
  }

  if (result != PMB_OK) {
    dout(0) << " error writing omaps: " << result << dendl;
  }

  return result;
}

int PMStore::Object::remove_xattr(pmb_handle* store, const uint64_t tx_id, const char* xattr_key) {
  if (!xattr_keys.count(xattr_key)) {
    return -ENOENT;
  }

  if (!xattrs) {
    return -ENOENT;
  }

  pmb_pair kvp {};
  int result = pmb_get(store, xattrs, &kvp);
  if (result != PMB_OK) {
    dout(0) << __func__ << " ERROR remove attr: " << xattr_key << " "
       << " obj_id: " << xattrs << dendl;
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
  kvp.obj_id = xattrs;

  result = pmb_tput_meta(store, tx_id, &kvp);

  if (result == PMB_OK) {
    xattrs = kvp.obj_id;

    for(auto iset = xattr_keys.begin(); iset != xattr_keys.end(); ++iset) {
      if (!iset->compare(xattr_key)) {
        xattr_keys.erase(iset);
      }
    }
  } else {
    dout(0) << " error writing xattrs: " << result << dendl;
  }

  dout(10) << __func__ << " removed attr: " << xattr_key << " len: " << kvp.val_len << dendl;
  return result;
}

int PMStore::Object::write_omap(pmb_handle* store, const coll_t& cid,
    const ghobject_t& oid, const uint64_t tx_id, map<string, bufferlist>& aset) {

  pmb_pair kvp = {};
  bufferlist to_write;
  if (omaps != 0) {
    // populate omaps with missing omap values
    assert(pmb_get(store, omaps, &kvp) == PMB_OK);
    bufferlist bl;
    bl.append((const char*) kvp.val, kvp.val_len);
    bufferlist::iterator bi = bl.begin();
    map<string,bufferlist> old_omaps;
    ::decode(old_omaps, bi);
    for(map<string,bufferlist>::iterator i = old_omaps.begin(); i != old_omaps.end(); ++i) {
      if (!aset.count(i->first)) {
        aset.insert(pair<string,bufferlist>(i->first, i->second));
      }
    }
  }

  for(map<string, bufferlist>::const_iterator i = aset.begin(); i !=aset.end(); ++i) {
    omap_keys.insert(i->first);
  }

  // encode map in single buffer
  ::encode(aset, to_write);

  bufferlist okey;
  encode_key_meta(okey, cid, oid, 'o');
  kvp.obj_id = omaps;
  kvp.key = (void *) okey.c_str();
  kvp.key_len = okey.length();
  kvp.val = (void *) to_write.c_str();
  kvp.val_len = to_write.length();

  int result = pmb_tput_meta(store, tx_id, &kvp);

  if (result != PMB_OK) {
    dout(0) << __func__ << " failed to write omaps! error: " << result << \
      " key len: " << kvp.key_len << " val len: " << kvp.val_len  << dendl;
  }

  omaps = kvp.obj_id;

  return result;
}

int PMStore::Object::write_omap(pmb_handle* store, const coll_t& cid,
    const ghobject_t& oid, const uint64_t tx_id, void* buf, const size_t len)
{
  pmb_pair kvp = {};
  bufferlist okey;
  encode_key_meta(okey, cid, oid, 'o');
  kvp.key = okey.c_str();
  kvp.key_len = okey.length();
  kvp.val = buf;
  kvp.val_len = len;

  int result = pmb_tput_meta(store, tx_id, &kvp);

  if (result == PMB_OK) {
    omaps = kvp.obj_id;
  }

  return result;
}


/*
 * Changes cid and oid for an object
 */
int PMStore::Object::change_key(pmb_handle* store, const coll_t& newcid,
    const ghobject_t& newoid, const uint64_t tx_id) {

  pmb_pair kvp = {};

  int result = 0;

  // rewrite blocks
  for(size_t i = 0; i < data.size(); i++) {
    if (data[i] == 0)
      continue;

    pmb_get(store, data[i], &kvp);
    bufferlist write_key;
    encode_key(write_key, newcid, newoid, i);
    kvp.key = (void*) write_key.c_str();
    kvp.key_len = write_key.length();

    result = pmb_tput(store, tx_id, &kvp);

    if (result != PMB_OK) {
      if (result == PMB_ENOSPC)
        return -ENOSPC;
      else
        return -EIO;
    } else {
      data[i] = kvp.obj_id;
    }
  }

  // rewrite omap_header
  if (omap_header != 0) {
    pmb_get(store, omap_header, &kvp);
    bufferlist bl;
    bl.append((const char*) kvp.val, kvp.val_len);
    result = write_omap_header(store, newcid, newoid, tx_id, bl);
  }

  return result;
}

#undef dout_prefix
#define dout_prefix *_dout << "pmstore(" << path << ") "

int PMStore::peek_journal_fsid(uuid_d *fsid)
{
  *fsid = uuid_d();
  return 0;
}

int PMStore::mount()
{
  dout(10) << __func__ << dendl;
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
  dout(10) << __func__ << dendl;

  //op_tp.stop();
  finisher.stop();
  int retval= _save();
  return retval;
}

int PMStore::_save()
{
  dout(10) << __func__ << dendl;
  Mutex::Locker l(apply_lock); // block any writer
  dump_all();
  set<coll_t> collections;
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    dout(20) << __func__ << " coll " << p->first << " " << p->second << dendl;
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

  return 0;
}

void PMStore::dump_all()
{
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
      if (q->second)
	q->second->dump(f);
      f->close_section();
    }
    f->close_section();

    f->close_section();
  }
  f->close_section();
}

int PMStore::_load()
{
  dout(10) << __func__ << dendl;

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
  if (::stat(fn.c_str(), &st) == 0)
    set_allow_sharded_objects();

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
  dout(10) << __func__ << " open PMStore data.pool at: " << opts.path << dendl;
  store = pmb_open(&opts, &error);

  if (store == NULL) {
    dout(0) << __func__ << " ERROR cannot open data.pool: " << error << dendl;
    return -1;
  }


  pmb_iter *iter = pmb_iter_open(store, PMB_DATA);
  pmb_pair kvp;
  while(pmb_iter_valid(iter)) {
    dout(10) << __func__ << " current iter @ obj_id: " << pmb_iter_pos(iter) << dendl;
    r = pmb_iter_get(iter, &kvp);
    if (r != PMB_OK) {
      dout(10) << __func__ << " ERROR processing iterator!" << dendl;
    } else if (kvp.key_len != 0) {
      _add_object(&kvp);
    } else {
      dout(0) << __func__ << " ERROR iterator returned empty object: " << kvp.obj_id << dendl;
    }
    pmb_iter_next(iter);
  }
  pmb_iter_close(iter);

  iter = pmb_iter_open(store, PMB_META);
  while(pmb_iter_valid(iter)) {
    dout(10) << __func__ << " current iter @ obj_id: " << pmb_iter_pos(iter) << dendl;
    r = pmb_iter_get(iter, &kvp);
    if (r != PMB_OK) {
      dout(0) << __func__ << " ERROR processing iterator!" << dendl;
    } else if (kvp.key_len != 0) {
      _add_meta_object(&kvp);
    } else {
      dout(0) << __func__ << " ERROR iterator returned empty object: " << kvp.obj_id << dendl;
    }
    pmb_iter_next(iter);
  }
  pmb_iter_close(iter);

  dout(0) << __func__ << " finished" << dendl;


  return 0;
}

void PMStore::_add_object(pmb_pair *kvp)
{
  dout(10) << __func__ << dendl;

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
    dout(10) << __func__ << " ERROR collection not found!" << dendl;

    return;
  }

  dout(10) << __func__ << " before getting ObjectRef:" << oid << dendl;
  ObjectRef o = c->get_or_create_object(oid);

  dout(10) << __func__ << " adding part: " << part << " obj_id: " << kvp->obj_id
    << " block size: " << data_blocksize << dendl;
  o->add(part, kvp->obj_id);
  if((part + 1) * data_blocksize > o->data_len) {
    o->data_len = part * data_blocksize + kvp->val_len;
  }
  dout(10) << __func__ << " object data_len: " << o->data_len << dendl;
}

/*
 * Format: type$...
 * Type: x
 * Type: h
 */
void PMStore::_add_meta_object(pmb_pair *kvp)
{
  dout(10) << __func__ << dendl;

  bufferlist bl;
  bl.append((const char*) kvp->key, kvp->key_len);
  coll_t cid;
  ghobject_t oid;
  char type;

  //dout(10) << __func__ << " key: " << key << " key_len: " << kvp->key_len <<
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
    dout(10) << __func__ << " ERROR collection not found!" << dendl;

    return;
  }

  ObjectRef o = c->get_object(oid);
  if(!o) {
    dout(10) << __func__ << " ERROR object not found!" << dendl;

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
        o->xattrs = kvp->obj_id;
        for(pair<string, bufferptr> i : obj_xattrs) {
          o->xattr_keys.insert(i.first);
        }
        break;
      }
    case 'o':
      {
        bufferlist obl;
        obl.append((const char *)kvp->val, kvp->val_len);
        bufferlist::iterator obi = obl.begin();
        map<string, bufferlist> obj_omaps;
        DECODE_START(1, obi);
        ::decode(obj_omaps, obi);
        DECODE_FINISH(obi);
        o->omaps = kvp->obj_id;
        for(pair<string, bufferlist> i : obj_omaps) {
          o->omap_keys.insert(i.first);
        }
        break;
      }
    case 'h':
      o->omap_header = kvp->obj_id;
      break;
    default:
      assert(0 == "invalid meta type in store");
  }
}

void PMStore::set_fsid(uuid_d u)
{
  int r = write_meta("fs_fsid", stringify(u));
  assert(r >= 0);
}

uuid_d PMStore::get_fsid()
{
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
  return 0;
}

int PMStore::statfs(struct statfs *st)
{
  dout(10) << __func__ << dendl;
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

PMStore::CollectionRef PMStore::get_collection(coll_t cid)
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

bool PMStore::exists(coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
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
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
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
    coll_t cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags,
    bool allow_eio)
{
  dout(10) << __func__ << " " << cid << " " << oid << " "
	   << offset << "~" << len << dendl;
  CollectionRef c = get_collection(cid);
  if (!c){
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  dout(10) << __func__ << " oid: " << oid << " object data_len: " << o->data_len << dendl;
  if (offset >= o->data_len) {
    return 0;
  }
  size_t l = len;
  if (l == 0)  // note: len == 0 means read the entire object
    l = o->data_len;
  else if (offset + l > o->data_len)
    l = o->data_len - offset;
  bl.clear();

  uint64_t start, end, block_offset, remining;
  start = offset / data_blocksize;
  end = (offset + l) / data_blocksize;
  block_offset = start * data_blocksize;
  remining = l;

  dout(0) << __func__ << " " << oid << " data_len: " << o->data_len << " offset: " << offset <<
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
        dout(10) << __func__ << " allocated, but empty block, appending " << kvp.val_len << " zeros" << dendl;
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
      dout(10) << __func__ << " block " << start << " don't exist, appending " << kvp.val_len << " zeros" << dendl;
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
int PMStore::fiemap(coll_t cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, bufferlist& bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << offset << "~"
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
  if (offset + l > o->data_len)
    l = o->data.size() * data_blocksize - offset;
  m[offset] = l;
  ::encode(m, bl);
  return 0;
}

int PMStore::getattr(coll_t cid, const ghobject_t& oid,
		      const char *name, bufferptr& value)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << name << dendl;
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
      stringify(oid) << " " << k << " obj_id: " << o->xattrs << dendl;
    return -ENODATA;
  }

  bufferlist bl;
  bl.append((const char*) kvp.val, kvp.val_len);
  bufferlist::iterator bi = bl.begin();
  map<string,bufferptr> aset;
  ::decode(aset, bi);

  assert(aset.count(k) == 1);
  value = aset[k];

  dout(10) << __func__ << " attr: " << name << " len: " << kvp.val_len << dendl;
  return 0;
}

int PMStore::getattrs(coll_t cid, const ghobject_t& oid,
		       map<string,bufferptr>& aset)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
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
      stringify(oid) << " " << " obj_id: " << o->xattrs << dendl;
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
  dout(10) << __func__ << dendl;
  RWLock::RLocker l(coll_lock);
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    ls.push_back(p->first);
  }
  return 0;
}

bool PMStore::collection_exists(coll_t cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  RWLock::RLocker l(coll_lock);
  bool ret = coll_map.count(cid);
  return ret;
}

bool PMStore::collection_empty(coll_t cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }
  RWLock::RLocker l(c->lock);
  bool ret = c->object_map.empty();
  return ret;
}

int PMStore::collection_list(coll_t cid, ghobject_t start, ghobject_t end,
                              bool sort_bitwise, int max,
                              vector<ghobject_t> *ls, ghobject_t *next)
{
  if (!sort_bitwise)
    return -EOPNOTSUPP;
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
    if (p == c->object_map.end())
      *next = ghobject_t::get_max();
    else
      *next = p->first;
  }

  return 0;
}

int PMStore::omap_get(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  pmb_pair kvp;
  dout(10) << __func__ << " get omap header: " << o->omap_header << dendl;

  // get header
  if (header != NULL) {
    header->clear();
  } else {
    header = new bufferlist();
  }
  if (o->omap_header != 0) {
    uint8_t result = pmb_get(store, o->omap_header, &kvp);
    assert(result == PMB_OK);
    header->append((const char*) kvp.val, kvp.val_len);
  }

  // get omap key and values
  if (out != NULL) {
    out->clear();
  } else {
    out = new map<string,bufferlist>();
  }
  if (o->omaps != 0) {
    assert(pmb_get(store, o->omaps, &kvp) == 0);
    bufferlist bl;
    bl.append((const char*) kvp.val, kvp.val_len);
    bufferlist::iterator p = bl.begin();
    ::decode(*out, p);
  }
  return 0;
}

int PMStore::omap_get_header(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio) ///< [in] don't assert on eio
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  if (o->omap_header == 0) {
    *header = bufferlist();
  } else {
    pmb_pair kvp;
    dout(10) << __func__ << " get omap header: " << o->omap_header << dendl;
    assert(pmb_get(store, o->omap_header, &kvp) == PMB_OK);
    if (header != NULL) {
      header->clear();
    } else {
      header = new bufferlist();
    }
    header->append((const char *) kvp.val, kvp.val_len);
  }
  return 0;
}

int PMStore::omap_get_keys(
    coll_t cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  for (set<string>::iterator p = o->omap_keys.begin();
       p != o->omap_keys.end(); ++p)
    keys->insert(*p);

  return 0;
}

int PMStore::omap_get_values(
    coll_t cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  if (o->omaps != 0) {
    pmb_pair kvp;
    assert(pmb_get(store, o->omaps, &kvp) == PMB_OK);
    bufferlist bl;
    bl.append((const char*) kvp.val, kvp.val_len);
    bufferlist::iterator p = bl.begin();
    ::decode(*out, p);
  }
  return 0;
}

int PMStore::omap_check_keys(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  for (set<string>::const_iterator p = keys.begin();
       p != keys.end();
       ++p) {
    set<string>::iterator q = o->omap_keys.find(*p);
    if (q != o->omap_keys.end())
      out->insert(*p);
  }
  return 0;
}

ObjectMap::ObjectMapIterator PMStore::get_omap_iterator(coll_t cid,
							 const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return ObjectMap::ObjectMapIterator();

  ObjectRef o = c->get_object(oid);
  if (!o)
    return ObjectMap::ObjectMapIterator();
  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(store, c, o));
}


// ---------------
// write operations

int PMStore::queue_transactions(Sequencer *osr,
				 list<Transaction*>& tls,
				 TrackedOpRef op,
				 ThreadPool::TPHandle *handle)
{

  // because memstore operations are synchronous, we can implement the
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

  uint64_t tx_id;
  pmb_tx_begin(store, &tx_id);

  // loop for tx slot = 0
  if (tx_id == 0) {
    int i = 5;
    while (tx_id == 0 && i > 0) {
      dout(0) << __func__ << " waiting for tx_id" << dendl;
      pmb_tx_begin(store, &tx_id);
      i--;
    }
  }

  assert(tx_id != 0);

  for (list<Transaction*>::iterator p = tls.begin(); p != tls.end(); ++p) {
    // poke the TPHandle heartbeat just to exercise that code path
    if (handle)
      handle->reset_tp_timeout();

    if(_do_transaction(**p, tx_id) < 0) {
      pmb_tx_abort(store, tx_id);
      return -EIO; // check potential error codes
    }
  }
  pmb_tx_commit(store, tx_id);
  pmb_tx_execute(store, tx_id);

  Context *on_apply = NULL, *on_apply_sync = NULL, *on_commit = NULL;
  ObjectStore::Transaction::collect_contexts(tls, &on_apply, &on_commit,
					     &on_apply_sync);
  if (on_apply_sync)
    on_apply_sync->complete(0);
  if (on_apply)
    finisher.queue(on_apply);
  if (on_commit)
    finisher.queue(on_commit);
  return 0;
}

int PMStore::_do_transaction(Transaction& t, const uint64_t tx_id)
{
  dout(10) << __func__ << dendl;
  Transaction::iterator i = t.begin();
  int pos = 0;

//#ifdef WITH_LTTNG
//  const char *osr_name =  "<NULL>";
//#endif

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
        r = _touch(tx_id, cid, oid);
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
        r = _write(tx_id, cid, oid, off, len, bl, fadvise_flags);
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
        r = _zero(tx_id, cid, oid, off, len);
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
        r = _truncate(tx_id, cid, oid, off);
	      tracepoint(objectstore, truncate_exit, r);
      }
      break;

    case Transaction::OP_REMOVE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	      tracepoint(objectstore, remove_enter, osr_name);
        r = _remove(tx_id, cid, oid);
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
        r = _setattrs(tx_id, cid, oid, to_set);
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
        r = _setattrs(tx_id, cid, oid, aset);
	      tracepoint(objectstore, setattrs_exit, r);
      }
      break;

    case Transaction::OP_RMATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        string name = i.decode_string();
	      tracepoint(objectstore, rmattr_enter, osr_name);
        r = _rmattr(tx_id, cid, oid, name.c_str());
	      tracepoint(objectstore, rmattr_exit, r);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	      tracepoint(objectstore, rmattrs_enter, osr_name);
        r = _rmattrs(tx_id, cid, oid);
	      tracepoint(objectstore, rmattrs_exit, r);
      }
      break;

    case Transaction::OP_CLONE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
	      tracepoint(objectstore, clone_enter, osr_name);
        r = _clone(tx_id, cid, oid, noid);
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
        r = _clone_range(tx_id, cid, oid, noid, off, len, off);
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
        r = _clone_range(tx_id, cid, oid, noid, srcoff ,len, dstoff);
	      tracepoint(objectstore, clone_range2_exit, r);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
	      tracepoint(objectstore, mkcoll_enter, osr_name);
        r = _create_collection(tx_id, cid);
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
          dout(10) << "Unrecognized collection hint type: " << type << dendl;
        }
      }
      break;

    case Transaction::OP_RMCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
	      tracepoint(objectstore, rmcoll_enter, osr_name);
        r = _destroy_collection(tx_id, cid);
        tracepoint(objectstore, rmcoll_exit, r);
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
        assert(0 == "not supported");
        coll_t ocid = i.get_cid(op->cid);
        coll_t ncid = i.get_cid(op->dest_cid);
        ghobject_t oid = i.get_oid(op->oid);
	      tracepoint(objectstore, coll_add_enter);
        r = _collection_add(tx_id, ncid, ocid, oid);
	      tracepoint(objectstore, coll_add_exit, r);
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        r = _remove(tx_id, cid, oid);
       }
      break;

    case Transaction::OP_COLL_MOVE:
      {
        assert(0 == "deprecated");
      }
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
        coll_t oldcid = i.get_cid(op->cid);
        ghobject_t oldoid = i.get_oid(op->oid);
        coll_t newcid = i.get_cid(op->dest_cid);
        ghobject_t newoid = i.get_oid(op->dest_oid);
	      tracepoint(objectstore, coll_move_rename_enter);
        r = _collection_move_rename(tx_id, oldcid, oldoid, newcid, newoid);
	      tracepoint(objectstore, coll_move_rename_exit, r);
      }
      break;

    case Transaction::OP_COLL_SETATTR:
    case Transaction::OP_COLL_RMATTR:
      assert(0 == "coll attributes no longer supported");
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
        r = _omap_clear(tx_id, cid, oid);
	      tracepoint(objectstore, omap_clear_exit, r);
      }
      break;

    case Transaction::OP_OMAP_SETKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        map<string, bufferlist> aset;
        i.decode_attrset(aset);
	      tracepoint(objectstore, omap_setkeys_enter, osr_name);
        r = _omap_setkeys(tx_id, cid, oid, aset);
	      tracepoint(objectstore, omap_setkeys_exit, r);
      }
      break;

    case Transaction::OP_OMAP_RMKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        set<string> keys;
        i.decode_keyset(keys);
	      tracepoint(objectstore, omap_rmkeys_enter, osr_name);
        r = _omap_rmkeys(tx_id, cid, oid, keys);
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
        r = _omap_rmkeyrange(tx_id, cid, oid, first, last);
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
        r = _omap_setheader(tx_id, cid, oid, bl);
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
        r = _split_collection(tx_id, cid, bits, rem, dest);
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
			    op->op == Transaction::OP_COLL_ADD))
        // -ENOENT is usually okay
        ok = true;

      if (r == -ENODATA)
        ok = true;

      if (!ok) {
        const char *msg = "unexpected error code";

        if (r == -ENOENT && (op->op == Transaction::OP_CLONERANGE ||
                 op->op == Transaction::OP_CLONE ||
                 op->op == Transaction::OP_CLONERANGE2))
          msg = "ENOENT on clone suggests osd bug";

        if (r == -ENOSPC)
          // For now, if we hit _any_ ENOSPC, crash, before we do any damage
          // by partially applying transactions.
          msg = "ENOSPC handling not implemented";

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

int PMStore::_touch(const uint64_t tx_id, coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_or_create_object(oid);

  return o->touch(store, cid, oid, tx_id);
}

int PMStore::_write(const uint64_t tx_id, coll_t cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, bufferlist& bl,
		     uint32_t fadvise_flags)
{
  dout(0) << __func__ << " cid: " << cid << " ghobject_t: " <<
    oid << " offset: " << offset << " len: " << len << dendl;
  assert(len == bl.length());

  CollectionRef c = get_collection(cid);
  if (!c) {
    dout(10) << __func__ << " NO COLLECTION!" << dendl;
    return -ENOENT;
  }

  ObjectRef o = c->get_or_create_object(oid);

  int result = o->write(store, cid, oid, tx_id, data_blocksize, bl.c_str(), offset, len);
  dout(10) << __func__ << " cid: " << cid << " ghobject_t: " <<
           oid << " result: " << result << dendl;
  if (result >= 0) {
    used_bytes += result;
    return 0;
  } else {
    return result;
  }
}

int PMStore::_zero(const uint64_t tx_id, coll_t cid, const ghobject_t& oid,
		    uint64_t offset, size_t len)
{
  dout(0) << __func__ << " " << cid << " " << oid << " " << offset << "~"
	   << len << dendl;
  bufferlist bl;
  bl.append_zero(len);
  int result = _write(tx_id, cid, oid, offset, len, bl);
  return result;
}

int PMStore::_truncate(const uint64_t tx_id, coll_t cid, const ghobject_t& oid, uint64_t size)
{
  dout(0) << __func__ << " " << cid << " " << oid << " " << size << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  int diff = 0;
  int result = o->truncate(store, cid, oid, tx_id, data_blocksize, size, &diff);

  if (result >= 0) {
    used_bytes += diff;
    return 0;
  } else {
    return result;
  }
}

int PMStore::_remove(const uint64_t tx_id, coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
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
      assert(pmb_tdel(store, tx_id, o->data[i]) == PMB_OK);
      o->data[i] = 0;
    }
  }

  // remove associated xattrs and omap
  if (o->omap_header != 0) {
    assert(pmb_tdel(store, tx_id, o->omap_header) == PMB_OK);
    o->omap_header = 0;
  }

  if (o->xattrs != 0) {
    assert (pmb_tdel(store, tx_id, o->xattrs) == PMB_OK);
    o->xattrs = 0;
    o->xattr_keys.clear();
  }

  if (o->omaps != 0) {
    assert (pmb_tdel(store, tx_id, o->omaps) == PMB_OK);
    o->omaps = 0;
    o->omap_keys.clear();
  }

  c->object_map.erase(oid);
  c->object_hash.erase(oid);

  used_bytes -= o->used_blocks * data_blocksize;
  return 0;
}

int PMStore::_setattrs(const uint64_t tx_id, coll_t cid, const ghobject_t& oid,
			map<string,bufferptr>& aset)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  int result = o->write_xattr(store, cid, oid, tx_id, aset);

  return result;
}

int PMStore::_rmattr(const uint64_t tx_id, coll_t cid, const ghobject_t& oid, const char *name)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << name << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
    return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  int result = o->remove_xattr(store, tx_id, name);
  return result;
}

int PMStore::_rmattrs(const uint64_t tx_id, coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  if (o->xattrs != 0) {
    assert(pmb_tdel(store, tx_id, o->xattrs) == PMB_OK);
    o->xattrs = 0;
    o->xattr_keys.clear();
  }
  return 0;
}

int PMStore::_clone(const uint64_t tx_id, coll_t cid,
    const ghobject_t& oldoid, const ghobject_t& newoid)
{
  dout(0) << __func__ << " " << cid << " " << oldoid
	   << " -> " << newoid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
    dout(0) << __func__ << " no such collection" << dendl;
     return -ENOENT;
  }

  ObjectRef oo = c->get_object(oldoid);
  if (!oo) {
    dout(0) << __func__ << " no such object " << dendl;
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
      pmb_tdel(store, tx_id, no->data[i]);
    }
  }
  for (size_t i = 0; i < oo->data.size(); i++) {
    if (oo->data[i] != 0) {
      // copy block
      pmb_get(store, oo->data[i], &kvp);
      result = no->write(store, cid, newoid, tx_id, data_blocksize, kvp.val, offset, kvp.val_len);
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


  // OMAP header

  if (oo->omap_header != 0) {
    pmb_get(store, oo->omap_header, &kvp);

    result = no->write_omap_header(store, cid, newoid, tx_id, kvp.val, kvp.val_len);

    if (result < 0) {
      dout(0) << __func__ << " error copying omap header: " << result << dendl;
      return result;
    }
  }

  if (oo->omaps != 0) {
    pmb_get(store, oo->omaps, &kvp);

    result = no->write_omap(store, cid, newoid, tx_id, kvp.val, kvp.val_len);

    if (result < 0) {
      dout(0) << __func__ << " error copying omaps: " << result << dendl;
      return result;
    }
  }

  if (oo->xattrs != 0) {
    pmb_get(store, oo->xattrs, &kvp);

    result = no->write_xattr(store, cid, newoid, tx_id, kvp.val, kvp.val_len);

    if (result < 0) {
      dout(0) << __func__ << " error copying xattrs: " << result << dendl;
      return result;
    }
  }
  dout(10) << __func__ << " return: " << result << dendl;
  return result;
}

int PMStore::_clone_range(const uint64_t tx_id, coll_t cid,
                          const ghobject_t& oldoid, const ghobject_t& newoid,
                          uint64_t srcoff, uint64_t len, uint64_t dstoff)
{
  dout(0) << __func__ << " " << cid << " "
	   << oldoid << " " << srcoff << "~" << len << " -> "
	   << newoid << " " << dstoff << "~" << len
	   << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef oo = c->get_object(oldoid);
  if (!oo)
    return -ENOENT;

  ObjectRef no = c->get_or_create_object(newoid);

  if (srcoff >= oo->data_len)
    return 0;
  if (srcoff + len >= oo->data_len)
    len = oo->data_len - srcoff;

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

      result = no->write(store, cid, newoid, tx_id, data_blocksize, kvp.val, dstoff, kvp.val_len);

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

  return len;
}

int PMStore::_omap_clear(const uint64_t tx_id, coll_t cid, const ghobject_t &oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  if (o->omap_header) {
    assert(pmb_tdel(store, tx_id, o->omap_header) == PMB_OK);
    o->omap_header = 0;
  }

  if (o->omaps != 0) {
    assert(pmb_tdel(store, tx_id, o->omaps) == PMB_OK);
    o->omaps = 0;
    o->omap_keys.clear();
  }
  return 0;
}

int PMStore::_omap_setkeys(const uint64_t tx_id, coll_t cid, const ghobject_t &oid,
			    map<string, bufferlist> &aset)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  int result = o->write_omap(store, cid, oid, tx_id, aset);

  return result;
}

int PMStore::_omap_rmkeys(const uint64_t tx_id, coll_t cid, const ghobject_t &oid,
			   const set<string> &keys)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  int result = o->remove_omaps(store, tx_id, keys);

  return result;
}

int PMStore::_omap_rmkeyrange(const uint64_t tx_id, coll_t cid, const ghobject_t &oid,
			       const string& first, const string& last)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << first
	   << " " << last << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  auto p = o->omap_keys.lower_bound(first);
  auto e = o->omap_keys.lower_bound(last);
  set<string> omap_keys_to_remove;

  for(auto it = p; it != e; it++) {
    dout(0) << __func__ << " key: " << *it << dendl;
    omap_keys_to_remove.insert(*it);
  }

  int result = o->remove_omaps(store, tx_id, omap_keys_to_remove);

  return result;
}

int PMStore::_omap_setheader(const uint64_t tx_id, coll_t cid, const ghobject_t &oid,
			      bufferlist &bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c) {
     return -ENOENT;
  }

  ObjectRef o = c->get_object(oid);
  if (!o) {
    return -ENOENT;
  }

  int result = o->write_omap_header(store, cid, oid, tx_id, bl);

  if (result < 0)
    return result;

  return 0;
}

int PMStore::_create_collection(const uint64_t tx_id, coll_t cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  RWLock::WLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp != coll_map.end()) {
    return -EEXIST;
  }
  coll_map[cid].reset(new Collection);

  // TODO: add entry to the metad store
  return 0;
}

int PMStore::_destroy_collection(const uint64_t tx_id, coll_t cid)
{
  dout(10) << __func__ << " " << cid << dendl;
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
            if (pmb_tdel(store, tx_id, o->data[i]) != PMB_OK) {
              dout(10) << __func__ << " ERROR: cannot destroy block: " << o->data[i] <<
                " from object: " << stringify(p->first) << dendl;
            }
          }
        }

        // <-- destroy all OMAP/XATTR related to the object
        // remove associated xattrs and omap
        if (o->omap_header != 0)
          pmb_tdel(store, tx_id, o->omap_header);

        if (o->xattrs != 0) {
          assert(pmb_tdel(store, tx_id, o->xattrs) == PMB_OK);
          o->xattrs = 0;
          o->xattr_keys.clear();
        }

        if (o->omaps != 0) {
          assert(pmb_tdel(store, tx_id, o->omaps) == PMB_OK);
          o->omaps = 0;
          o->omap_keys.clear();
        }
      }
    }
  }
  used_bytes -= cp->second->used_bytes();
  coll_map.erase(cp);
  return 0;
}

int PMStore::_collection_add(const uint64_t tx_id, coll_t cid, coll_t ocid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << ocid << " " << oid << dendl;
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

int PMStore::_collection_move_rename(const uint64_t tx_id,
                                      coll_t oldcid, const ghobject_t& oldoid,
				      coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << oldcid << " " << oldoid << " -> "
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
  if (c->object_hash.count(oid))
    goto out;
  r = -ENOENT;
  if (oc->object_hash.count(oldoid) == 0)
    goto out;
  {
    ObjectRef o = oc->object_hash[oldoid];
    r = o->change_key(store, cid, oid, tx_id);
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
  if (&(*c) != &(*oc))
    oc->lock.put_write();
  return r;
}

int PMStore::_split_collection(const uint64_t tx_id, coll_t cid, uint32_t bits,
                               uint32_t match, coll_t dest)
{
  dout(10) << __func__ << " " << cid << " " << bits << " " << match << " "
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
      dout(20) << " moving " << p->first << dendl;
      p->second->change_key(store, dest, p->first, tx_id);
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
