#include <iostream>
#include <sstream>
#include <cassert>

#include "rocksdb/db.h"
#include "rocksdb/utilities/object_registry.h"
#include "env_nvm.h"
#include "args.h"

using namespace rocksdb;

Status bulk_insert(DB* db, WriteOptions& woptions, int nkvpairs) {
  Status s;
  WriteBatch batch;

  for (int i = 0; i < nkvpairs; ++i) {
    std::string key("k{" + std::to_string(i) + "}");
    std::string val("v{" + std::to_string(i) + "}");

    batch.Put(key, val);
  }

  s = db->Write(woptions, &batch);
  assert(s.ok());

  return s;
}

Status bulk_delete(DB* db, WriteOptions& woptions, int nkvpairs) {
  Status s;

  WriteBatch batch;
  for (int i = 0; i < nkvpairs; ++i) {
    std::string key("k{" + std::to_string(i) + "}");

    batch.Delete(key);
  }

  s = db->Write(woptions, &batch);
  assert(s.ok());

  return s;
}

// Gets and verifies that all values are as expected.
// NOT: Not bulk in the sense that it uses the "batch" feature.
Status bulk_get(DB* db, ReadOptions& roptions, int nkvpairs) {
  Status s;

  for (int i = 0; i < nkvpairs; ++i) {  // GET
    std::string key("k{" + std::to_string(i) + "}");
    std::string val("v{" + std::to_string(i) + "}");
    std::string returned;

    s = db->Get(roptions, key, &returned);
    assert(s.ok());
    assert(returned.compare(val)==0);
    std::cout << "------------------ GET Return: " << returned << std::endl;

    if (!s.ok())
      break;
  }

  return s;
}

int main(int argc, char *argv[]) {

  if(argc < 4)
    exit(1);
  
  int nkvs = atoi(argv[1]);
  int punit_from = atoi(argv[2]);
  int punit_to = atoi(argv[3]);
  auto dev = std::string(argv[4]);

  std::unique_ptr<Env> env_guard;
  std::ostringstream env_str;
  // nvm://punits:${PUNIT_RANGE}@2-4%${DEV}/opt/rocks/${DEV}_yunchih.meta
  env_str << "nvm://punits:" << punit_from << "-" << punit_to << "@2-4%" << dev << "/opt/rocks/nvm.meta";
  Env *env = NewCustomObject<Env>(env_str.str(), &env_guard);
  assert(env);

  Options options;
  options.env = env;
  options.compression = rocksdb::kNoCompression;
  options.IncreaseParallelism();
  options.create_if_missing = true;

  WriteOptions woptions;
  woptions.disableWAL = 1;

  ReadOptions roptions;

  DB* db;
  Status s = DB::Open(options, "/opt/rocks/test", &db);
  std::cout << s.ToString() << std::endl;
  assert(s.ok());

  for (int i = 0; i < 1; ++i) {
    std::cout << "--------------------- Inserting " << nkvs << "kvs" << std::endl;
    s = bulk_insert(db, woptions, nkvs);
    std::cout << "--------------------- Reading " << nkvs << "kvs" << std::endl;
    s = bulk_get(db, roptions, nkvs);
    std::cout << "--------------------- Deleting " << nkvs << "kvs" << std::endl;
    // s = bulk_delete(db, woptions, nkvs);
  }

  delete db;
  return 0;
}

