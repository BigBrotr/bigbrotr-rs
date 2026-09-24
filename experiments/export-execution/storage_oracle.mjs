// Independent closed browser-storage oracle for the selected no-snapshot panel.
// Kit2.70.3 client.js persist_state writes only these two framework dictionaries.
export function inspectStorage({ local, session }) {
  const refused = { status: 'refused', localEntries: local.length, sessionEntries: session.length };
  if (local.length !== 0 || session.length !== 2) return refused;
  const keys=session.map(([key])=>key).sort();
  if (JSON.stringify(keys)!==JSON.stringify(['sveltekit:scroll','sveltekit:snapshot'])) return refused;
  const parsed={};
  for(const [key,raw] of session) {
    if(typeof raw!=='string'||raw.length>4096)return refused;
    try{parsed[key]=JSON.parse(raw);}catch{return refused;}
    // Kit writes JSON.stringify output; duplicate keys/hidden discarded bytes
    // must never disappear from the privacy oracle during JSON.parse.
    if(raw!==JSON.stringify(parsed[key]))return refused;
  }
  const object=value=>value!==null && typeof value==='object' && !Array.isArray(value);
  const scroll=parsed['sveltekit:scroll'], snapshot=parsed['sveltekit:snapshot'];
  if(!object(scroll)||!object(snapshot)||Object.keys(snapshot).length!==0)return refused;
  const entries=Object.entries(scroll);
  if(entries.length===0||entries.length>8)return refused;
  for(const [index,position] of entries) {
    if(!/^[1-9][0-9]{0,15}$/.test(index)||!Number.isSafeInteger(Number(index)))return refused;
    if(!object(position)||JSON.stringify(Object.keys(position).sort())!==JSON.stringify(['x','y']))return refused;
    if(![position.x,position.y].every(n=>typeof n==='number'&&Number.isFinite(n)&&n>=0&&n<=1_000_000))return refused;
  }
  // Only recognized numeric scroll positions and an empty snapshot reach evidence.
  return {status:'verified',scroll,snapshot};
}

export function inspectResumeStorage(databases) {
  const refused = { status: 'refused' };
  const keys = (value, expected) => value !== null && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).sort().join(',') === expected;
  if (!Array.isArray(databases) || databases.length !== 1) return refused;
  const db = databases[0];
  if (!keys(db, 'name,stores,version') || db.name !== 'bigbrotr-private-commands' || db.version !== 1
    || !Array.isArray(db.stores) || db.stores.length !== 1) return refused;
  const store = db.stores[0];
  if (!keys(store, 'autoIncrement,indexes,keyPath,name,records') || store.name !== 'export-resume'
    || store.keyPath !== 'principal_id' || store.autoIncrement !== false
    || !Array.isArray(store.indexes) || store.indexes.length !== 0
    || !Array.isArray(store.records) || store.records.length > 32) return refused;
  const identity = value => typeof value === 'string' && value.length === 32
    && /^[0-9a-f]+$/.test(value) && value !== '0'.repeat(32);
  const revision = value => typeof value === 'string' && /^[1-9][0-9]{0,19}$/.test(value)
    && BigInt(value) <= 18446744073709551615n;
  const accounts = new Set();
  for (const record of store.records) {
    if (!keys(record, 'command,format,job_id,key,outcome,principal_id') || record.format !== 'bigbrotr-browser-resume/1'
      || ![record.principal_id, record.job_id, record.key].every(identity)
      || !['pending', 'accepted', 'refused'].includes(record.outcome)
      || !keys(record.command, 'expected_config_revision,expected_job_revision,expected_policy_revision')
      || !Object.values(record.command).every(revision) || JSON.stringify(record).length > 512
      || accounts.has(record.principal_id)) return refused;
    accounts.add(record.principal_id);
  }
  return { status: 'verified', records: store.records };
}

// Read only in the isolated browser context; rejected contents never enter the
// report. No application decoder or SessionModel participates in this oracle.
export async function readResumeStorage(page) {
  const databases = await page.evaluate(async () => {
    const values = [];
    for (const identity of await indexedDB.databases()) {
      const db = await new Promise((resolve, reject) => {
        const request = indexedDB.open(identity.name, identity.version);
        request.onsuccess = () => resolve(request.result); request.onerror = () => reject(request.error);
      });
      try {
        const stores = [];
        for (const name of db.objectStoreNames) {
          const tx = db.transaction(name), store = tx.objectStore(name), request = store.getAll();
          const records = await new Promise((resolve, reject) => {
            tx.oncomplete = () => resolve(request.result); tx.onabort = () => reject(tx.error);
          });
          stores.push({ name, keyPath: store.keyPath, autoIncrement: store.autoIncrement, indexes: Array.from(store.indexNames), records });
        }
        values.push({ name: db.name, version: db.version, stores });
      } finally { db.close(); }
    }
    return values;
  });
  return inspectResumeStorage(databases);
}
