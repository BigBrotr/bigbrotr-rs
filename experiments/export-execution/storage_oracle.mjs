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
