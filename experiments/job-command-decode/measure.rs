// Isolated measurement only; product crates retain forbid(unsafe_code).
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering::SeqCst};
static ACTIVE: AtomicBool = AtomicBool::new(false);
static CURRENT: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);
struct Meter;
fn change(n:isize) { if ACTIVE.load(SeqCst) { let now=CURRENT.fetch_add(n,SeqCst)+n; PEAK.fetch_max(now,SeqCst); } }
unsafe impl GlobalAlloc for Meter {
 unsafe fn alloc(&self,l:Layout)->*mut u8 { let p=unsafe{System.alloc(l)};if !p.is_null(){change(l.size() as isize);}p }
 unsafe fn dealloc(&self,p:*mut u8,l:Layout) { change(-(l.size() as isize));unsafe{System.dealloc(p,l)} }
 unsafe fn realloc(&self,p:*mut u8,l:Layout,n:usize)->*mut u8 {let q=unsafe{System.realloc(p,l,n)};if !q.is_null(){change(n as isize-l.size() as isize);}q}
}
#[global_allocator] static ALLOC:Meter=Meter;
fn main() {
 let case=std::env::args().nth(1).unwrap_or_else(|| "unknown".into());
 let body=if case=="projection" {
  let fixture:serde_json::Value=serde_json::from_str(include_str!("../../tests/fixtures/management/export-command.json")).unwrap();
  let mut value=fixture["vectors"][0]["command"].clone();
  value["query"]["projection"]=serde_json::json!(vec!["";2049]);
  value.to_string()
 } else { format!("{{\"kind\":\"export\",\"x\":[{}]}}",vec!["[0]";1500].join(",")) };
 assert!(body.len()<8192);
 ACTIVE.store(true,SeqCst);
 let result=serde_json::from_str::<bigbrotr::management::jobs::JobCommand>(&body);
 let rejected=result.is_err();drop(result);
 ACTIVE.store(false,SeqCst);
 println!("body_bytes={} rejected={} peak_live_allocated_bytes={} retained_bytes={}",body.len(),rejected,PEAK.load(SeqCst),CURRENT.load(SeqCst));
 assert!(rejected);
 assert!(PEAK.load(SeqCst) as usize + body.len() < 65536,"decoder exceeds request reservation");
}
