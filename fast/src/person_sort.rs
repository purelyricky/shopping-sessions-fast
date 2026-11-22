use super::{Click, Event};
use rayon::prelude::*;

const BUCKETS: usize = 256;

// A user occurs in exactly one bucket. Global user order is unnecessary: both
// sessionization and click indexing only require each user's rows contiguous.
fn partition<T: Send>(parts: Vec<Vec<T>>, user: impl Fn(&T) -> u32 + Sync + Send) -> Vec<Vec<T>> {
    let routed: Vec<Vec<Vec<T>>> = parts
        .into_par_iter()
        .map(|part| {
            let mut buckets: Vec<Vec<T>> = (0..BUCKETS).map(|_| Vec::new()).collect();
            for value in part {
                let at = user(&value) as usize & (BUCKETS - 1);
                buckets[at].push(value);
            }
            buckets
        })
        .collect();
    let mut groups: Vec<Vec<Vec<T>>> = (0..BUCKETS).map(|_| Vec::new()).collect();
    for source in routed {
        for (group, part) in groups.iter_mut().zip(source) {
            group.push(part);
        }
    }
    groups
        .into_par_iter()
        .map(|parts| {
            let mut result = Vec::with_capacity(parts.iter().map(Vec::len).sum());
            for mut part in parts {
                result.append(&mut part);
            }
            result
        })
        .collect()
}
fn flatten<T>(parts: Vec<Vec<T>>) -> Vec<T> {
    let mut result = Vec::with_capacity(parts.iter().map(Vec::len).sum());
    for mut part in parts {
        result.append(&mut part);
    }
    result
}
pub fn events(parts: Vec<Vec<Event>>) -> Vec<Event> {
    let mut buckets = partition(parts, |event| event.user);
    buckets.par_iter_mut().for_each(|part| {
        radsort::sort_by_key(part, |event| {
            (u64::from(event.user) << 32) | u64::from(event.ts)
        })
    });
    flatten(buckets)
}
pub fn clicks(parts: Vec<Vec<Click<'_>>>, origin: i64, span: i64) -> Vec<Click<'_>> {
    let mut buckets = partition(parts, |click| click.user);
    buckets.par_iter_mut().for_each(|part| {
        if span <= i64::from(u32::MAX) {
            radsort::sort_by_cached_key(part, |click| {
                (u64::from(click.user) << 32) | ((click.ts - origin) as u64)
            });
        } else {
            radsort::sort_by_key(part, |click| {
                (u128::from(click.user) << 64) | u128::from((click.ts as u64) ^ (1u64 << 63))
            });
        }
        let mut at = 0;
        while at < part.len() {
            let mut end = at + 1;
            while end < part.len() && part[end].user == part[at].user && part[end].ts == part[at].ts
            {
                end += 1;
            }
            // The smallest click ID is last at tied timestamps.
            if end - at > 1 {
                part[at..end].sort_unstable_by(|a, b| b.id.cmp(a.id));
            }
            at = end;
        }
        part.dedup_by(|a, b| a.id == b.id);
    });
    flatten(buckets)
}
