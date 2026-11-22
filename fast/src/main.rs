use ahash::{AHashMap as Map, AHashSet as Set, RandomState};
use memmap2::{Mmap, MmapOptions};
use parquet::{
    data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use rayon::prelude::*;
use std::{
    collections::HashMap,
    fs::{self, File},
    hash::{BuildHasherDefault, Hash, Hasher},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

#[derive(Clone, Copy)]
struct EventKey<'a> {
    hash: u64,
    bytes: &'a [u8],
}
impl PartialEq for EventKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.bytes == other.bytes
    }
}
impl Eq for EventKey<'_> {}
impl Hash for EventKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}
#[derive(Default)]
struct CachedHasher(u64);
impl Hasher for CachedHasher {
    // Separate table bucket/fingerprint bits from the low shard-selection bits.
    fn finish(&self) -> u64 {
        self.0.rotate_left(32)
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("EventKey writes one cached u64 hash");
    }
    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
type Bytes<'a> = &'a [u8];

// LF is the only record delimiter. All other bytes, including CR and spaces,
// belong to fields. There are no quoted delimiters in this specified dialect.
fn fields<const N: usize>(row: &[u8]) -> Option<[&[u8]; N]> {
    let mut result = [&[][..]; N];
    let mut n = 0;
    for mut value in memchr::memchr_iter(b',', row)
        .map(|i| i + 1)
        .chain(std::iter::once(row.len() + 1))
        .scan(0, |start, end| {
            let part = &row[*start..end - 1];
            *start = end;
            Some(part)
        })
    {
        if n == N {
            return None;
        }
        if value.first() == Some(&b'"') {
            if value.len() < 2 || value.last() != Some(&b'"') {
                return None;
            }
            value = &value[1..value.len() - 1];
        }
        if value.contains(&b'"') {
            return None;
        }
        result[n] = value;
        n += 1;
    }
    (n == N).then_some(result)
}

fn number(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let s = &s[s.iter().position(|&c| c != b'0').unwrap_or(s.len())..];
    if s.len() > 19 {
        return None;
    }
    let mut value = 0u64;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u64::from(c - b'0');
    }
    i64::try_from(value).ok()
}

// Proleptic Gregorian calendar -> days since 1970-01-01.
fn civil_days(year: i64, month: i64, day: i64) -> i64 {
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468
}

fn timestamp(s: &[u8]) -> Option<i64> {
    if s.len() != 20 && s.len() != 25 {
        return number(s);
    }
    if s.iter().all(u8::is_ascii_digit) {
        return number(s);
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = number(&s[0..4])?;
    let m = number(&s[5..7])?;
    let d = number(&s[8..10])?;
    let h = number(&s[11..13])?;
    let minute = number(&s[14..16])?;
    let sec = number(&s[17..19])?;
    if !(1..=12).contains(&m) || h > 23 || minute > 59 || sec > 59 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let maxday = match m {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d < 1 || d > maxday {
        return None;
    }
    let offset = if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        if (s[19] != b'+' && s[19] != b'-') || s[22] != b':' {
            return None;
        }
        let oh = number(&s[20..22])?;
        let om = number(&s[23..25])?;
        if oh > 23 || om > 59 {
            return None;
        }
        (oh * 60 + om) * 60_000 * if s[19] == b'+' { 1 } else { -1 }
    };
    Some(civil_days(y, m, d) * DAY + h * 3_600_000 + minute * 60_000 + sec * 1000 - offset)
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "csv"))
        .collect();
    paths.sort();
    paths
}
fn map_file(path: &Path) -> Mmap {
    let file = File::open(path).unwrap();
    // Files are immutable for the lifetime of the process (/data is read-only).
    unsafe { MmapOptions::new().map(&file).unwrap() }
}
fn records(data: &[u8]) -> impl Iterator<Item = &[u8]> {
    lines(data).skip(1)
}
fn lines(data: &[u8]) -> impl Iterator<Item = &[u8]> {
    memchr::memchr_iter(b'\n', data)
        .map(|at| at + 1)
        .chain(std::iter::once(data.len() + 1))
        .scan(0, |start, end| {
            let line = &data[*start..end - 1];
            *start = end;
            Some(line)
        })
}
fn chunks(maps: &[Mmap]) -> Vec<&[u8]> {
    let mut jobs = Vec::new();
    for data in maps {
        let mut start = data
            .iter()
            .position(|&c| c == b'\n')
            .map_or(data.len(), |p| p + 1);
        while start < data.len() {
            let mut end = (start + 4 * 1024 * 1024).min(data.len());
            if end < data.len() {
                end += data[end..]
                    .iter()
                    .position(|&c| c == b'\n')
                    .map_or(data.len() - end, |p| p + 1);
            }
            jobs.push(&data[start..end]);
            start = end;
        }
    }
    jobs
}

struct Users<'a> {
    ids: Map<Bytes<'a>, u32>,
    names: Vec<Bytes<'a>>,
    parent: Vec<u32>,
}
impl<'a> Users<'a> {
    fn new() -> Self {
        Self {
            ids: Map::new(),
            names: Vec::new(),
            parent: Vec::new(),
        }
    }
    fn add(&mut self, s: Bytes<'a>) -> u32 {
        if let Some(&id) = self.ids.get(s) {
            return id;
        }
        let id = self.names.len() as u32;
        self.ids.insert(s, id);
        self.names.push(s);
        self.parent.push(id);
        id
    }
    fn root(&mut self, mut id: u32) -> u32 {
        while self.parent[id as usize] != id {
            self.parent[id as usize] = self.parent[self.parent[id as usize] as usize];
            id = self.parent[id as usize];
        }
        id
    }
    fn join(&mut self, a: Bytes<'a>, b: Bytes<'a>) {
        let ai = self.add(a);
        let bi = self.add(b);
        let mut ar = self.root(ai);
        let mut br = self.root(bi);
        if ar == br {
            return;
        }
        if self.names[ar as usize] > self.names[br as usize] {
            std::mem::swap(&mut ar, &mut br);
        }
        self.parent[br as usize] = ar;
    }
    fn finish(&mut self) {
        for id in 0..self.parent.len() {
            self.parent[id] = self.root(id as u32);
        }
        for id in self.ids.values_mut() {
            *id = self.parent[*id as usize];
        }
    }
}

#[derive(Clone, Copy)]
struct Candidate {
    user: u32,
    ingest: i64,
    ts: i64,
    revenue: u32,
    category: u32,
}
type Raw<'a> = (EventKey<'a>, Candidate);

struct Parsed<'a> {
    shards: Vec<Vec<Raw<'a>>>,
    users: Vec<Bytes<'a>>,
}
#[derive(Clone, Copy)]
struct Event {
    // Milliseconds from manifest.window_start (the generator window is 14 days).
    ts: u32,
    revenue: u32,
    user: u32,
    category: u32,
}
#[derive(Clone, Copy)]
struct Click<'a> {
    ts: i64,
    id: Bytes<'a>,
    user: u32,
    campaign: u32,
}
struct ClickInput<'a> {
    ts: i64,
    id: Bytes<'a>,
    user: u32,
    campaign: u32,
}

fn intern_local<'a>(
    id: Bytes<'a>,
    known: &Map<Bytes<'a>, u32>,
    local: &mut Map<Bytes<'a>, u32>,
    names: &mut Vec<Bytes<'a>>,
    base: u32,
) -> u32 {
    if let Some(&user) = known.get(id) {
        return user;
    }
    *local.entry(id).or_insert_with(|| {
        let user = base + names.len() as u32;
        names.push(id);
        user
    })
}
fn remap(user: u32, base: u32, missing: &[u32]) -> u32 {
    if user < base {
        user
    } else {
        missing[(user - base) as usize]
    }
}
#[derive(Default, Clone, Copy)]
struct Metrics {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assists: i64,
}
struct UserRow {
    user: u32,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    category: Option<u32>,
}
#[derive(Default)]
struct Results {
    users: Vec<UserRow>,
    campaigns: Map<(u32, i32), Metrics>,
}

fn process_people(
    spans: &[(usize, usize)],
    events: &[Event],
    clicks: &[Click],
    click_offsets: &[usize],
    window_start: i64,
) -> Results {
    let mut result = Results::default();
    for &(begin, end) in spans {
        let uid = events[begin].user;
        let user_clicks = &clicks[click_offsets[uid as usize]..click_offsets[uid as usize + 1]];
        let (mut left, mut right) = (0, 0);
        let mut active: Map<u32, u32> = Map::new();
        let mut day = i32::MIN;
        let (mut daily_sessions, mut daily_events, mut daily_revenue) = (0, 0, 0);
        let mut categories: Map<u32, i64> = Map::new();
        let push_daily =
            |result: &mut Results, day, sessions, evs, revenue, categories: &Map<u32, i64>| {
                if sessions == 0 {
                    return;
                }
                let top = categories
                    .iter()
                    .max_by(|(ca, va), (cb, vb)| va.cmp(vb).then_with(|| cb.cmp(ca)))
                    .map(|(&c, _)| c);
                result.users.push(UserRow {
                    user: uid,
                    day,
                    sessions,
                    events: evs,
                    revenue,
                    category: top,
                });
            };
        let mut at = begin;
        while at < end {
            let start = window_start + i64::from(events[at].ts);
            let session_day = start.div_euclid(DAY) as i32;
            if day != session_day {
                push_daily(
                    &mut result,
                    day,
                    daily_sessions,
                    daily_events,
                    daily_revenue,
                    &categories,
                );
                day = session_day;
                daily_sessions = 0;
                daily_events = 0;
                daily_revenue = 0;
                categories.clear();
            }
            let mut stop = at + 1;
            while stop < end && events[stop].ts - events[stop - 1].ts <= 1_800_000 {
                stop += 1;
            }
            let mut revenue = 0;
            for event in &events[at..stop] {
                if event.revenue > 0 {
                    revenue += i64::from(event.revenue);
                    *categories.entry(event.category).or_default() += i64::from(event.revenue);
                }
            }
            daily_sessions += 1;
            daily_events += (stop - at) as i64;
            daily_revenue += revenue;
            while right < user_clicks.len() && user_clicks[right].ts <= start {
                *active.entry(user_clicks[right].campaign).or_default() += 1;
                right += 1;
            }
            while left < right && user_clicks[left].ts < start - WEEK {
                let campaign = user_clicks[left].campaign;
                let n = active.get_mut(&campaign).unwrap();
                *n -= 1;
                if *n == 0 {
                    active.remove(&campaign);
                }
                left += 1;
            }
            let credited = if right > left {
                user_clicks[right - 1].campaign
            } else {
                0
            };
            let metric = result.campaigns.entry((credited, day)).or_default();
            metric.sessions += 1;
            metric.purchasing += i64::from(revenue > 0);
            metric.revenue += revenue;
            for &campaign in active.keys() {
                result.campaigns.entry((campaign, day)).or_default().assists += 1;
            }
            at = stop;
        }
        push_daily(
            &mut result,
            day,
            daily_sessions,
            daily_events,
            daily_revenue,
            &categories,
        );
    }
    result
}

enum Col {
    Strings(Vec<ByteArray>),
    OptionalStrings(Vec<ByteArray>, Vec<i16>),
    Ints(Vec<i64>),
    Dates(Vec<i32>),
}
fn str_col<'a>(values: impl Iterator<Item = Bytes<'a>>) -> Col {
    Col::Strings(values.map(ByteArray::from).collect())
}
fn write_table(path: &Path, filename: &str, schema: &str, columns: Vec<Col>) {
    fs::create_dir_all(path).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
        .build();
    let mut writer = SerializedFileWriter::new(
        File::create(path.join(filename)).unwrap(),
        Arc::new(parse_message_type(schema).unwrap()),
        Arc::new(props),
    )
    .unwrap();
    let mut group = writer.next_row_group().unwrap();
    for values in columns {
        let mut column = group.next_column().unwrap().unwrap();
        match values {
            Col::Strings(v) => {
                column
                    .typed::<ByteArrayType>()
                    .write_batch(&v, None, None)
                    .unwrap();
            }
            Col::OptionalStrings(v, d) => {
                column
                    .typed::<ByteArrayType>()
                    .write_batch(&v, Some(&d), None)
                    .unwrap();
            }
            Col::Ints(v) => {
                column
                    .typed::<Int64Type>()
                    .write_batch(&v, None, None)
                    .unwrap();
            }
            Col::Dates(v) => {
                column
                    .typed::<Int32Type>()
                    .write_batch(&v, None, None)
                    .unwrap();
            }
        }
        column.close().unwrap();
    }
    group.close().unwrap();
    writer.close().unwrap();
}

fn main() {
    let clock = Instant::now();
    let args: Vec<_> = std::env::args().collect();
    let data = args
        .get(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into())));
    let output = args
        .get(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into())));
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(8);
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(data.join("manifest.json")).unwrap()).unwrap();
    let window_start = timestamp(manifest["window_start"].as_str().unwrap().as_bytes()).unwrap();
    let window_end = timestamp(manifest["window_end"].as_str().unwrap().as_bytes()).unwrap();
    assert!(
        window_end > window_start && window_end - window_start <= i64::from(u32::MAX),
        "window must fit relative milliseconds"
    );
    let product_data = map_file(&data.join("products.csv"));
    let campaign_data = map_file(&data.join("campaigns.csv"));
    let mut categories: Vec<&[u8]> = records(&product_data)
        .filter_map(fields::<2>)
        .map(|f| f[1])
        .collect();
    categories.push(b"unknown");
    categories.sort_unstable();
    categories.dedup();
    let unknown = categories.binary_search(&b"unknown".as_slice()).unwrap() as u32;
    let products: Map<&[u8], u32> = records(&product_data)
        .filter_map(fields::<2>)
        .map(|f| (f[0], categories.binary_search(&f[1]).unwrap() as u32))
        .collect();
    let mut campaign_names: Vec<&[u8]> = vec![b"organic"];
    let mut channels: Vec<&[u8]> = vec![b"organic"];
    let mut campaigns: Map<&[u8], u32> = Map::new();
    for f in records(&campaign_data).filter_map(fields::<2>) {
        campaigns.insert(f[0], campaign_names.len() as u32);
        campaign_names.push(f[0]);
        channels.push(f[1]);
    }
    let links: Vec<_> = csv_files(&data.join("links"))
        .iter()
        .map(|p| map_file(p))
        .collect();
    let mut users = Users::new();
    for link in &links {
        for f in records(link).filter_map(fields::<2>) {
            if !f[0].is_empty() && !f[1].is_empty() {
                users.join(f[0], f[1]);
            }
        }
    }
    users.finish();
    let known_count = users.names.len() as u32;
    eprintln!("dimensions+links {:.3}s", clock.elapsed().as_secs_f64());
    let event_files: Vec<_> = csv_files(&data.join("events"))
        .iter()
        .map(|p| map_file(p))
        .collect();
    let click_files: Vec<_> = csv_files(&data.join("clicks"))
        .iter()
        .map(|p| map_file(p))
        .collect();
    let hash = RandomState::with_seeds(123, 456, 789, 987);
    // Keep the dedup working set near cache size as input volume changes.
    let event_bytes: usize = event_files.iter().map(|file| file.len()).sum();
    let shard_count = event_bytes
        .div_ceil(8 * 1024 * 1024)
        .next_power_of_two()
        .clamp(64, 8192);
    let mut parsed: Vec<Parsed> = chunks(&event_files)
        .into_par_iter()
        .map(|chunk| {
            let mut shards: Vec<Vec<Raw>> = (0..shard_count).map(|_| Vec::new()).collect();
            let mut names = Vec::new();
            let mut local = Map::new();
            for row in lines(chunk) {
                let Some(f) = fields::<7>(row) else {
                    continue;
                };
                if f[0].is_empty() {
                    continue;
                }
                let ingest = timestamp(f[3]).expect("ingest_ts is valid F1");
                // Cache validation, but retain invalid candidates for exact
                // earliest-ingest dedup. An invalid winner is never rescued.
                let valid = (|| {
                    if f[1].is_empty() || f[5].is_empty() {
                        return None;
                    }
                    let ts = timestamp(f[2])?;
                    if ts < window_start || ts >= window_end || ingest - ts > DAY {
                        return None;
                    }
                    let revenue = match f[4] {
                        b"view" | b"cart" => 0,
                        b"purchase" => match number(f[6]) {
                            Some(v) if (1..=10_000_000).contains(&v) => v,
                            _ => return None,
                        },
                        _ => return None,
                    };
                    let category = if revenue > 0 {
                        products.get(f[5]).copied().unwrap_or(unknown)
                    } else {
                        unknown
                    };
                    Some((ts, revenue, category))
                })();
                let (ts, revenue, category) = valid.unwrap_or((i64::MIN, 0, unknown));
                let user = if valid.is_some() {
                    intern_local(f[1], &users.ids, &mut local, &mut names, known_count)
                } else {
                    0
                };
                let event_hash = hash.hash_one(f[0]);
                shards[(event_hash as usize) & (shard_count - 1)].push((
                    EventKey {
                        hash: event_hash,
                        bytes: f[0],
                    },
                    Candidate {
                        user,
                        ingest,
                        ts,
                        revenue: revenue as u32,
                        category,
                    },
                ));
            }
            Parsed {
                shards,
                users: names,
            }
        })
        .collect();
    eprintln!("parse events {:.3}s", clock.elapsed().as_secs_f64());
    let parsed_clicks: Vec<(Vec<ClickInput>, Vec<&[u8]>)> = chunks(&click_files)
        .into_par_iter()
        .map(|chunk| {
            let mut list = Vec::new();
            let mut ids = Vec::new();
            let mut local = Map::new();
            for row in lines(chunk) {
                let Some(f) = fields::<4>(row) else {
                    continue;
                };
                if f[0].is_empty() || f[1].is_empty() {
                    continue;
                }
                let Some(&campaign) = campaigns.get(f[2]) else {
                    continue;
                };
                let Some(ts) = timestamp(f[3]) else {
                    continue;
                };
                if ts < window_start - WEEK || ts >= window_end {
                    continue;
                }
                list.push(ClickInput {
                    ts,
                    id: f[0],
                    user: intern_local(f[1], &users.ids, &mut local, &mut ids, known_count),
                    campaign,
                });
            }
            (list, ids)
        })
        .collect();
    eprintln!("parse clicks {:.3}s", clock.elapsed().as_secs_f64());
    let missing = parsed
        .par_iter()
        .map(|p| p.users.iter().copied().collect::<Set<_>>())
        .chain(
            parsed_clicks
                .par_iter()
                .map(|(_, ids)| ids.iter().copied().collect::<Set<_>>()),
        )
        .reduce(Set::new, |mut a, mut b| {
            if a.len() < b.len() {
                std::mem::swap(&mut a, &mut b);
            }
            a.extend(b);
            a
        });
    users.ids.reserve(missing.len());
    for user in missing {
        users.add(user);
    }
    users.finish();
    parsed.par_iter_mut().for_each(|p| {
        let mapping: Vec<u32> = p.users.iter().map(|id| users.ids[id]).collect();
        for part in &mut p.shards {
            for (_, candidate) in part {
                if candidate.ts != i64::MIN {
                    candidate.user = remap(candidate.user, known_count, &mapping);
                }
            }
        }
    });
    eprintln!("intern users {:.3}s", clock.elapsed().as_secs_f64());
    let mut raw_shards: Vec<Vec<Vec<Raw>>> = (0..shard_count).map(|_| Vec::new()).collect();
    for p in parsed {
        for (out, part) in raw_shards.iter_mut().zip(p.shards) {
            out.push(part);
        }
    }
    let mut clicks: Vec<Click> = parsed_clicks
        .into_par_iter()
        .flat_map_iter(|(v, missing)| {
            let mapping: Vec<u32> = missing.iter().map(|id| users.ids[id]).collect();
            v.into_iter().map(move |c| Click {
                ts: c.ts,
                id: c.id,
                user: remap(c.user, known_count, &mapping),
                campaign: c.campaign,
            })
        })
        .collect();
    let user_names = &users.names;
    eprintln!(
        "parse+identity {:.3}s; {} raw events, {} IDs",
        clock.elapsed().as_secs_f64(),
        raw_shards.iter().flatten().map(Vec::len).sum::<usize>(),
        user_names.len()
    );
    let event_parts: Vec<Vec<Event>> = raw_shards
        .into_par_iter()
        .map(|raw| {
            let mut keep: HashMap<EventKey, Candidate, BuildHasherDefault<CachedHasher>> =
                HashMap::with_capacity_and_hasher(
                    raw.iter().map(Vec::len).sum(),
                    Default::default(),
                );
            for (id, candidate) in raw.into_iter().flatten() {
                let entry = keep.entry(id).or_insert(candidate);
                if candidate.ingest < entry.ingest {
                    *entry = candidate;
                }
            }
            let mut events = Vec::with_capacity(keep.len());
            for (_, candidate) in keep {
                if candidate.ts == i64::MIN {
                    continue;
                }
                events.push(Event {
                    ts: (candidate.ts - window_start) as u32,
                    revenue: candidate.revenue,
                    user: candidate.user,
                    category: candidate.category,
                });
            }
            events
        })
        .collect();
    let mut events = Vec::with_capacity(event_parts.iter().map(Vec::len).sum());
    for mut part in event_parts {
        events.append(&mut part);
    }
    eprintln!(
        "dedup+validate {:.3}s; {} events, {} clicks",
        clock.elapsed().as_secs_f64(),
        events.len(),
        clicks.len()
    );
    rayon::join(
        || events.par_sort_unstable_by_key(|e| (e.user, e.ts)),
        || {
            // At a tied timestamp the smallest click ID is last, for last-click credit.
            clicks.par_sort_unstable_by(|a, b| {
                a.user
                    .cmp(&b.user)
                    .then(a.ts.cmp(&b.ts))
                    .then_with(|| b.id.cmp(a.id))
            });
            clicks.dedup_by(|a, b| a.id == b.id);
        },
    );
    let mut click_offsets = vec![0; user_names.len() + 1];
    for c in &clicks {
        click_offsets[c.user as usize + 1] += 1;
    }
    for i in 1..click_offsets.len() {
        click_offsets[i] += click_offsets[i - 1];
    }
    let mut spans = Vec::new();
    let mut at = 0;
    while at < events.len() {
        let begin = at;
        at += 1;
        while at < events.len() && events[at].user == events[begin].user {
            at += 1;
        }
        spans.push((begin, at));
    }
    eprintln!(
        "sort {:.3}s; {} people with events",
        clock.elapsed().as_secs_f64(),
        spans.len()
    );
    let batches: Vec<Results> = spans
        .par_chunks(4096)
        .map(|p| process_people(p, &events, &clicks, &click_offsets, window_start))
        .collect();
    let mut results = Results::default();
    for mut batch in batches {
        results.users.append(&mut batch.users);
        for (key, v) in batch.campaigns {
            let m = results.campaigns.entry(key).or_default();
            m.sessions += v.sessions;
            m.purchasing += v.purchasing;
            m.revenue += v.revenue;
            m.assists += v.assists;
        }
    }
    let us = &results.users;
    let parts: Vec<&[UserRow]> = if us.is_empty() {
        vec![us]
    } else {
        us.chunks(131_072).collect()
    };
    parts.into_par_iter().enumerate().for_each(|(index, part)| {
        let mut cv = Vec::new();
        let mut definitions = Vec::with_capacity(part.len());
        for u in part {
            definitions.push(i16::from(u.category.is_some()));
            if let Some(c) = u.category {
                cv.push(ByteArray::from(categories[c as usize]));
            }
        }
        write_table(
        &output.join("user_daily"), &format!("part-{index:05}.parquet"),
        "message user_daily { REQUIRED BYTE_ARRAY user_id (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BYTE_ARRAY top_category (UTF8); }",
        vec![
            str_col(part.iter().map(|u| user_names[u.user as usize])),
            Col::Dates(part.iter().map(|u| u.day).collect()),
            Col::Ints(part.iter().map(|u| u.sessions).collect()),
            Col::Ints(part.iter().map(|u| u.events).collect()),
            Col::Ints(part.iter().map(|u| u.revenue).collect()),
            Col::OptionalStrings(cv, definitions),
        ],
        );
    });
    let cs: Vec<_> = results.campaigns.into_iter().collect();
    write_table(
        &output.join("campaign_daily"),
        "part-0.parquet",
        "message campaign_daily { REQUIRED BYTE_ARRAY campaign_id (UTF8); REQUIRED BYTE_ARRAY channel (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }",
        vec![
            str_col(cs.iter().map(|((c, _), _)| campaign_names[*c as usize])),
            str_col(cs.iter().map(|((c, _), _)| channels[*c as usize])),
            Col::Dates(cs.iter().map(|((_, d), _)| *d).collect()),
            Col::Ints(cs.iter().map(|(_, m)| m.sessions).collect()),
            Col::Ints(cs.iter().map(|(_, m)| m.purchasing).collect()),
            Col::Ints(cs.iter().map(|(_, m)| m.revenue).collect()),
            Col::Ints(cs.iter().map(|(_, m)| m.assists).collect()),
        ],
    );
    eprintln!(
        "finished {:.3}s; {} user/day rows, {} campaign/day rows",
        clock.elapsed().as_secs_f64(),
        us.len(),
        cs.len()
    );
    // All files are closed and committed above. Avoid walking large temporary
    // allocations and unmapping read-only inputs just before process termination.
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_hash_collisions_preserve_distinct_event_ids() {
        let mut map: HashMap<EventKey, i32, BuildHasherDefault<CachedHasher>> = HashMap::default();
        map.insert(
            EventKey {
                hash: 7,
                bytes: b"e1",
            },
            10,
        );
        map.insert(
            EventKey {
                hash: 7,
                bytes: b"e1 ",
            },
            20,
        );
        map.insert(
            EventKey {
                hash: 7,
                bytes: b"E1",
            },
            30,
        );
        map.insert(
            EventKey {
                hash: 7,
                bytes: b"e1\r",
            },
            40,
        );
        let same_id_in_another_buffer = b"e1".to_vec();
        assert_eq!(
            map.insert(
                EventKey {
                    hash: 7,
                    bytes: &same_id_in_another_buffer
                },
                50
            ),
            Some(10)
        );
        assert_eq!(map.len(), 4);
        assert_eq!(
            map[&EventKey {
                hash: 7,
                bytes: b"e1 "
            }],
            20
        );
        assert_eq!(
            map[&EventKey {
                hash: 7,
                bytes: b"E1"
            }],
            30
        );
        assert_eq!(
            map[&EventKey {
                hash: 7,
                bytes: b"e1\r"
            }],
            40
        );
    }
}
