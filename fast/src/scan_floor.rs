//! Local measurement helper; excluded from the production build by default.
#[allow(dead_code)]
mod engine {
    include!("main.rs");

    fn fast_ingest(s: &[u8]) -> i64 {
        let s: &[u8; 20] = s.try_into().expect("ingest_ts is guaranteed valid F1");
        let pair = |at| i64::from(s[at]) * 10 + i64::from(s[at + 1]) - 528;
        let year = pair(0) * 100 + pair(2);
        civil_days(year, pair(5), pair(8)) * DAY
            + pair(11) * 3_600_000
            + pair(14) * 60_000
            + pair(17) * 1000
    }
    struct BenchPart<'a> {
        rows: u64,
        checksum: u64,
        buffers: Vec<Vec<Raw<'a>>>,
    }

    fn rung<'a, const LEVEL: u8, const FAST: bool>(
        jobs: &[&'a [u8]],
        users: &Users<'a>,
        products: &Map<&'a [u8], u32>,
        unknown_category: u32,
        window_start: i64,
        window_end: i64,
        threads: usize,
        shard_count: usize,
    ) {
        let unknown = UnknownUsers::new(users.names.len() as u32);
        let hash = RandomState::with_seeds(123, 456, 789, 987);
        let mut profile = profiling::Profiler::new(threads);
        let started = Instant::now();
        let parts: Vec<BenchPart> = jobs
            .par_iter()
            .map(|chunk| {
                if LEVEL == 0 {
                    return BenchPart {
                        rows: memchr::memchr_iter(b'\n', chunk).count() as u64,
                        checksum: 0,
                        buffers: Vec::new(),
                    };
                }
                let mut result = BenchPart {
                    rows: 0,
                    checksum: 0,
                    buffers: if LEVEL >= 7 {
                        (0..shard_count).map(|_| Vec::new()).collect()
                    } else {
                        Vec::new()
                    },
                };
                let mut local = Map::new();
                for row in lines(chunk) {
                    let Some(f) = fields::<7>(row) else {
                        continue;
                    };
                    if f[0].is_empty() {
                        continue;
                    }
                    result.rows += 1;
                    result.checksum = result
                        .checksum
                        .wrapping_add(f.iter().map(|field| field.len() as u64).sum());
                    let ts = if LEVEL >= 2 { timestamp(f[2]) } else { None };
                    if LEVEL >= 2 {
                        result.checksum = result.checksum.wrapping_add(ts.unwrap_or(0) as u64);
                    }
                    let ingest = if LEVEL >= 3 {
                        if FAST {
                            fast_ingest(f[3])
                        } else {
                            timestamp(f[3]).unwrap()
                        }
                    } else {
                        0
                    };
                    if LEVEL >= 3 {
                        result.checksum = result.checksum.wrapping_add(ingest as u64);
                    }
                    let event_hash = if LEVEL >= 4 { hash.hash_one(f[0]) } else { 0 };
                    if LEVEL >= 4 {
                        result.checksum ^= event_hash;
                    }
                    let mut valid = true;
                    let mut revenue = 0i64;
                    if LEVEL >= 5 {
                        valid = !f[1].is_empty()
                            && !f[5].is_empty()
                            && ts.is_some_and(|t| {
                                t >= window_start && t < window_end && ingest - t <= DAY
                            });
                        if valid {
                            match f[4] {
                                b"view" | b"cart" => {}
                                b"purchase" => match number(f[6]) {
                                    Some(v) if (1..=10_000_000).contains(&v) => revenue = v,
                                    _ => valid = false,
                                },
                                _ => valid = false,
                            }
                        }
                        result.checksum = result
                            .checksum
                            .wrapping_add(revenue as u64)
                            .wrapping_add(u64::from(valid));
                    }
                    let user = if LEVEL >= 6 && valid {
                        intern_local(f[1], users, &mut local, &unknown)
                    } else {
                        0
                    };
                    if LEVEL >= 6 {
                        result.checksum = result.checksum.wrapping_add(u64::from(user));
                    }
                    let category = if LEVEL >= 8 && valid && revenue > 0 {
                        products.get(f[5]).copied().unwrap_or(unknown_category)
                    } else {
                        unknown_category
                    };
                    if LEVEL >= 8 {
                        result.checksum = result.checksum.wrapping_add(u64::from(category));
                    }
                    if LEVEL >= 7 {
                        let candidate = Candidate {
                            ingest,
                            user,
                            ts: if valid {
                                (ts.unwrap() - window_start) as u32
                            } else {
                                u32::MAX
                            },
                            revenue: revenue as u32,
                            category,
                        };
                        result.buffers[event_hash as usize & (shard_count - 1)].push((
                            EventKey {
                                hash: event_hash,
                                bytes: f[0],
                            },
                            candidate,
                        ));
                    }
                }
                result
            })
            .collect();
        std::hint::black_box(&parts);
        let rows: u64 = parts.iter().map(|part| part.rows).sum();
        let checksum = parts
            .iter()
            .fold(0u64, |sum, part| sum.wrapping_add(part.checksum));
        profile.mark(&format!("r{LEVEL}{}", if FAST { "fast" } else { "" }));
        println!(
            "rung={LEVEL} fast_ingest={FAST} rows={rows} checksum={checksum} candidate_bytes={} raw_bytes={} seconds={:.6}",
            std::mem::size_of::<Candidate>(),
            std::mem::size_of::<Raw>(),
            started.elapsed().as_secs_f64()
        );
    }

    pub fn probe() {
        let args: Vec<String> = std::env::args().collect();
        let data = PathBuf::from(&args[1]);
        let mode = args.get(2).map(String::as_str).unwrap_or("scan");
        let started = Instant::now();
        let threads = std::env::var("NCPU")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8);
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .unwrap();
        let maps: Vec<_> = csv_files(&data.join("events"))
            .iter()
            .map(|p| map_file(p))
            .collect();
        let total_bytes: usize = maps.iter().map(|m| m.len()).sum();
        if mode.starts_with('r') {
            let links: Vec<_> = csv_files(&data.join("links"))
                .iter()
                .map(|path| map_file(path))
                .collect();
            let users = Users::from_links(&links);
            let product_data = map_file(&data.join("products.csv"));
            let mut categories: Vec<&[u8]> = records(&product_data)
                .filter_map(fields::<2>)
                .map(|f| f[1])
                .collect();
            categories.push(b"unknown");
            categories.sort_unstable();
            categories.dedup();
            let unknown_category = categories.binary_search(&b"unknown".as_slice()).unwrap() as u32;
            let products: Map<&[u8], u32> = records(&product_data)
                .filter_map(fields::<2>)
                .map(|f| (f[0], categories.binary_search(&f[1]).unwrap() as u32))
                .collect();
            let manifest: serde_json::Value =
                serde_json::from_slice(&fs::read(data.join("manifest.json")).unwrap()).unwrap();
            let ws = timestamp(manifest["window_start"].as_str().unwrap().as_bytes()).unwrap();
            let we = timestamp(manifest["window_end"].as_str().unwrap().as_bytes()).unwrap();
            let jobs = chunks(&maps);
            let shards = total_bytes
                .div_ceil(8 * 1024 * 1024)
                .next_power_of_two()
                .clamp(64, 8192);
            println!(
                "setup_seconds={:.6} event_bytes={total_bytes} chunks={} threads={threads}",
                started.elapsed().as_secs_f64(),
                jobs.len()
            );
            macro_rules! run {
                ($n:literal,$fast:literal) => {
                    rung::<$n, $fast>(
                        &jobs,
                        &users,
                        &products,
                        unknown_category,
                        ws,
                        we,
                        threads,
                        shards,
                    )
                };
            }
            match mode {
                "r0" => run!(0, false),
                "r1" => run!(1, false),
                "r2" => run!(2, false),
                "r3" => run!(3, false),
                "r3fast" => run!(3, true),
                "r4" => run!(4, false),
                "r5" => run!(5, false),
                "r6" => run!(6, false),
                "r7" => run!(7, false),
                "r8" => run!(8, false),
                _ => panic!("unknown rung"),
            };
            std::process::exit(0);
        }
        let (rows, checksum) = if mode == "scan" {
            (
                chunks(&maps)
                    .par_iter()
                    .map(|chunk| memchr::memchr_iter(b'\n', chunk).count() as u64)
                    .sum(),
                0u64,
            )
        } else {
            chunks(&maps)
                .par_iter()
                .map(|chunk| {
                    let (mut count, mut checksum) = (0u64, 0u64);
                    for row in lines(chunk) {
                        let Some(f) = fields::<7>(row) else {
                            continue;
                        };
                        if f[0].is_empty() {
                            continue;
                        }
                        checksum = checksum.wrapping_add(timestamp(f[2]).unwrap_or(0) as u64);
                        checksum = checksum.wrapping_add(timestamp(f[3]).unwrap() as u64);
                        count += 1;
                    }
                    (count, checksum)
                })
                .reduce(|| (0, 0), |a, b| (a.0 + b.0, a.1.wrapping_add(b.1)))
        };
        println!(
            "mode={mode} threads={threads} bytes={total_bytes} rows={rows} checksum={checksum} seconds={:.6}",
            started.elapsed().as_secs_f64()
        );
        std::process::exit(0);
    }
}

fn main() {
    engine::probe();
}
