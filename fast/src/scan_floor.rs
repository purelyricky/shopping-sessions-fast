//! Local measurement helper; excluded from the production build by default.
#[allow(dead_code)]
mod engine {
    include!("main.rs");

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
