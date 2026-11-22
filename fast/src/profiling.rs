//! Optional Linux-only measurements. Set PROFILE=1 for local development runs.
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

#[derive(Clone, Copy, Default)]
struct Usage {
    user: f64,
    system: f64,
    minor: i64,
    major: i64,
    rss_kib: i64,
}

#[cfg(target_os = "linux")]
fn usage() -> Usage {
    let mut raw = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, raw.as_mut_ptr()) } != 0 {
        return Usage::default();
    }
    let raw = unsafe { raw.assume_init() };
    Usage {
        user: raw.ru_utime.tv_sec as f64 + raw.ru_utime.tv_usec as f64 / 1e6,
        system: raw.ru_stime.tv_sec as f64 + raw.ru_stime.tv_usec as f64 / 1e6,
        minor: raw.ru_minflt,
        major: raw.ru_majflt,
        rss_kib: raw.ru_maxrss,
    }
}
#[cfg(not(target_os = "linux"))]
fn usage() -> Usage {
    Usage::default()
}

#[cfg(target_os = "linux")]
fn thread_cpu() -> f64 {
    let mut raw = std::mem::MaybeUninit::<libc::timespec>::zeroed();
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, raw.as_mut_ptr()) } != 0 {
        return 0.0;
    }
    let raw = unsafe { raw.assume_init() };
    raw.tv_sec as f64 + raw.tv_nsec as f64 / 1e9
}
#[cfg(not(target_os = "linux"))]
fn thread_cpu() -> f64 {
    0.0
}

pub struct Job {
    wall: Instant,
    cpu: f64,
}
#[derive(Default)]
struct Worker {
    jobs: AtomicU64,
    bytes: AtomicU64,
    wall_ns: AtomicU64,
    cpu_ns: AtomicU64,
}
pub struct Profiler {
    enabled: bool,
    last_wall: Instant,
    last_usage: Usage,
    workers: Vec<Worker>,
}

impl Profiler {
    pub fn new(threads: usize) -> Self {
        let enabled = cfg!(target_os = "linux") && std::env::var_os("PROFILE").is_some();
        Self {
            enabled,
            last_wall: Instant::now(),
            last_usage: if enabled { usage() } else { Usage::default() },
            workers: (0..threads).map(|_| Worker::default()).collect(),
        }
    }
    pub fn mark(&mut self, label: &str) {
        if !self.enabled {
            return;
        }
        let now = Instant::now();
        let next = usage();
        let wall = now.duration_since(self.last_wall).as_secs_f64();
        let user = next.user - self.last_usage.user;
        let system = next.system - self.last_usage.system;
        eprintln!(
            "PROFILE phase={label} wall={wall:.6}s user={user:.6}s sys={system:.6}s cores={:.3} minflt={} majflt={} peak_rss_mib={:.3}",
            (user + system) / wall.max(1e-9),
            next.minor - self.last_usage.minor,
            next.major - self.last_usage.major,
            next.rss_kib as f64 / 1024.0
        );
        self.last_wall = now;
        self.last_usage = next;
    }
    pub fn start_job(&self) -> Option<Job> {
        self.enabled.then(|| Job {
            wall: Instant::now(),
            cpu: thread_cpu(),
        })
    }
    pub fn finish_job(&self, job: Option<Job>, bytes: usize) {
        let Some(job) = job else {
            return;
        };
        let worker = &self.workers[rayon::current_thread_index().unwrap_or(0)];
        worker.jobs.fetch_add(1, Ordering::Relaxed);
        worker.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        worker
            .wall_ns
            .fetch_add(job.wall.elapsed().as_nanos() as u64, Ordering::Relaxed);
        worker.cpu_ns.fetch_add(
            ((thread_cpu() - job.cpu) * 1e9).max(0.0) as u64,
            Ordering::Relaxed,
        );
    }
    pub fn report_parse(&self) {
        if !self.enabled {
            return;
        }
        for (index, w) in self.workers.iter().enumerate() {
            eprintln!(
                "PROFILE parse_worker={index} chunks={} mib={:.3} wall={:.6}s cpu={:.6}s",
                w.jobs.load(Ordering::Relaxed),
                w.bytes.load(Ordering::Relaxed) as f64 / 1048576.0,
                w.wall_ns.load(Ordering::Relaxed) as f64 / 1e9,
                w.cpu_ns.load(Ordering::Relaxed) as f64 / 1e9
            );
        }
    }
}
