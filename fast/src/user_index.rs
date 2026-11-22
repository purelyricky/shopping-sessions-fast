//! Read-only exact ID lookup. The hot table stores 8-byte tag/raw-ID slots;
//! raw ID indexes the separately retained full byte slice, never a truncated ID.

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Slot {
    tag: u32,
    raw_plus_one: u32,
}

pub struct UserIndex {
    slots: Vec<Slot>,
    mask: usize,
}

#[inline(always)]
fn prefetch(pointer: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        // A cache hint only, with no dereference or effect on correctness.
        std::arch::x86_64::_mm_prefetch(pointer.cast(), std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = pointer;
}

impl UserIndex {
    pub fn new(count: usize, entries: impl IntoIterator<Item = (u64, u32)>) -> Self {
        let capacity = count.max(1).checked_mul(2).unwrap().next_power_of_two();
        let mut table = Self {
            slots: vec![Slot::default(); capacity],
            mask: capacity - 1,
        };
        for (hash, raw) in entries {
            let mut at = hash as usize & table.mask;
            while table.slots[at].raw_plus_one != 0 {
                at = (at + 1) & table.mask;
            }
            table.slots[at] = Slot {
                tag: (hash >> 32) as u32,
                raw_plus_one: raw.checked_add(1).expect("raw user ID overflow"),
            };
        }
        table
    }

    #[inline(always)]
    pub fn prefetch_slot(&self, hash: u64) {
        let at = hash as usize & self.mask;
        prefetch(self.slots.as_ptr().wrapping_add(at).cast());
        // A probe crossing its first cache line can fetch the following line.
        prefetch(
            self.slots
                .as_ptr()
                .wrapping_add((at + 8) & self.mask)
                .cast(),
        );
    }

    #[inline(always)]
    pub fn candidate(&self, hash: u64) -> Option<u32> {
        let mut at = hash as usize & self.mask;
        loop {
            let slot = self.slots[at];
            if slot.raw_plus_one == 0 {
                return None;
            }
            if slot.tag == (hash >> 32) as u32 {
                return Some(slot.raw_plus_one - 1);
            }
            at = (at + 1) & self.mask;
        }
    }

    #[inline(always)]
    pub fn prefetch_name(&self, raw: u32, names: &[&[u8]], parents: &[u32]) {
        let raw = raw as usize;
        if raw < names.len() {
            prefetch(names.as_ptr().wrapping_add(raw).cast());
        }
        if raw < parents.len() {
            prefetch(parents.as_ptr().wrapping_add(raw).cast());
        }
    }

    #[inline(always)]
    pub fn prefetch_bytes(&self, raw: u32, names: &[&[u8]]) {
        if let Some(bytes) = names.get(raw as usize) {
            if !bytes.is_empty() {
                prefetch(bytes.as_ptr());
                prefetch(bytes.as_ptr().wrapping_add(bytes.len() - 1));
            }
        }
    }

    #[inline]
    pub fn find(&self, hash: u64, bytes: &[u8], names: &[&[u8]]) -> Option<u32> {
        let mut at = hash as usize & self.mask;
        let tag = (hash >> 32) as u32;
        loop {
            let slot = self.slots[at];
            if slot.raw_plus_one == 0 {
                return None;
            }
            let raw = slot.raw_plus_one - 1;
            if slot.tag == tag && names[raw as usize] == bytes {
                return Some(raw);
            }
            at = (at + 1) & self.mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_hash_collisions_and_probe_wrap_keep_exact_opaque_ids() {
        assert_eq!(std::mem::size_of::<Slot>(), 8);
        let long = vec![0xc3; 131_072];
        let names: Vec<&[u8]> = vec![b"u1", b" u1", b"U1", &long];
        let hash = u64::MAX;
        let table = UserIndex::new(names.len(), (0..names.len() as u32).map(|raw| (hash, raw)));
        for (raw, name) in names.iter().enumerate() {
            table.prefetch_slot(hash);
            let candidate = table.candidate(hash).unwrap();
            table.prefetch_name(candidate, &names, &[]);
            table.prefetch_bytes(candidate, &names);
            let separately_allocated = name.to_vec();
            assert_eq!(
                table.find(hash, &separately_allocated, &names),
                Some(raw as u32)
            );
        }
        assert_eq!(table.find(hash, b"u1\r", &names), None);
        assert_eq!(table.find(hash ^ (1 << 32), b"u1", &names), None);
    }

    #[test]
    fn empty_index_has_safe_miss_and_prefetch_paths() {
        let table = UserIndex::new(0, []);
        table.prefetch_slot(u64::MAX);
        table.prefetch_name(u32::MAX, &[], &[]);
        table.prefetch_bytes(u32::MAX, &[]);
        assert_eq!(table.candidate(u64::MAX), None);
        assert_eq!(table.find(u64::MAX, b"any", &[]), None);
    }
}
