//! The specified CSV dialect has no escaped or embedded delimiters. Scan comma
//! and quote structure once, then take exact byte slices without normalization.

#[inline(always)]
pub fn fields<const N: usize>(row: &[u8]) -> Option<[&[u8]; N]> {
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("avx2") {
        // Runtime detection also checks OS support for AVX state.
        return unsafe { vector_fields(row) };
    }
    scalar_fields(row)
}

pub fn scalar_fields<const N: usize>(row: &[u8]) -> Option<[&[u8]; N]> {
    let mut result = [&[][..]; N];
    let mut count = 0;
    for mut value in row.split(|&byte| byte == b',') {
        if count == N {
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
        result[count] = value;
        count += 1;
    }
    (count == N).then_some(result)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn vector_fields<const N: usize>(row: &[u8]) -> Option<[&[u8]; N]> {
    use std::arch::x86_64::*;
    if N == 0 {
        return None;
    }
    let mut ends = [0usize; N];
    let mut count = 0usize;
    let mut quotes = 0usize;
    let mut append = |mut mask: u32, offset: usize| {
        if count + mask.count_ones() as usize >= N {
            return false;
        }
        while mask != 0 {
            ends[count] = offset + mask.trailing_zeros() as usize;
            count += 1;
            mask &= mask - 1;
        }
        true
    };
    // Every vector load is within row or an initialized short-row local copy.
    unsafe {
        let commas = _mm256_set1_epi8(b',' as i8);
        let quote = _mm256_set1_epi8(b'"' as i8);
        let mut offset = 0;
        while row.len() - offset >= 32 {
            let bytes = _mm256_loadu_si256(row.as_ptr().add(offset).cast());
            let cm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, commas)) as u32;
            let qm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, quote)) as u32;
            quotes += qm.count_ones() as usize;
            if !append(cm, offset) {
                return None;
            }
            offset += 32;
        }
        if offset < row.len() {
            let mut tail = [0u8; 32];
            let (bytes, base, keep) = if row.len() >= 32 {
                // Overlap the last complete vector, masking bytes that were
                // already counted. This never reads past the row boundary.
                let base = row.len() - 32;
                (
                    _mm256_loadu_si256(row.as_ptr().add(base).cast()),
                    base,
                    u32::MAX << (offset - base),
                )
            } else {
                tail[..row.len()].copy_from_slice(row);
                (_mm256_loadu_si256(tail.as_ptr().cast()), 0, u32::MAX)
            };
            let cm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, commas)) as u32 & keep;
            let qm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, quote)) as u32 & keep;
            quotes += qm.count_ones() as usize;
            if !append(cm, base) {
                return None;
            }
        }
    }
    if count + 1 != N {
        return None;
    }
    ends[count] = row.len();
    let mut result = [&[][..]; N];
    let mut start = 0;
    let mut syntax_quotes = 0;
    for i in 0..N {
        let end = ends[i];
        let mut value = &row[start..end];
        if value.first() == Some(&b'"') {
            if value.len() < 2 || value.last() != Some(&b'"') {
                return None;
            }
            value = &value[1..value.len() - 1];
            syntax_quotes += 2;
        }
        result[i] = value;
        start = end + 1;
    }
    // Validated endpoints are distinct actual quotes; any extra quote is bad.
    (quotes == syntax_quotes).then_some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn next(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }
    fn compare(row: &[u8]) {
        assert_eq!(fields::<2>(row), scalar_fields::<2>(row));
        assert_eq!(fields::<4>(row), scalar_fields::<4>(row));
        assert_eq!(fields::<7>(row), scalar_fields::<7>(row));
    }
    #[test]
    fn vector_matches_scalar_on_random_structure_and_tails() {
        let alphabet = b"ab19 ,\"\r\t\0\xff\xc3\xa9";
        let mut state = 0x7a5b_1234_9abc_def1;
        for _ in 0..30_000 {
            let len = (next(&mut state) % 300) as usize;
            let prefix = (next(&mut state) % 64) as usize;
            let mut storage = vec![0; prefix + len];
            for byte in &mut storage[prefix..] {
                *byte = alphabet[next(&mut state) as usize % alphabet.len()];
            }
            compare(&storage[prefix..]);
        }
        for len in [
            0, 1, 15, 16, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 8192,
        ] {
            let mut row = vec![b'x'; len];
            row.extend_from_slice(b",\" b \",c,d,e,f,g");
            compare(&row);
        }
        for iteration in 0..5000 {
            let mut row = Vec::new();
            for field in 0..7 {
                if field > 0 {
                    row.push(b',');
                }
                let quoted = next(&mut state) % 5 == 0;
                if quoted {
                    row.push(b'"');
                }
                for _ in 0..next(&mut state) % 48 {
                    row.push(b"ab01 _\r\t"[next(&mut state) as usize % 8]);
                }
                if quoted {
                    row.push(b'"');
                }
            }
            if iteration % 3 == 0 {
                let at = next(&mut state) as usize % (row.len() + 1);
                row.insert(at, b'"');
            }
            if iteration % 7 == 0 {
                row.push(b'\r');
            }
            compare(&row);
        }
    }
    #[test]
    fn exact_quote_and_cr_rules() {
        for row in [
            b"\"a\",\"b\"".as_slice(),
            b",",
            b" a ,\" b \"",
            b"a,b\r",
            b"\"\",\"\"",
        ] {
            compare(row);
            assert!(fields::<2>(row).is_some());
        }
        for row in [
            b"a\"x,b".as_slice(),
            b"\"a\"x,b",
            b"\"a\"\"b\",c",
            b"\"a,b\"",
            b"\"a\",\"b\"\r",
            b"a,b,c",
            b"",
            b"\"\"\"\",b",
        ] {
            compare(row);
            assert!(fields::<2>(row).is_none());
        }
    }
}
