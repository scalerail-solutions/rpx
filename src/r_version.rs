use std::cmp::Ordering;

pub(crate) fn r_version_components(version: &str) -> Result<Vec<u32>, String> {
    version
        .split(['.', '-'])
        .map(|part| {
            if part.is_empty() {
                return Err(format!("invalid version {version}"));
            }

            part.parse::<u32>()
                .map_err(|_| format!("invalid version {version}"))
        })
        .collect()
}

pub(crate) fn compare_r_versions(left: &str, right: &str) -> Ordering {
    match (r_version_components(left), r_version_components(right)) {
        (Ok(left), Ok(right)) => compare_version_components(&left, &right),
        _ => left.cmp(right),
    }
}

pub(crate) fn compare_version_components(left: &[u32], right: &[u32]) -> Ordering {
    for (left, right) in left.iter().zip(right) {
        match left.cmp(right) {
            Ordering::Equal => continue,
            ordering => return ordering,
        }
    }

    left.len().cmp(&right.len())
}

#[cfg(test)]
mod tests {
    use super::compare_r_versions;

    #[test]
    fn compares_r_versions_numerically() {
        assert!(compare_r_versions("1.10", "1.2").is_gt());
        assert!(compare_r_versions("1.2-1", "1.2").is_gt());
        assert!(compare_r_versions("2.0.0", "1.99.99").is_gt());
    }
}
