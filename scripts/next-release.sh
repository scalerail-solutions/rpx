#!/usr/bin/env bash
set -euo pipefail

current_version="$({
    awk '
        $0 == "[package]" { in_package = 1; next }
        /^\[/ && $0 != "[package]" { in_package = 0 }
        in_package && /^version = "/ {
            gsub(/^version = "/, "")
            gsub(/"$/, "")
            print
            exit
        }
    ' Cargo.toml
})"

if [[ -z "$current_version" ]]; then
    echo "failed to read package version from Cargo.toml" >&2
    exit 1
fi

last_tag="$(git describe --tags --abbrev=0 2>/dev/null || true)"

if [[ -n "$last_tag" ]]; then
    mapfile -t intent_files < <(git diff --name-only --diff-filter=A "$last_tag"...HEAD -- '.release/*.patch' '.release/*.minor' '.release/*.major')
else
    mapfile -t intent_files < <(git ls-files '.release/*.patch' '.release/*.minor' '.release/*.major')
fi

bump_level="none"
for file in "${intent_files[@]}"; do
    case "$file" in
        *.major)
            bump_level="major"
            break
            ;;
        *.minor)
            if [[ "$bump_level" != "major" ]]; then
                bump_level="minor"
            fi
            ;;
        *.patch)
            if [[ "$bump_level" == "none" ]]; then
                bump_level="patch"
            fi
            ;;
    esac
done

next_version=""
tag=""
release_required="false"

if [[ "$bump_level" != "none" ]]; then
    IFS='.' read -r major minor patch <<< "$current_version"

    case "$bump_level" in
        major)
            next_version="$((major + 1)).0.0"
            ;;
        minor)
            next_version="$major.$((minor + 1)).0"
            ;;
        patch)
            next_version="$major.$minor.$((patch + 1))"
            ;;
    esac

    tag="v$next_version"
    release_required="true"
fi

write_output() {
    local key="$1"
    local value="$2"

    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
        {
            printf '%s<<__VALUE__\n' "$key"
            printf '%s\n' "$value"
            printf '__VALUE__\n'
        } >> "$GITHUB_OUTPUT"
    else
        printf '%s=%s\n' "$key" "$value"
    fi
}

write_output current_version "$current_version"
write_output last_tag "$last_tag"
write_output bump_level "$bump_level"
write_output next_version "$next_version"
write_output tag "$tag"
write_output release_required "$release_required"
write_output intent_files "$(printf '%s\n' "${intent_files[@]}")"
