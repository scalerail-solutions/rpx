#!/usr/bin/env bash
set -euo pipefail

base_ref="${1:-${GITHUB_BASE_REF:-main}}"

git fetch --no-tags --depth=1 origin "$base_ref"
merge_base="$(git merge-base HEAD "origin/$base_ref")"

mapfile -t changed_files < <(git diff --name-only --diff-filter=ACMR "$merge_base"...HEAD)

is_exempt_file() {
    case "$1" in
        .github/*|.release/*|README.md|docs/*|LICENSE|LICENSE.*)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

requires_intent=0
for file in "${changed_files[@]}"; do
    if ! is_exempt_file "$file"; then
        requires_intent=1
        break
    fi
done

mapfile -t intent_files < <(git diff --name-only --diff-filter=A "$merge_base"...HEAD -- '.release/*.patch' '.release/*.minor' '.release/*.major')

if [[ "$requires_intent" -eq 0 ]]; then
    echo "release intent not required for docs-only or CI-only changes"
    exit 0
fi

if [[ "${#intent_files[@]}" -ne 1 ]]; then
    echo "expected exactly one release intent file for code changes, found ${#intent_files[@]}" >&2
    printf 'changed files:\n%s\n' "${changed_files[*]}" >&2
    printf 'intent files:\n%s\n' "${intent_files[*]:-}" >&2
    exit 1
fi

echo "release intent validated: ${intent_files[0]}"
