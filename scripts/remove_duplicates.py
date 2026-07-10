#!/usr/bin/env python3
"""
Remove duplicate function/struct definitions that were previously guarded by
#[cfg(not(feature = "blob-store"))] or #[cfg(not(feature = "similarity"))].

These are the "no blob" variants that duplicate the "with blob" variants.
We keep the "with blob" version (which has the blob field/param) and delete
the "without blob" version.

Strategy: find patterns like:
  /// Owned bundle (no blob-store variant).
  struct OwnedCtx<...> { ... }  ← DELETE this and the comment

  /// Process all complete commands ... (no blob variant)
  fn process_buffer<...> { ... } ← DELETE

  etc.
"""
from pathlib import Path
import re

files = [
    "src/core/server/tcp.rs",
    "src/core/server/io_uring.rs",
    "src/core/lsh.rs",
    "src/main.rs",
    "src/core/checkpoint.rs",
]

for fpath in files:
    p = Path(fpath)
    if not p.exists():
        continue
    text = p.read_text()
    original = text

    # Pattern: comment about "no blob" variant followed by struct/fn
    # These are duplicates that were previously behind #[cfg(not(feature = "blob-store"))]
    # We need to remove the entire duplicate block.

    # Remove blocks starting with "/// ... no blob ..." or "/// ... without blob ..."
    # up to the next blank line + /// or end of function
    # This is fragile — let's use a simpler approach: find duplicate names and remove
    # the second occurrence.

    # Find all "no blob" comment lines and remove the block after them
    lines = text.split('\n')
    new_lines = []
    skip_until_blank = False
    i = 0
    while i < len(lines):
        line = lines[i]
        # Check if this is a "no blob variant" comment
        if 'no blob' in line.lower() or 'without blob' in line.lower() or 'no-blob' in line.lower():
            # Skip this comment line and the following block (until next /// or empty + non-indented)
            i += 1
            # Skip empty lines
            while i < len(lines) and lines[i].strip() == '':
                i += 1
            # Skip the struct/fn block (until we hit a line that starts with /// or fn at column 0)
            brace_depth = 0
            started = False
            while i < len(lines):
                l = lines[i]
                if not started:
                    if l.strip().startswith('struct ') or l.strip().startswith('fn ') or l.strip().startswith('pub fn ') or l.strip().startswith('async fn ') or l.strip().startswith('pub async fn '):
                        started = True
                        brace_depth = l.count('{') - l.count('}')
                        i += 1
                        if brace_depth <= 0 and '{' in l:
                            break
                        continue
                    else:
                        # Not a struct/fn — keep this line, stop skipping
                        break
                else:
                    brace_depth += l.count('{') - l.count('}')
                    i += 1
                    if brace_depth <= 0:
                        # Skip trailing empty line
                        if i < len(lines) and lines[i].strip() == '':
                            i += 1
                        break
            continue
        new_lines.append(line)
        i += 1

    text = '\n'.join(new_lines)
    if text != original:
        p.write_text(text)
        print(f"  ✓ {fpath} — removed duplicates")
    else:
        print(f"  - {fpath} — no changes")
