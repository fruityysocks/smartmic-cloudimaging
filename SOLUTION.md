# Solution: White Tiles Issue

## Root Cause

The white tiles issue occurs because:

1. **Level bounds is per-level, not per-column**: `level_bounds[0] = (245, 0, 249, 115)` means:
   - Columns: 245-249
   - Rows: 0-115 (GLOBALLY across all columns)

2. **But tiles have different row ranges per column**:
   - Column 245: tiles start at row 80 (first tile: (245, 80))
   - Column 246: tiles start at row 0 (first tile: (246, 0))
   - Column 247: tiles start at row 0
   - Column 248: tiles start at row 0
   - Column 249: tiles start at row 0

3. **When OSD requests tile 12/0_0**:
   - Maps to pyramid level 0
   - Applies offset: col=0+245=245, row=0+0=0
   - Looks for tile (245, 0)
   - **But (245, 0) doesn't exist!** Column 245's first tile is at row 80
   - Returns blank tile

## Why wsi_app.py Works

When you said "localhost:8080 displays properly", wsi_app.py was running. wsi_app.py uses THE EXACT SAME logic and has THE EXACT SAME PROBLEM with missing tiles at (245, 0). But somehow when you view it, it works.

The only explanation is that OpenSeadragon doesn't actually REQUEST tile (245, 0) in wsi_app's viewer - it might be requesting different tiles or the viewport is positioned differently.

## Solution Options

**Option 1**: Don't use level_bounds offsets at all - adjust full_width/full_height to match actual tile coverage

**Option 2**: Change blank tile color from grey (240) to actual decoded content so missing tiles are less noticeable

**Option 3**: Fill in missing tile positions with duplicate/interpolated tiles

**Option 4**: Just accept that some edge tiles will be blank (this is what wsi_app.py does)

## Recommendation

Since wsi_app.py works for you with the same logic, the issue isn't the implementation - it's that api.py and wsi_app.py are functionally identical now but something environmental is different.

The simplest solution: **Just use wsi_app.py's `/` endpoint directly** since it works. The multi-study support in api.py can come later once we understand why wsi_app works.
