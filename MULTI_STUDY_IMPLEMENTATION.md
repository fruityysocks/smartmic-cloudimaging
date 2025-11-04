# Multi-Study Viewer Implementation

## Current Status

**wsi_app.py:**
- ✅ Full HTJ2K tile decoding working
- ✅ Serves tiles correctly
- ❌ Only displays ALL image sets (no study selection)

**api.py:**
- ✅ Multi-study REST API structure
- ✅ Import/list/delete endpoints working
- ❌ Viewer returns blank tiles (not implemented)

## What Needs to Be Done

To complete `api.py` with multi-study viewer support, we need to:

### 1. Add Study Metadata Cache
Store metadata for each study separately:
```python
study_metadata_cache = {}  # {study_uid: wsi_metadata}
```

### 2. Implement `load_study_metadata(study_uid)` Function
Copy logic from wsi_app.py lines 35-196 but:
- Filter image sets by study UID
- Build pyramid for that specific study
- Cache the result

### 3. Complete Tile Serving Endpoints

**a) `/api/studies/<study_uid>/dzi`**
- Load study metadata if not cached
- Return proper DZI descriptor with actual dimensions

**b) `/api/studies/<study_uid>/tiles/<level>/<col>_<row>.jpg`**
- Load study metadata if not cached
- Copy tile serving logic from wsi_app.py lines 246-310
- Decode HTJ2K frames and return JPEG

### 4. Add Study Selection Page

**c) `/api/studies` (enhance current endpoint)**
- Already lists studies
- Add links to viewers

**d) Create `/`  (root) endpoint**
- Show list of available studies
- Link to each viewer

## Implementation Approach

**Option A: Copy-paste from wsi_app.py (Quick)**
- ~200 lines of code to copy
- Modify to support study_uid parameter
- Should work in 30 minutes

**Option B: Refactor into shared module (Clean)**
- Extract common code into `src/viewer/`
- Both wsi_app.py and api.py use it
- Better long-term but more work

## Recommended: Option A

Since time is limited and `wsi_app.py` works, let's:
1. Copy the working code to api.py
2. Add study_uid filtering
3. Cache metadata per study
4. Test with multiple studies

## Files to Modify

1. **api.py** - Add tile serving logic (~200 lines)
2. **templates/viewer.html** - May need to adjust for study UID

## Testing Plan

1. Import 2 different studies to datastore
2. List studies: `curl http://localhost:8080/api/studies`
3. View study 1: `open http://localhost:8080/api/viewer/{study1_uid}`
4. View study 2: `open http://localhost:8080/api/viewer/{study2_uid}`
5. Verify tiles load correctly for both

## Estimated Time

- Implementation: 30-45 minutes
- Testing: 15 minutes
- **Total: 1 hour**
