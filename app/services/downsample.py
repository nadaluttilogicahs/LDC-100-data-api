import numpy as np

def lttb(xy: np.ndarray, threshold: int) -> np.ndarray:
    """
    xy: Nx2 (x asc) -> <= threshold punti
    """
    n = xy.shape[0]
    if threshold <= 0 or threshold >= n:
        return xy
    bucket_size = (n - 2) / (threshold - 2)
    a = 0
    out = [xy[0]]
    for i in range(0, threshold - 2):
        start = int(np.floor((i + 1) * bucket_size)) + 1
        end = int(np.floor((i + 2) * bucket_size)) + 1
        if end > n: end = n
        if start >= end: 
            continue
        bucket = xy[start:end]
        avg_x = bucket[:, 0].mean()
        avg_y = bucket[:, 1].mean()

        rstart = int(np.floor(i * bucket_size)) + 1
        rend = int(np.floor((i + 1) * bucket_size)) + 1
        if rend > n: rend = n
        segment = xy[rstart:rend]
        if segment.shape[0] == 0:
            continue

        ax, ay = xy[a]
        dx = segment[:, 0] - ax
        dy1 = segment[:, 1] - ay
        dy2 = avg_y - ay
        areas = np.abs(dx * (dy1 - dy2))
        a = rstart + int(np.argmax(areas))
        out.append(xy[a])
    out.append(xy[-1])
    return np.array(out)

def minmax_bucket(xy: np.ndarray, buckets: int) -> np.ndarray:
    n = xy.shape[0]
    if buckets <= 0 or buckets * 2 >= n:
        return xy
    size = n // buckets
    res = []
    for i in range(buckets):
        s = i * size
        e = (i + 1) * size if i < buckets - 1 else n
        chunk = xy[s:e]
        if chunk.size == 0:
            continue
        ymin = chunk[np.argmin(chunk[:, 1])]
        ymax = chunk[np.argmax(chunk[:, 1])]
        if ymin[0] <= ymax[0]:
            res.extend([ymin, ymax])
        else:
            res.extend([ymax, ymin])
    return np.array(res)
